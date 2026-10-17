use anyhow::Context;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::{fs::File, io::Read, path::Path, sync::Mutex};

#[derive(Clone, Debug, Serialize)]
pub struct RoomConfig {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing)]
    pub socket_key: String,
    #[serde(skip_serializing)]
    pub podium_key: Option<String>,
}

pub struct RoomStore {
    connection: Mutex<Connection>,
}

impl RoomStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let connection = Connection::open(path).context("open room database")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS rooms (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                socket_key TEXT NOT NULL,
                podium_key TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_rooms_name ON rooms(name);",
        )?;
        let store = Self {
            connection: Mutex::new(connection),
        };
        Ok(store)
    }

    pub fn list(&self) -> anyhow::Result<Vec<RoomConfig>> {
        let connection = self.connection.lock().unwrap();
        let mut statement =
            connection.prepare("SELECT id,name,socket_key,podium_key FROM rooms ORDER BY id")?;
        let rows = statement.query_map([], |row| {
            Ok(RoomConfig {
                id: row.get(0)?,
                name: row.get(1)?,
                socket_key: row.get(2)?,
                podium_key: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get(&self, id: &str) -> anyhow::Result<Option<RoomConfig>> {
        let connection = self.connection.lock().unwrap();
        connection
            .query_row(
                "SELECT id,name,socket_key,podium_key FROM rooms WHERE id=?1",
                [id],
                |row| {
                    Ok(RoomConfig {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        socket_key: row.get(2)?,
                        podium_key: row.get(3)?,
                    })
                },
            )
            .optional()
            .context("query room")
    }

    pub fn exists(&self, id: &str) -> anyhow::Result<bool> {
        let connection = self.connection.lock().unwrap();
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM rooms WHERE id=?1)",
                [id],
                |row| row.get(0),
            )
            .context("check room")
    }

    pub fn authenticate(&self, id: &str, key: &str) -> anyhow::Result<bool> {
        Ok(self
            .get(id)?
            .map(|room| room.socket_key == key)
            .unwrap_or(false))
    }

    pub fn create(
        &self,
        id: &str,
        name: &str,
        podium_key: Option<&str>,
    ) -> anyhow::Result<RoomConfig> {
        validate_room_id(id)?;
        if name.trim().is_empty() {
            anyhow::bail!("room name must not be empty");
        }
        let key = generate_key();
        let connection = self.connection.lock().unwrap();
        connection.execute(
            "INSERT INTO rooms(id,name,socket_key,podium_key) VALUES(?1,?2,?3,?4)",
            params![
                id,
                name.trim(),
                key,
                podium_key.filter(|value| !value.is_empty())
            ],
        )?;
        Ok(RoomConfig {
            id: id.to_owned(),
            name: name.trim().to_owned(),
            socket_key: key,
            podium_key: podium_key
                .map(str::to_owned)
                .filter(|value| !value.is_empty()),
        })
    }

    pub fn delete(&self, id: &str) -> anyhow::Result<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.execute("DELETE FROM rooms WHERE id=?1", [id])? != 0)
    }

    pub fn regenerate_key(&self, id: &str) -> anyhow::Result<Option<String>> {
        let key = generate_key();
        let connection = self.connection.lock().unwrap();
        let changed = connection.execute(
            "UPDATE rooms SET socket_key=?2,updated_at=CURRENT_TIMESTAMP WHERE id=?1",
            params![id, key],
        )?;
        Ok((changed != 0).then_some(key))
    }

    pub fn set_podium_key(&self, id: &str, key: Option<&str>) -> anyhow::Result<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.execute(
            "UPDATE rooms SET podium_key=?2,updated_at=CURRENT_TIMESTAMP WHERE id=?1",
            params![id, key.filter(|value| !value.is_empty())],
        )? != 0)
    }
}

pub fn validate_room_id(id: &str) -> anyhow::Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        anyhow::bail!("room id must be 1..64 ASCII letters, digits, '_' or '-'");
    }
    Ok(())
}

fn generate_key() -> String {
    let mut bytes = [0u8; 24];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut bytes))
        .expect("/dev/urandom must be readable");
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    bytes
        .iter()
        .map(|byte| ALPHABET[(*byte as usize) % ALPHABET.len()] as char)
        .collect()
}
