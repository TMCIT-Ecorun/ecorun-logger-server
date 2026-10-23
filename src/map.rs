use anyhow::Context;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::Path, sync::Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MapConfig {
    pub center_lat: f64,
    pub center_lon: f64,
    pub zoom: f64,
    pub plots: Vec<MapPlot>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MapPlot {
    pub id: String,
    pub kind: String,
    pub coordinates: Vec<[f64; 2]>,
    pub color: String,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TelemetryRecord {
    pub id: i64,
    /// Stable across nodes; legacy/local-only records have no event ID.
    pub event_id: Option<String>,
    pub room_id: String,
    pub source: String,
    pub timestamp: Option<String>,
    pub data: Value,
    pub received_at: String,
}

pub struct MapStore {
    connection: Mutex<rusqlite::Connection>,
}

impl MapStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let mut connection = rusqlite::Connection::open(path).context("open SQLite database")?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS map_configs (
                 room_id TEXT PRIMARY KEY, center_lat REAL NOT NULL, center_lon REAL NOT NULL,
                 zoom REAL NOT NULL, updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE TABLE IF NOT EXISTS map_plots (
                 id TEXT PRIMARY KEY, room_id TEXT NOT NULL, kind TEXT NOT NULL,
                 coordinates_json TEXT NOT NULL, color TEXT NOT NULL, label TEXT,
                 updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             CREATE INDEX IF NOT EXISTS idx_map_plots_room ON map_plots(room_id);
             CREATE TABLE IF NOT EXISTS telemetry (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL, source TEXT NOT NULL,
                 timestamp TEXT, data_json TEXT NOT NULL, received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                 event_id TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_telemetry_room_id ON telemetry(room_id, id DESC);",
        )?;

        // Serialize schema migrations across processes. Older databases have
        // telemetry rows without event IDs; keep those rows and their local IDs.
        let transaction =
            connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let has_event_id = {
            let mut statement = transaction.prepare("PRAGMA table_info(telemetry)")?;
            let names = statement
                .query_map([], |row| row.get::<_, String>(1))?
                .collect::<Result<Vec<_>, _>>()?;
            names.iter().any(|name| name == "event_id")
        };
        if !has_event_id {
            transaction.execute("ALTER TABLE telemetry ADD COLUMN event_id TEXT", [])?;
        }
        transaction.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_telemetry_event_id
             ON telemetry(event_id) WHERE event_id IS NOT NULL",
            [],
        )?;
        transaction.commit()?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    pub fn get_map(&self, room_id: &str) -> anyhow::Result<MapConfig> {
        let connection = self.connection.lock().unwrap();
        let center = connection
            .query_row(
                "SELECT center_lat, center_lon, zoom FROM map_configs WHERE room_id=?1",
                [room_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        let (center_lat, center_lon, zoom) = center.unwrap_or((0.0, 0.0, 3.0));
        let mut statement = connection.prepare("SELECT id,kind,coordinates_json,color,label FROM map_plots WHERE room_id=?1 ORDER BY id")?;
        let plots = statement
            .query_map([room_id], |row| {
                let coordinates: String = row.get(2)?;
                Ok(MapPlot {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    coordinates: serde_json::from_str(&coordinates).unwrap_or_default(),
                    color: row.get(3)?,
                    label: row.get(4)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(MapConfig {
            center_lat,
            center_lon,
            zoom,
            plots,
        })
    }

    pub fn has_map(&self, room_id: &str) -> anyhow::Result<bool> {
        let connection = self.connection.lock().unwrap();
        Ok(connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM map_configs WHERE room_id=?1)",
            [room_id],
            |row| row.get(0),
        )?)
    }

    pub fn save_map(&self, room_id: &str, map: &MapConfig) -> anyhow::Result<()> {
        let mut connection = self.connection.lock().unwrap();
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT INTO map_configs(room_id,center_lat,center_lon,zoom) VALUES(?1,?2,?3,?4)
             ON CONFLICT(room_id) DO UPDATE SET center_lat=excluded.center_lat,center_lon=excluded.center_lon,zoom=excluded.zoom,updated_at=CURRENT_TIMESTAMP",
            rusqlite::params![room_id, map.center_lat, map.center_lon, map.zoom],
        )?;
        transaction.execute("DELETE FROM map_plots WHERE room_id=?1", [room_id])?;
        for plot in &map.plots {
            transaction.execute(
                "INSERT INTO map_plots(id,room_id,kind,coordinates_json,color,label) VALUES(?1,?2,?3,?4,?5,?6)",
                rusqlite::params![plot.id, room_id, plot.kind, serde_json::to_string(&plot.coordinates)?, plot.color, plot.label],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn append_telemetry(
        &self,
        room_id: &str,
        source: &str,
        timestamp: Option<&str>,
        data: &Value,
    ) -> anyhow::Result<i64> {
        let connection = self.connection.lock().unwrap();
        connection.execute(
            "INSERT INTO telemetry(room_id,source,timestamp,data_json) VALUES(?1,?2,?3,?4)",
            rusqlite::params![room_id, source, timestamp, serde_json::to_string(data)?],
        )?;
        Ok(connection.last_insert_rowid())
    }

    /// Store a telemetry event once across retries, replays, and concurrent writers.
    /// Event IDs must be stable and globally unique across rooms and nodes.
    /// Returns true for the first insert, or false if the event ID already exists.
    pub fn append_replicated_telemetry(
        &self,
        event_id: &str,
        room_id: &str,
        source: &str,
        timestamp: Option<&str>,
        data: &Value,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            !event_id.trim().is_empty(),
            "telemetry event_id must not be empty"
        );
        let connection = self.connection.lock().unwrap();
        let inserted = connection.execute(
            "INSERT INTO telemetry(event_id,room_id,source,timestamp,data_json)
             VALUES(?1,?2,?3,?4,?5) ON CONFLICT DO NOTHING",
            rusqlite::params![
                event_id,
                room_id,
                source,
                timestamp,
                serde_json::to_string(data)?
            ],
        )?;
        Ok(inserted == 1)
    }

    /// Page through this node's replicated rows in local insertion order.
    /// The returned `id` is a local SQLite cursor, not a cross-node event ID.
    /// Legacy rows without event IDs are excluded because they cannot be deduplicated.
    pub fn replicated_telemetry_since(
        &self,
        room_id: &str,
        after_local_id: i64,
        limit: usize,
    ) -> anyhow::Result<Vec<TelemetryRecord>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT id,event_id,room_id,source,timestamp,data_json,received_at
             FROM telemetry WHERE room_id=?1 AND id>?2 AND event_id IS NOT NULL
             ORDER BY id ASC LIMIT ?3",
        )?;
        let rows = statement
            .query_map(
                rusqlite::params![
                    room_id,
                    after_local_id,
                    i64::try_from(limit).unwrap_or(i64::MAX)
                ],
                read_telemetry_record,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn recent_telemetry(
        &self,
        room_id: &str,
        limit: usize,
    ) -> anyhow::Result<Vec<TelemetryRecord>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare("SELECT id,event_id,room_id,source,timestamp,data_json,received_at FROM telemetry WHERE room_id=?1 ORDER BY id DESC LIMIT ?2")?;
        let rows = statement
            .query_map(
                rusqlite::params![room_id, i64::try_from(limit).unwrap_or(i64::MAX)],
                read_telemetry_record,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn telemetry_history(
        &self,
        room_id: &str,
        since: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<TelemetryRecord>> {
        let connection = self.connection.lock().unwrap();
        let mut statement = connection.prepare(
            "SELECT id,event_id,room_id,source,timestamp,data_json,received_at FROM telemetry
             WHERE room_id=?1 AND (?2 IS NULL OR datetime(received_at)>=datetime(?2))
             AND (?3 IS NULL OR datetime(received_at)<=datetime(?3))
             ORDER BY id DESC LIMIT ?4",
        )?;
        let rows = statement
            .query_map(
                rusqlite::params![room_id, since, until, limit.min(5000) as i64],
                read_telemetry_record,
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

fn read_telemetry_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<TelemetryRecord> {
    let data: String = row.get(5)?;
    Ok(TelemetryRecord {
        id: row.get(0)?,
        event_id: row.get(1)?,
        room_id: row.get(2)?,
        source: row.get(3)?,
        timestamp: row.get(4)?,
        data: serde_json::from_str(&data).unwrap_or(Value::Null),
        received_at: row.get(6)?,
    })
}
