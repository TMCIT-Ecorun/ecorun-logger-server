//! Per-node telemetry files.
//!
//! Logger runs are kept as JSON Lines, one file per room and session, so a run
//! can be read back (or tailed live) without the database. A new session file
//! starts whenever the logger sends `podium.meta` (connect, or channel change):
//!
//!   <dir>/<room_id>/<YYYYMMDD-HHMMSS>Z.jsonl
//!   {"recv_ms":1791077786762,"meta":[...]}
//!   {"recv_ms":1791077787262,"samples":[[...],[...]]}
//!
//! `recv_ms` is the receive time (Unix ms) on the node the logger is connected
//! to, so replicated copies match the original. Rows are stored exactly as the
//! logger sent them: values in `meta` order followed by the bitmask words.
//! The named rows decoded from them (`podium`) are not repeated here. Any other
//! telemetry (for example Socket.IO `telemetry.ingest`) is appended as the whole
//! event to `<dir>/<room_id>/events.ndjson`.
//!
//! The directory comes from `ECORUN_TELEMETRY_DIR` (default `./data`); set it to
//! an empty string to disable the files.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};
use tokio::{
    fs::{self, File, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};
use tracing::{info, warn};

use crate::socketio_api::TelemetryEvent;

const DEFAULT_DIR: &str = "./data";

pub struct TelemetryFiles {
    dir: Option<PathBuf>,
    /// room_id -> currently open session file
    sessions: Mutex<HashMap<String, File>>,
}

impl TelemetryFiles {
    pub fn from_env() -> Self {
        let dir = match std::env::var("ECORUN_TELEMETRY_DIR") {
            Ok(value) if value.trim().is_empty() => None,
            Ok(value) => Some(PathBuf::from(value)),
            Err(_) => Some(PathBuf::from(DEFAULT_DIR)),
        };
        match &dir {
            Some(dir) => info!(dir = %dir.display(), "telemetry files enabled"),
            None => info!("telemetry files disabled"),
        }
        Self::new(dir)
    }

    pub fn new(dir: Option<PathBuf>) -> Self {
        Self {
            dir,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    pub async fn append(&self, event: &TelemetryEvent) {
        let Some(dir) = &self.dir else { return };
        let recv_ms: u64 = event.received_at.parse().unwrap_or_else(|_| now_ms());
        match event.source.as_str() {
            "podium.meta" => {
                let Some(meta) = event.data.get("meta") else {
                    return;
                };
                let mut sessions = self.sessions.lock().await;
                sessions.remove(&event.room_id);
                match open_session(dir, &event.room_id).await {
                    Ok(file) => {
                        sessions.insert(event.room_id.clone(), file);
                    }
                    Err(error) => {
                        warn!(room = %event.room_id, %error, "failed to open telemetry file");
                        return;
                    }
                }
                let line = json!({ "recv_ms": recv_ms, "meta": meta });
                write_session(&mut sessions, &event.room_id, line).await;
            }
            "podium.raw" => {
                let Some(samples) = event.data.get("samples") else {
                    return;
                };
                let mut sessions = self.sessions.lock().await;
                // Rows arriving before any meta still get a file, so nothing is
                // lost when a logger reconnects mid-session.
                if !sessions.contains_key(&event.room_id) {
                    match open_session(dir, &event.room_id).await {
                        Ok(file) => {
                            sessions.insert(event.room_id.clone(), file);
                        }
                        Err(error) => {
                            warn!(room = %event.room_id, %error, "failed to open telemetry file");
                            return;
                        }
                    }
                }
                let line = json!({ "recv_ms": recv_ms, "samples": samples });
                write_session(&mut sessions, &event.room_id, line).await;
            }
            "podium" => {}
            _ => {
                let room_dir = dir.join(safe_name(&event.room_id));
                let result = async {
                    fs::create_dir_all(&room_dir).await?;
                    let mut file = OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(room_dir.join("events.ndjson"))
                        .await?;
                    let mut line = serde_json::to_vec(event)?;
                    line.push(b'\n');
                    file.write_all(&line).await
                }
                .await;
                if let Err(error) = result {
                    warn!(room = %event.room_id, %error, "failed to append telemetry event");
                }
            }
        }
    }
}

async fn open_session(dir: &Path, room_id: &str) -> std::io::Result<File> {
    let room_dir = dir.join(safe_name(room_id));
    fs::create_dir_all(&room_dir).await?;
    let stamp = utc_stamp(now_ms() / 1000);
    // Two sessions in the same second (quick reconnect) get a suffix.
    let mut path = room_dir.join(format!("{stamp}Z.jsonl"));
    let mut n = 1;
    while fs::try_exists(&path).await.unwrap_or(false) {
        path = room_dir.join(format!("{stamp}Z-{n}.jsonl"));
        n += 1;
    }
    info!(path = %path.display(), "telemetry session started");
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

async fn write_session(sessions: &mut HashMap<String, File>, room_id: &str, value: Value) {
    let Some(file) = sessions.get_mut(room_id) else {
        return;
    };
    let mut line = value.to_string();
    line.push('\n');
    // Write straight through (no buffering) so a crash loses at most the batch
    // in flight.
    if let Err(error) = file.write_all(line.as_bytes()).await {
        warn!(%room_id, %error, "failed to write telemetry; reopening on next batch");
        sessions.remove(room_id);
    }
}

/// Room IDs come from the URL; keep only characters that are safe in a path.
fn safe_name(room_id: &str) -> String {
    let name: String = room_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.is_empty() {
        "_".into()
    } else {
        name
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `YYYYMMDD-HHMMSS` in UTC, without pulling in a date crate.
fn utc_stamp(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + if month <= 2 { 1 } else { 0 };
    format!(
        "{year:04}{month:02}{day:02}-{:02}{:02}{:02}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(source: &str, received_at: &str, data: Value) -> TelemetryEvent {
        let mut event = TelemetryEvent::new("room", source.into(), None, data);
        event.received_at = received_at.into();
        event
    }

    #[test]
    fn utc_stamp_matches_known_dates() {
        assert_eq!(utc_stamp(0), "19700101-000000");
        assert_eq!(utc_stamp(951_782_400), "20000229-000000");
        // 2026-10-14 16:44:53 UTC
        assert_eq!(utc_stamp(1_791_996_293), "20261014-164453");
    }

    #[test]
    fn safe_name_strips_path_characters() {
        assert_eq!(safe_name("main"), "main");
        assert_eq!(safe_name("../etc"), "___etc");
        assert_eq!(safe_name("a/b\\c"), "a_b_c");
    }

    #[tokio::test]
    async fn writes_meta_and_raw_rows_to_one_file_per_session() {
        let dir = std::env::temp_dir().join(format!("telemetry-files-test-{}", now_ms()));
        let files = TelemetryFiles::new(Some(dir.clone()));
        files
            .append(&event("podium.raw", "1", json!({"samples": [[1]]})))
            .await; // before meta
        files
            .append(&event("podium.meta", "2", json!({"meta": [{"nm": "Utc"}]})))
            .await;
        files
            .append(&event(
                "podium.raw",
                "3",
                json!({"samples": [[2, 1], [3, 1]]}),
            ))
            .await;
        files.append(&event("podium", "3", json!({"Utc": 2}))).await;
        files
            .append(&event("browser", "4", json!({"speed_kmh": 12.4})))
            .await;

        let mut entries = fs::read_dir(dir.join("room")).await.unwrap();
        let mut sessions = Vec::new();
        let mut others = Vec::new();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let text = fs::read_to_string(entry.path()).await.unwrap();
            if entry.file_name().to_string_lossy().ends_with(".jsonl") {
                sessions.push(text);
            } else {
                others.push(text);
            }
        }
        sessions.sort_by_key(|text| text.contains("meta"));
        assert_eq!(sessions.len(), 2, "meta must start a new session file");
        assert_eq!(sessions[0].lines().count(), 1);
        let session: Vec<Value> = sessions[1]
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(session.len(), 2, "decoded rows are not repeated");
        assert_eq!(session[0], json!({"recv_ms": 2, "meta": [{"nm": "Utc"}]}));
        assert_eq!(
            session[1],
            json!({"recv_ms": 3, "samples": [[2, 1], [3, 1]]})
        );
        assert_eq!(others.len(), 1);
        let other: Value = serde_json::from_str(others[0].trim()).unwrap();
        assert_eq!(other["source"], "browser");
        assert_eq!(other["data"]["speed_kmh"], 12.4);
        fs::remove_dir_all(dir).await.unwrap();
    }
}
