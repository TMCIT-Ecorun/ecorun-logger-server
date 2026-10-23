#[allow(dead_code)]
#[path = "../src/map.rs"]
mod map;

use map::MapStore;
use serde_json::json;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestDatabase {
    directory: PathBuf,
    path: PathBuf,
}

impl TestDatabase {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ecorun-replication-{}-{nonce}-{}",
            std::process::id(),
            NEXT_PATH.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("telemetry.sqlite");
        Self { directory, path }
    }

    fn open(&self) -> MapStore {
        MapStore::open(&self.path).unwrap()
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.directory).unwrap();
    }
}

#[test]
fn replicated_events_are_idempotent_across_connections_and_restarts() {
    let database = TestDatabase::new();
    let primary = database.open();
    let secondary = database.open();
    let data = json!({"Latitude": 35.7, "Speed": 12.0});

    assert!(primary
        .append_replicated_telemetry("node-A:1", "race", "podium.samples", Some("1234"), &data)
        .unwrap());
    assert!(!secondary
        .append_replicated_telemetry("node-A:1", "race", "podium.samples", Some("1234"), &data)
        .unwrap());
    // A replay with changed content or another room cannot overwrite the first event.
    assert!(!secondary
        .append_replicated_telemetry("node-A:1", "other", "other", None, &json!({"Speed": 1}))
        .unwrap());

    let saved = primary.recent_telemetry("race", 10).unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].event_id.as_deref(), Some("node-A:1"));
    assert_eq!(saved[0].timestamp.as_deref(), Some("1234"));
    assert_eq!(saved[0].data, data);
    assert!(secondary.recent_telemetry("other", 10).unwrap().is_empty());

    drop(primary);
    drop(secondary);
    let reopened = database.open();
    assert!(!reopened
        .append_replicated_telemetry("node-A:1", "race", "podium.samples", None, &data)
        .unwrap());
    assert_eq!(reopened.recent_telemetry("race", 10).unwrap().len(), 1);
}

#[test]
fn cursor_pages_only_replicated_rows_in_local_id_order() {
    let database = TestDatabase::new();
    let store = database.open();
    let data = json!({"speed": 17});

    store
        .append_telemetry("race", "legacy", None, &data)
        .unwrap();
    assert!(store
        .append_replicated_telemetry("a", "race", "logger", None, &data)
        .unwrap());
    assert!(store
        .append_replicated_telemetry("other", "test", "logger", None, &data)
        .unwrap());
    assert!(store
        .append_replicated_telemetry("b", "race", "logger", None, &data)
        .unwrap());
    store
        .append_telemetry("race", "legacy", None, &data)
        .unwrap();
    assert!(store
        .append_replicated_telemetry("c", "race", "logger", None, &data)
        .unwrap());

    let first = store.replicated_telemetry_since("race", 0, 2).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|v| v.event_id.as_deref())
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("b")]
    );
    assert!(first[0].id < first[1].id);
    let second = store
        .replicated_telemetry_since("race", first[1].id, 2)
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].event_id.as_deref(), Some("c"));
    assert!(store
        .replicated_telemetry_since("race", second[0].id, 2)
        .unwrap()
        .is_empty());
    assert!(store
        .replicated_telemetry_since("race", 0, 0)
        .unwrap()
        .is_empty());

    let all = store.recent_telemetry("race", 10).unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(
        all.iter()
            .filter(|record| record.event_id.is_none())
            .count(),
        2
    );
    assert_eq!(all[0].event_id.as_deref(), Some("c"));
}

#[test]
fn opening_old_database_migrates_without_losing_rows_or_ids() {
    let database = TestDatabase::new();
    {
        let legacy = rusqlite::Connection::open(&database.path).unwrap();
        legacy
            .execute_batch(
                r#"CREATE TABLE telemetry (
                 id INTEGER PRIMARY KEY AUTOINCREMENT, room_id TEXT NOT NULL, source TEXT NOT NULL,
                 timestamp TEXT, data_json TEXT NOT NULL,
                 received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
             );
             INSERT INTO telemetry (room_id,source,timestamp,data_json)
             VALUES ('race','legacy','2026-10-19','{"speed":23}');"#,
            )
            .unwrap();
    }

    let store = database.open();
    let previous = store.recent_telemetry("race", 10).unwrap();
    assert_eq!(previous.len(), 1);
    assert_eq!(previous[0].id, 1);
    assert_eq!(previous[0].event_id, None);
    assert_eq!(previous[0].timestamp.as_deref(), Some("2026-10-19"));
    assert_eq!(previous[0].data, json!({"speed":23}));
    assert!(store
        .append_replicated_telemetry("first", "race", "logger", None, &json!({"speed":24}))
        .unwrap());
    assert_eq!(
        store.replicated_telemetry_since("race", 0, 10).unwrap()[0].id,
        2
    );
    drop(store);

    // Schema upgrades are repeatable, and uniqueness holds after reopening.
    let reopened = database.open();
    assert!(!reopened
        .append_replicated_telemetry("first", "race", "logger", None, &json!({"speed":24}))
        .unwrap());
    assert_eq!(reopened.recent_telemetry("race", 10).unwrap().len(), 2);
}

#[test]
fn invalid_ids_and_extreme_page_limits_do_not_create_duplicate_rows() {
    let database = TestDatabase::new();
    let store = database.open();
    assert!(store
        .append_replicated_telemetry("   ", "race", "logger", None, &json!(1))
        .is_err());
    assert!(store
        .append_replicated_telemetry("valid", "race", "logger", None, &json!(1))
        .unwrap());
    assert_eq!(
        store
            .replicated_telemetry_since("race", 0, usize::MAX)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.recent_telemetry("race", usize::MAX).unwrap().len(), 1);
}
