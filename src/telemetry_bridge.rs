//! Translate Podium rows into named telemetry for the web map.
//!
//! Podium rows contain one value per channel followed by one 32-bit validity
//! mask per 32 channels. Metadata is kept by the owning logger connection,
//! never by room, so a reconnect cannot reuse another logger's channel order.

use serde_json::{Map, Value};
use tracing::warn;

use crate::{cluster::ClusterKind, socketio_api::TelemetryEvent, AppState};

/// Persist once, then publish the same named event through both transports.
pub async fn publish(
    state: &AppState,
    room: &str,
    source: String,
    timestamp: Option<String>,
    data: Value,
) {
    let mut event = TelemetryEvent::new(room, source.clone(), timestamp.clone(), data.clone());
    let envelope = state.cluster.event(ClusterKind::Telemetry {
        event: event.clone(),
    });
    let event_id = state.cluster.event_id(envelope.sequence);
    event.event_id = Some(event_id.clone());
    match state.map.append_replicated_telemetry(
        &event_id,
        room,
        &source,
        timestamp.as_deref(),
        &data,
    ) {
        Ok(true) => {
            state.cluster.publish(crate::cluster::ClusterEvent {
                kind: ClusterKind::Telemetry {
                    event: event.clone(),
                },
                ..envelope
            });
            state.telemetry.append(event.clone()).await;
            let raw_event = serde_json::json!({
                "type":"telemetry", "event_id":event_id, "source":source,
                "timestamp":timestamp, "data":data,
            });
            state
                .rooms
                .broadcast_control(room, raw_event.to_string())
                .await;
            let _ = state.telemetry_updates.send(event);
        }
        Ok(false) => {}
        Err(error) => warn!(%error, room, "failed to persist telemetry"),
    }
}

#[derive(Default)]
pub struct PodiumChannels {
    names: Vec<String>,
}

impl PodiumChannels {
    pub fn update(&mut self, meta: &[Value]) {
        // Always forget the previous order on a new meta message, including
        // malformed messages, to avoid publishing incorrectly labeled values.
        self.names.clear();
        if meta.is_empty() || meta.len() > 128 {
            return;
        }
        let names: Option<Vec<_>> = meta
            .iter()
            .map(|entry| {
                entry
                    .get("nm")
                    .and_then(Value::as_str)
                    .filter(|name| {
                        !name.is_empty()
                            && name.len() <= 32
                            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    })
                    .map(str::to_owned)
            })
            .collect();
        if let Some(names) = names {
            self.names = names;
        }
    }

    pub fn decode(&self, row: &[Value]) -> Option<Value> {
        let count = self.names.len();
        if count == 0 || row.len() != count + count.div_ceil(32) {
            return None;
        }
        let masks: Option<Vec<u32>> = row[count..].iter().map(decode_mask).collect();
        let masks = masks?;
        let mut data = Map::new();
        for (index, name) in self.names.iter().enumerate() {
            if (masks[index / 32] & (1u32 << (index % 32))) == 0 {
                continue;
            }
            let value = &row[index];
            if !value.is_number() {
                return None;
            }
            data.insert(name.clone(), value.clone());
            // The browser accepts lowercase location keys. Keep the exact
            // Podium channel names as well for other telemetry consumers.
            if name.eq_ignore_ascii_case("latitude") {
                data.insert("latitude".into(), value.clone());
            } else if name.eq_ignore_ascii_case("longitude") {
                data.insert("longitude".into(), value.clone());
            }
        }
        (!data.is_empty()).then_some(Value::Object(data))
    }
}

fn decode_mask(value: &Value) -> Option<u32> {
    // Signed 32-bit masks appear when a logger uses JavaScript bit operators.
    value
        .as_u64()
        .and_then(|mask| u32::try_from(mask).ok())
        .or_else(|| {
            value
                .as_i64()
                .filter(|mask| (i32::MIN as i64..0).contains(mask))
                .map(|mask| (mask as i32) as u32)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn converts_podium_position_into_map_keys_and_respects_mask() {
        let mut channels = PodiumChannels::default();
        channels.update(&[
            json!({"nm":"Utc"}),
            json!({"nm":"Latitude"}),
            json!({"nm":"Longitude"}),
            json!({"nm":"Speed"}),
        ]);
        let telemetry = channels
            .decode(&[json!(1000), json!(35.5), json!(139.7), json!(41), json!(15)])
            .unwrap();
        assert_eq!(
            telemetry,
            json!({"Utc":1000,"Latitude":35.5,"Longitude":139.7,"Speed":41,"latitude":35.5,"longitude":139.7})
        );
        let no_gps = channels
            .decode(&[json!(2000), json!(0), json!(0), json!(42), json!(9)])
            .unwrap();
        assert_eq!(no_gps, json!({"Utc":2000,"Speed":42}));
    }

    #[test]
    fn mask_words_follow_every_32_channels() {
        let mut channels = PodiumChannels::default();
        let meta: Vec<_> = (0..34).map(|i| json!({"nm":format!("Field{i}")})).collect();
        channels.update(&meta);
        let mut row: Vec<_> = (0..34).map(|i| json!(i)).collect();
        row.extend([json!(0x8000_0001_u32), json!(2)]);
        assert_eq!(
            channels.decode(&row).unwrap(),
            json!({"Field0":0,"Field31":31,"Field33":33})
        );
        row[34] = json!(-2147483647_i64);
        assert_eq!(
            channels.decode(&row).unwrap(),
            json!({"Field0":0,"Field31":31,"Field33":33})
        );
    }

    #[test]
    fn updated_meta_replaces_channel_order_and_invalid_meta_clears_it() {
        let mut channels = PodiumChannels::default();
        channels.update(&[json!({"nm":"Latitude"}), json!({"nm":"Longitude"})]);
        assert_eq!(
            channels.decode(&[json!(35), json!(139), json!(3)]).unwrap()["latitude"],
            35
        );
        channels.update(&[json!({"nm":"Longitude"}), json!({"nm":"Latitude"})]);
        assert_eq!(
            channels.decode(&[json!(139), json!(35), json!(3)]).unwrap()["latitude"],
            35
        );
        channels.update(&[json!({"name":"Latitude"})]);
        assert!(channels.decode(&[json!(35), json!(1)]).is_none());
    }

    #[test]
    fn channel_order_is_local_to_each_logger_connection() {
        let mut first_logger = PodiumChannels::default();
        let mut second_logger = PodiumChannels::default();
        first_logger.update(&[json!({"nm":"Latitude"}), json!({"nm":"Longitude"})]);
        second_logger.update(&[json!({"nm":"Longitude"}), json!({"nm":"Latitude"})]);

        assert_eq!(
            first_logger
                .decode(&[json!(35), json!(139), json!(3)])
                .unwrap()["latitude"],
            35
        );
        assert_eq!(
            second_logger
                .decode(&[json!(139), json!(36), json!(3)])
                .unwrap()["latitude"],
            36
        );
    }

    #[test]
    fn malformed_rows_and_unset_masks_are_not_published() {
        let mut channels = PodiumChannels::default();
        assert!(channels.decode(&[json!(3), json!(1)]).is_none());
        channels.update(&[json!({"nm":"Latitude"})]);
        for row in [
            vec![json!(12)],
            vec![json!(12), json!(0)],
            vec![json!(12), json!(1.5)],
            vec![json!(12), json!(4294967296_u64)],
            vec![json!("bad"), json!(1)],
        ] {
            assert!(
                channels.decode(&row).is_none(),
                "row should be rejected: {row:?}"
            );
        }
    }
}
