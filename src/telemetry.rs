//! Turns a logger's Podium rows into named values for the web client.
//!
//! A logger sends `podium.meta` (channel definitions, `nm` = name) once and
//! then `podium.samples` rows in Podium's `d` layout: the values of the
//! channels present in that sample, in meta order, followed by one 32-bit
//! presence mask per 32 channels. The web map looks values up by name
//! (`Latitude`/`Longitude`), so the server labels the rows here.

use serde_json::{Map, Value};

/// Channel names in meta order. A definition without a name keeps its slot
/// (so later channels still line up) under a placeholder.
pub fn channel_names(meta: &[Value]) -> Vec<String> {
    meta.iter()
        .enumerate()
        .map(|(index, channel)| {
            channel
                .get("nm")
                .and_then(Value::as_str)
                .map_or_else(|| format!("ch{index}"), str::to_owned)
        })
        .collect()
}

/// `{name: value}` for the channels present in `row`, or `None` when the row
/// does not fit the names (no meta yet, or meta and row out of step).
pub fn named_row(names: &[String], row: &[Value]) -> Option<Map<String, Value>> {
    if names.is_empty() {
        return None;
    }
    let mask_words = names.len().div_ceil(32);
    let split = row.len().checked_sub(mask_words)?;
    let (values, masks) = row.split_at(split);
    let masks: Vec<u64> = masks.iter().map(Value::as_u64).collect::<Option<_>>()?;
    let mut values = values.iter();
    let mut named = Map::new();
    for (index, name) in names.iter().enumerate() {
        if masks[index / 32] & (1 << (index % 32)) != 0 {
            named.insert(name.clone(), values.next()?.clone());
        }
    }
    // Leftover values mean the row was built from a different meta.
    values.next().is_none().then_some(named)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn names(list: &[&str]) -> Vec<String> {
        list.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn names_follow_meta_order() {
        let meta = vec![
            json!({"nm":"Utc"}),
            json!({"ut":"m"}),
            json!({"nm":"Latitude"}),
        ];
        assert_eq!(channel_names(&meta), names(&["Utc", "ch1", "Latitude"]));
    }

    #[test]
    fn labels_only_present_channels() {
        let names = names(&["Utc", "Speed", "Latitude", "Longitude"]);
        // Speed missing: mask 0b1101.
        let row = vec![
            json!(1_760_000_000_000_u64),
            json!(35.5),
            json!(139.7),
            json!(13),
        ];
        let named = named_row(&names, &row).unwrap();
        assert_eq!(named.len(), 3);
        assert_eq!(named["Latitude"], json!(35.5));
        assert_eq!(named["Longitude"], json!(139.7));
        assert!(!named.contains_key("Speed"));
    }

    #[test]
    fn reads_the_second_mask_word() {
        let names: Vec<String> = (0..33).map(|i| format!("c{i}")).collect();
        let row = vec![json!(1), json!(2), json!(1), json!(1)];
        let named = named_row(&names, &row).unwrap();
        assert_eq!(named["c0"], json!(1));
        assert_eq!(named["c32"], json!(2));
    }

    #[test]
    fn rejects_rows_that_do_not_fit() {
        let names = names(&["A", "B"]);
        assert!(named_row(&[], &[json!(1), json!(1)]).is_none());
        assert!(named_row(&names, &[]).is_none());
        // Mask says two values, row has one.
        assert!(named_row(&names, &[json!(1), json!(3)]).is_none());
        // Mask says one value, row has two.
        assert!(named_row(&names, &[json!(1), json!(2), json!(1)]).is_none());
        assert!(named_row(&names, &[json!(1), json!("x")]).is_none());
    }
}
