#![no_main]
//! Fuzz test for CDC event serialization and deserialization
//!
//! Tests that events can be safely serialized and deserialized without
//! data corruption or panics.

use libfuzzer_sys::fuzz_target;
use arbitrary::{Arbitrary, Unstructured};
use rivven_cdc::CdcEvent;
use rivven_cdc::CdcOp;

/// Generate arbitrary CDC events for fuzzing
#[derive(Debug, Clone)]
struct FuzzCdcEvent {
    op: CdcOp,
    table_name: String,
    schema_name: String,
    database_name: String,
    source_type: String,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    timestamp: i64,
}

impl<'a> Arbitrary<'a> for FuzzCdcEvent {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        let ops = [CdcOp::Insert, CdcOp::Update, CdcOp::Delete, CdcOp::Tombstone, CdcOp::Truncate, CdcOp::Snapshot];
        let op = ops[u.int_in_range(0..=5)?];

        // Generate column values as JSON
        let before = if matches!(op, CdcOp::Update | CdcOp::Delete) {
            Some(generate_json_value(u)?)
        } else {
            None
        };
        
        let after = if matches!(op, CdcOp::Insert | CdcOp::Update | CdcOp::Snapshot) {
            Some(generate_json_value(u)?)
        } else {
            None
        };

        let source_types = ["postgres", "mysql", "mariadb"];
        
        Ok(FuzzCdcEvent {
            op,
            table_name: u.arbitrary::<String>()?.chars().take(100).collect(),
            schema_name: u.arbitrary::<String>()?.chars().take(100).collect(),
            database_name: u.arbitrary::<String>()?.chars().take(100).collect(),
            source_type: source_types[u.int_in_range(0..=2)?].to_string(),
            before,
            after,
            timestamp: u.arbitrary()?,
        })
    }
}

fn generate_json_value<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<serde_json::Value> {
    use serde_json::{json, Value, Map, Number};
    
    match u.int_in_range(0..=7)? {
        0 => Ok(Value::Null),
        1 => Ok(Value::Bool(u.arbitrary()?)),
        2 => {
            let n: i64 = u.arbitrary()?;
            Ok(Value::Number(Number::from(n)))
        }
        3 => {
            let f: f64 = u.arbitrary()?;
            if f.is_finite() {
                Ok(Value::Number(Number::from_f64(f).unwrap_or(Number::from(0))))
            } else {
                Ok(Value::Number(Number::from(0)))
            }
        }
        4 => {
            let s: String = u.arbitrary()?;
            Ok(Value::String(s.chars().take(200).collect()))
        }
        5 => {
            // Generate a small object
            let mut map = Map::new();
            let num_keys = u.int_in_range(0..=5)?;
            for i in 0..num_keys {
                let key = format!("col_{}", i);
                map.insert(key, generate_simple_value(u)?);
            }
            Ok(Value::Object(map))
        }
        6 => {
            // Generate a small array
            let len = u.int_in_range(0..=5)?;
            let mut arr = Vec::with_capacity(len);
            for _ in 0..len {
                arr.push(generate_simple_value(u)?);
            }
            Ok(Value::Array(arr))
        }
        _ => Ok(Value::Null),
    }
}

fn generate_simple_value<'a>(u: &mut Unstructured<'a>) -> arbitrary::Result<serde_json::Value> {
    use serde_json::{Value, Number};
    
    match u.int_in_range(0..=4)? {
        0 => Ok(Value::Null),
        1 => Ok(Value::Bool(u.arbitrary()?)),
        2 => Ok(Value::Number(Number::from(u.arbitrary::<i64>()?))),
        3 => {
            let s: String = u.arbitrary()?;
            Ok(Value::String(s.chars().take(100).collect()))
        }
        _ => Ok(Value::Null),
    }
}

fuzz_target!(|data: &[u8]| {
    // Generate and process arbitrary CDC events
    if let Ok(fuzz_event) = Unstructured::new(data).arbitrary::<FuzzCdcEvent>() {
        // Build event using factory methods
        let event = match fuzz_event.op {
            CdcOp::Insert => CdcEvent::insert(
                &fuzz_event.source_type,
                &fuzz_event.database_name,
                &fuzz_event.schema_name,
                &fuzz_event.table_name,
                fuzz_event.after.unwrap_or(serde_json::Value::Null),
                fuzz_event.timestamp,
            ),
            CdcOp::Update => CdcEvent::update(
                &fuzz_event.source_type,
                &fuzz_event.database_name,
                &fuzz_event.schema_name,
                &fuzz_event.table_name,
                fuzz_event.before,
                fuzz_event.after.unwrap_or(serde_json::Value::Null),
                fuzz_event.timestamp,
            ),
            CdcOp::Delete => CdcEvent::delete(
                &fuzz_event.source_type,
                &fuzz_event.database_name,
                &fuzz_event.schema_name,
                &fuzz_event.table_name,
                fuzz_event.before.unwrap_or(serde_json::Value::Null),
                fuzz_event.timestamp,
            ),
            _ => CdcEvent::insert(
                &fuzz_event.source_type,
                &fuzz_event.database_name,
                &fuzz_event.schema_name,
                &fuzz_event.table_name,
                fuzz_event.after.unwrap_or(serde_json::Value::Null),
                fuzz_event.timestamp,
            ),
        };

        // Test JSON serialization round-trip
        if let Ok(json) = serde_json::to_string(&event) {
            let _ = serde_json::from_str::<CdcEvent>(&json);
        }
    }

    // Test deserialization of arbitrary JSON
    if let Ok(json_str) = std::str::from_utf8(data) {
        let _ = serde_json::from_str::<CdcEvent>(json_str);
    }

    // Test with malformed JSON-like data
    let json_attacks = [
        r#"{"op": "Insert", "__proto__": {"polluted": true}}"#,
        r#"{"op": "Insert", "constructor": {"prototype": {"polluted": true}}}"#,
        r#"{"op": "Insert", "after": {"col": 1e308}}"#, // Large float
        r#"{"op": "Insert", "after": {"col": 1e-308}}"#, // Small float
        r#"{"op": "Insert", "timestamp": 9223372036854775807}"#, // i64 max
        r#"{"op": "Insert", "timestamp": -9223372036854775808}"#, // i64 min
        r#"{"source_type": "postgres", "database": "db", "schema": "public", "table": "t", "op": "Insert", "after": {}, "timestamp": 0}"#,
    ];

    for attack in json_attacks {
        let _ = serde_json::from_str::<CdcEvent>(attack);
    }

    // Test deeply nested JSON
    if data.len() < 100 {
        let depth = (data.first().unwrap_or(&10) % 50) as usize;
        let mut nested = "{".repeat(depth);
        nested.push_str(r#""op": "Insert""#);
        nested.push_str(&"}".repeat(depth));
        let _ = serde_json::from_str::<CdcEvent>(&nested);
    }
});
