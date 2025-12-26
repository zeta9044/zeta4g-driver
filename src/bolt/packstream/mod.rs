//! PackStream serialization format.
//!
//! PackStream is the binary serialization format used by the Bolt protocol
//! to encode values for transmission between client and server.
//!
//! # Supported Types
//!
//! - **Null**: Single byte marker
//! - **Boolean**: True/False markers
//! - **Integer**: Variable-length encoding (-2^63 to 2^63-1)
//! - **Float**: 64-bit IEEE 754
//! - **String**: UTF-8 encoded, variable length prefix
//! - **Bytes**: Raw bytes, variable length prefix
//! - **List**: Homogeneous or heterogeneous collections
//! - **Map**: String keys to arbitrary values
//! - **Structure**: Tagged structures for graph types
//!
//! # Graph Structures
//!
//! - **Node**: id, labels, properties
//! - **Relationship**: id, start_id, end_id, type, properties
//! - **Path**: nodes, relationships, indices
//!
//! # Temporal Structures
//!
//! - **Date**: Days since Unix epoch
//! - **Time**: Nanoseconds since midnight + timezone
//! - **LocalTime**: Nanoseconds since midnight (no timezone)
//! - **DateTime**: Seconds + nanoseconds + timezone
//! - **LocalDateTime**: Seconds + nanoseconds (no timezone)
//! - **Duration**: Months, days, seconds, nanoseconds
//!
//! # Spatial Structures
//!
//! - **Point2D**: SRID + x, y coordinates
//! - **Point3D**: SRID + x, y, z coordinates

pub mod decoder;
pub mod encoder;
pub mod marker;
pub mod structures;
pub mod types;

pub use decoder::{decode, PackStreamDecoder};
pub use encoder::{encode, PackStreamEncoder};
pub use marker::*;
pub use structures::{
    PackStreamDate, PackStreamDateTime, PackStreamDuration, PackStreamLocalDateTime,
    PackStreamLocalTime, PackStreamNode, PackStreamPath, PackStreamPoint2D, PackStreamPoint3D,
    PackStreamRelationship, PackStreamTime, PackStreamUnboundRelationship,
};
pub use types::{PackStreamStructure, PackStreamValue};

use std::fmt;

/// PackStream errors.
#[derive(Debug, Clone)]
pub enum PackStreamError {
    /// Unexpected end of input
    UnexpectedEof,
    /// Unknown marker byte
    UnknownMarker(u8),
    /// Invalid UTF-8 in string
    InvalidUtf8(String),
    /// Invalid map key (must be string)
    InvalidMapKey,
    /// Value too large to encode
    ValueTooLarge(&'static str, usize),
    /// Invalid structure format
    InvalidStructure(String),
}

impl fmt::Display for PackStreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PackStreamError::UnexpectedEof => write!(f, "Unexpected end of PackStream data"),
            PackStreamError::UnknownMarker(m) => write!(f, "Unknown PackStream marker: 0x{:02X}", m),
            PackStreamError::InvalidUtf8(e) => write!(f, "Invalid UTF-8 in string: {}", e),
            PackStreamError::InvalidMapKey => write!(f, "Map keys must be strings"),
            PackStreamError::ValueTooLarge(t, s) => write!(f, "{} too large: {} bytes", t, s),
            PackStreamError::InvalidStructure(msg) => write!(f, "Invalid structure: {}", msg),
        }
    }
}

impl std::error::Error for PackStreamError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_roundtrip_null() {
        let value = PackStreamValue::Null;
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_roundtrip_bool() {
        for v in [true, false] {
            let value = PackStreamValue::Boolean(v);
            let bytes = encode(&value).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_roundtrip_int() {
        for v in [0i64, 1, -1, 127, -16, 128, -128, 1000, -1000, i64::MAX, i64::MIN] {
            let value = PackStreamValue::Integer(v);
            let bytes = encode(&value).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(value, decoded, "Failed for {}", v);
        }
    }

    #[test]
    fn test_roundtrip_float() {
        for v in [0.0f64, 1.0, -1.0, 3.14159, f64::MAX, f64::MIN] {
            let value = PackStreamValue::Float(v);
            let bytes = encode(&value).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_roundtrip_string() {
        for s in ["", "a", "hello", "hello world", &"x".repeat(100), &"y".repeat(1000)] {
            let value = PackStreamValue::String(s.to_string());
            let bytes = encode(&value).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_roundtrip_bytes() {
        for b in [vec![], vec![1u8], vec![1, 2, 3], vec![0u8; 100]] {
            let value = PackStreamValue::Bytes(b);
            let bytes = encode(&value).unwrap();
            let decoded = decode(&bytes).unwrap();
            assert_eq!(value, decoded);
        }
    }

    #[test]
    fn test_roundtrip_list() {
        let value = PackStreamValue::List(vec![
            PackStreamValue::Integer(1),
            PackStreamValue::String("two".into()),
            PackStreamValue::Boolean(true),
            PackStreamValue::Null,
        ]);
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_roundtrip_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), PackStreamValue::Integer(1));
        map.insert("b".to_string(), PackStreamValue::String("hello".into()));
        map.insert("c".to_string(), PackStreamValue::Boolean(false));

        let value = PackStreamValue::Map(map);
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();

        // Compare maps element by element due to HashMap ordering
        if let (PackStreamValue::Map(m1), PackStreamValue::Map(m2)) = (&value, &decoded) {
            assert_eq!(m1.len(), m2.len());
            for (k, v) in m1 {
                assert_eq!(m2.get(k), Some(v));
            }
        } else {
            panic!("Expected maps");
        }
    }

    #[test]
    fn test_roundtrip_structure() {
        let s = PackStreamStructure::new(0x4E, vec![
            PackStreamValue::Integer(1),
            PackStreamValue::List(vec![PackStreamValue::String("Person".into())]),
            PackStreamValue::Map(HashMap::new()),
        ]);
        let value = PackStreamValue::Structure(s);
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        assert_eq!(value, decoded);
    }

    #[test]
    fn test_roundtrip_node() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), PackStreamValue::String("Alice".into()));

        let node = PackStreamNode::new(1, vec!["Person".to_string()], props);
        let value = node.to_value();
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        let parsed = PackStreamNode::from_value(&decoded).unwrap();

        assert_eq!(node.id, parsed.id);
        assert_eq!(node.labels, parsed.labels);
    }

    #[test]
    fn test_roundtrip_date() {
        let date = PackStreamDate::new(18628);
        let value = date.to_value();
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        let parsed = PackStreamDate::from_value(&decoded).unwrap();
        assert_eq!(date.days, parsed.days);
    }

    #[test]
    fn test_roundtrip_duration() {
        let dur = PackStreamDuration::new(12, 30, 3600, 500);
        let value = dur.to_value();
        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();
        let parsed = PackStreamDuration::from_value(&decoded).unwrap();
        assert_eq!(dur, parsed);
    }

    #[test]
    fn test_deeply_nested() {
        // List of maps of lists
        let mut inner_map = HashMap::new();
        inner_map.insert(
            "items".to_string(),
            PackStreamValue::List(vec![
                PackStreamValue::Integer(1),
                PackStreamValue::Integer(2),
            ]),
        );

        let value = PackStreamValue::List(vec![
            PackStreamValue::Map(inner_map.clone()),
            PackStreamValue::Map(inner_map),
        ]);

        let bytes = encode(&value).unwrap();
        let decoded = decode(&bytes).unwrap();

        if let PackStreamValue::List(l) = decoded {
            assert_eq!(l.len(), 2);
        } else {
            panic!("Expected list");
        }
    }
}
