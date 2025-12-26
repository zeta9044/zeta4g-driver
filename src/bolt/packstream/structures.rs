//! PackStream structure types for graph and temporal data.

use std::collections::HashMap;

use super::marker::*;
use super::types::{PackStreamStructure, PackStreamValue};
use super::PackStreamError;

/// A Node structure in PackStream format.
#[derive(Debug, Clone, PartialEq)]
pub struct PackStreamNode {
    /// Node ID
    pub id: i64,
    /// Node labels
    pub labels: Vec<String>,
    /// Node properties
    pub properties: HashMap<String, PackStreamValue>,
    /// Element ID (Neo4j 5.x)
    pub element_id: Option<String>,
}

impl PackStreamNode {
    /// Create a new node.
    pub fn new(id: i64, labels: Vec<String>, properties: HashMap<String, PackStreamValue>) -> Self {
        Self {
            id,
            labels,
            properties,
            element_id: None,
        }
    }

    /// Set the element ID (Neo4j 5.x).
    pub fn with_element_id(mut self, element_id: String) -> Self {
        self.element_id = Some(element_id);
        self
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        let fields = if self.element_id.is_some() {
            // Neo4j 5.x format: 4 fields
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::List(
                    self.labels
                        .iter()
                        .map(|s| PackStreamValue::String(s.clone()))
                        .collect(),
                ),
                PackStreamValue::Map(self.properties.clone()),
                PackStreamValue::String(self.element_id.clone().unwrap_or_default()),
            ]
        } else {
            // Neo4j 4.x format: 3 fields
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::List(
                    self.labels
                        .iter()
                        .map(|s| PackStreamValue::String(s.clone()))
                        .collect(),
                ),
                PackStreamValue::Map(self.properties.clone()),
            ]
        };

        PackStreamValue::Structure(PackStreamStructure::new(NODE_TAG, fields))
    }

    /// Try to parse from a PackStreamValue.
    pub fn from_value(value: &PackStreamValue) -> Result<Self, PackStreamError> {
        let s = value
            .as_structure()
            .ok_or_else(|| PackStreamError::InvalidStructure("expected Node structure".into()))?;

        if s.tag != NODE_TAG {
            return Err(PackStreamError::InvalidStructure(format!(
                "expected Node tag 0x{:02X}, got 0x{:02X}",
                NODE_TAG, s.tag
            )));
        }

        if s.fields.len() < 3 {
            return Err(PackStreamError::InvalidStructure(
                "Node requires at least 3 fields".into(),
            ));
        }

        let id = s.fields[0]
            .as_int()
            .ok_or_else(|| PackStreamError::InvalidStructure("Node id must be integer".into()))?;

        let labels = s.fields[1]
            .as_list()
            .ok_or_else(|| PackStreamError::InvalidStructure("Node labels must be list".into()))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(|s| s.to_string())
                    .ok_or_else(|| PackStreamError::InvalidStructure("Label must be string".into()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let properties = s.fields[2]
            .as_map()
            .ok_or_else(|| PackStreamError::InvalidStructure("Node properties must be map".into()))?
            .clone();

        let element_id = if s.fields.len() > 3 {
            s.fields[3].as_str().map(|s| s.to_string())
        } else {
            None
        };

        Ok(Self {
            id,
            labels,
            properties,
            element_id,
        })
    }
}

/// A Relationship structure in PackStream format.
#[derive(Debug, Clone, PartialEq)]
pub struct PackStreamRelationship {
    /// Relationship ID
    pub id: i64,
    /// Start node ID
    pub start_node_id: i64,
    /// End node ID
    pub end_node_id: i64,
    /// Relationship type
    pub rel_type: String,
    /// Relationship properties
    pub properties: HashMap<String, PackStreamValue>,
    /// Element ID (Neo4j 5.x)
    pub element_id: Option<String>,
    /// Start node element ID (Neo4j 5.x)
    pub start_node_element_id: Option<String>,
    /// End node element ID (Neo4j 5.x)
    pub end_node_element_id: Option<String>,
}

impl PackStreamRelationship {
    /// Create a new relationship.
    pub fn new(
        id: i64,
        start_node_id: i64,
        end_node_id: i64,
        rel_type: String,
        properties: HashMap<String, PackStreamValue>,
    ) -> Self {
        Self {
            id,
            start_node_id,
            end_node_id,
            rel_type,
            properties,
            element_id: None,
            start_node_element_id: None,
            end_node_element_id: None,
        }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        let fields = if self.element_id.is_some() {
            // Neo4j 5.x format: 8 fields
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::Integer(self.start_node_id),
                PackStreamValue::Integer(self.end_node_id),
                PackStreamValue::String(self.rel_type.clone()),
                PackStreamValue::Map(self.properties.clone()),
                PackStreamValue::String(self.element_id.clone().unwrap_or_default()),
                PackStreamValue::String(self.start_node_element_id.clone().unwrap_or_default()),
                PackStreamValue::String(self.end_node_element_id.clone().unwrap_or_default()),
            ]
        } else {
            // Neo4j 4.x format: 5 fields
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::Integer(self.start_node_id),
                PackStreamValue::Integer(self.end_node_id),
                PackStreamValue::String(self.rel_type.clone()),
                PackStreamValue::Map(self.properties.clone()),
            ]
        };

        PackStreamValue::Structure(PackStreamStructure::new(RELATIONSHIP_TAG, fields))
    }

    /// Try to parse from a PackStreamValue.
    pub fn from_value(value: &PackStreamValue) -> Result<Self, PackStreamError> {
        let s = value.as_structure().ok_or_else(|| {
            PackStreamError::InvalidStructure("expected Relationship structure".into())
        })?;

        if s.tag != RELATIONSHIP_TAG {
            return Err(PackStreamError::InvalidStructure(format!(
                "expected Relationship tag 0x{:02X}, got 0x{:02X}",
                RELATIONSHIP_TAG, s.tag
            )));
        }

        if s.fields.len() < 5 {
            return Err(PackStreamError::InvalidStructure(
                "Relationship requires at least 5 fields".into(),
            ));
        }

        let id = s.fields[0].as_int().ok_or_else(|| {
            PackStreamError::InvalidStructure("Relationship id must be integer".into())
        })?;
        let start_node_id = s.fields[1].as_int().ok_or_else(|| {
            PackStreamError::InvalidStructure("start_node_id must be integer".into())
        })?;
        let end_node_id = s.fields[2].as_int().ok_or_else(|| {
            PackStreamError::InvalidStructure("end_node_id must be integer".into())
        })?;
        let rel_type = s.fields[3]
            .as_str()
            .ok_or_else(|| PackStreamError::InvalidStructure("rel_type must be string".into()))?
            .to_string();
        let properties = s.fields[4]
            .as_map()
            .ok_or_else(|| {
                PackStreamError::InvalidStructure("Relationship properties must be map".into())
            })?
            .clone();

        let (element_id, start_node_element_id, end_node_element_id) = if s.fields.len() > 5 {
            (
                s.fields.get(5).and_then(|v| v.as_str().map(String::from)),
                s.fields.get(6).and_then(|v| v.as_str().map(String::from)),
                s.fields.get(7).and_then(|v| v.as_str().map(String::from)),
            )
        } else {
            (None, None, None)
        };

        Ok(Self {
            id,
            start_node_id,
            end_node_id,
            rel_type,
            properties,
            element_id,
            start_node_element_id,
            end_node_element_id,
        })
    }
}

/// An unbound relationship (used in paths).
#[derive(Debug, Clone, PartialEq)]
pub struct PackStreamUnboundRelationship {
    /// Relationship ID
    pub id: i64,
    /// Relationship type
    pub rel_type: String,
    /// Relationship properties
    pub properties: HashMap<String, PackStreamValue>,
    /// Element ID (Neo4j 5.x)
    pub element_id: Option<String>,
}

impl PackStreamUnboundRelationship {
    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        let fields = if self.element_id.is_some() {
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::String(self.rel_type.clone()),
                PackStreamValue::Map(self.properties.clone()),
                PackStreamValue::String(self.element_id.clone().unwrap_or_default()),
            ]
        } else {
            vec![
                PackStreamValue::Integer(self.id),
                PackStreamValue::String(self.rel_type.clone()),
                PackStreamValue::Map(self.properties.clone()),
            ]
        };

        PackStreamValue::Structure(PackStreamStructure::new(UNBOUND_RELATIONSHIP_TAG, fields))
    }
}

/// A Path structure in PackStream format.
#[derive(Debug, Clone, PartialEq)]
pub struct PackStreamPath {
    /// Nodes in the path
    pub nodes: Vec<PackStreamNode>,
    /// Relationships in the path (unbound)
    pub relationships: Vec<PackStreamUnboundRelationship>,
    /// Indices describing the path traversal
    pub indices: Vec<i64>,
}

impl PackStreamPath {
    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        let nodes: Vec<PackStreamValue> = self.nodes.iter().map(|n| n.to_value()).collect();
        let rels: Vec<PackStreamValue> = self.relationships.iter().map(|r| r.to_value()).collect();
        let indices: Vec<PackStreamValue> = self
            .indices
            .iter()
            .map(|i| PackStreamValue::Integer(*i))
            .collect();

        PackStreamValue::Structure(PackStreamStructure::new(
            PATH_TAG,
            vec![
                PackStreamValue::List(nodes),
                PackStreamValue::List(rels),
                PackStreamValue::List(indices),
            ],
        ))
    }
}

/// Date structure (days since Unix epoch).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamDate {
    /// Days since Unix epoch (1970-01-01)
    pub days: i64,
}

impl PackStreamDate {
    /// Create from days since epoch.
    pub fn new(days: i64) -> Self {
        Self { days }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            DATE_TAG,
            vec![PackStreamValue::Integer(self.days)],
        ))
    }

    /// Try to parse from PackStreamValue.
    pub fn from_value(value: &PackStreamValue) -> Result<Self, PackStreamError> {
        let s = value
            .as_structure()
            .ok_or_else(|| PackStreamError::InvalidStructure("expected Date structure".into()))?;

        if s.tag != DATE_TAG {
            return Err(PackStreamError::InvalidStructure("expected Date tag".into()));
        }

        let days = s
            .fields
            .first()
            .and_then(|v| v.as_int())
            .ok_or_else(|| PackStreamError::InvalidStructure("Date requires days field".into()))?;

        Ok(Self { days })
    }
}

/// Time structure (nanoseconds since midnight + timezone offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamTime {
    /// Nanoseconds since midnight
    pub nanoseconds: i64,
    /// Timezone offset in seconds
    pub tz_offset_seconds: i32,
}

impl PackStreamTime {
    /// Create a new time.
    pub fn new(nanoseconds: i64, tz_offset_seconds: i32) -> Self {
        Self {
            nanoseconds,
            tz_offset_seconds,
        }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            TIME_TAG,
            vec![
                PackStreamValue::Integer(self.nanoseconds),
                PackStreamValue::Integer(self.tz_offset_seconds as i64),
            ],
        ))
    }
}

/// LocalTime structure (nanoseconds since midnight, no timezone).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamLocalTime {
    /// Nanoseconds since midnight
    pub nanoseconds: i64,
}

impl PackStreamLocalTime {
    /// Create a new local time.
    pub fn new(nanoseconds: i64) -> Self {
        Self { nanoseconds }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            LOCAL_TIME_TAG,
            vec![PackStreamValue::Integer(self.nanoseconds)],
        ))
    }
}

/// DateTime structure (seconds since epoch + nanoseconds + timezone offset).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamDateTime {
    /// Seconds since Unix epoch
    pub seconds: i64,
    /// Nanoseconds adjustment
    pub nanoseconds: i64,
    /// Timezone offset in seconds
    pub tz_offset_seconds: i32,
}

impl PackStreamDateTime {
    /// Create a new datetime.
    pub fn new(seconds: i64, nanoseconds: i64, tz_offset_seconds: i32) -> Self {
        Self {
            seconds,
            nanoseconds,
            tz_offset_seconds,
        }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            DATE_TIME_TAG,
            vec![
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanoseconds),
                PackStreamValue::Integer(self.tz_offset_seconds as i64),
            ],
        ))
    }
}

/// LocalDateTime structure (seconds since epoch + nanoseconds, no timezone).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamLocalDateTime {
    /// Seconds since Unix epoch
    pub seconds: i64,
    /// Nanoseconds adjustment
    pub nanoseconds: i64,
}

impl PackStreamLocalDateTime {
    /// Create a new local datetime.
    pub fn new(seconds: i64, nanoseconds: i64) -> Self {
        Self {
            seconds,
            nanoseconds,
        }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            LOCAL_DATE_TIME_TAG,
            vec![
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanoseconds),
            ],
        ))
    }
}

/// Duration structure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackStreamDuration {
    /// Months
    pub months: i64,
    /// Days
    pub days: i64,
    /// Seconds
    pub seconds: i64,
    /// Nanoseconds
    pub nanoseconds: i64,
}

impl PackStreamDuration {
    /// Create a new duration.
    pub fn new(months: i64, days: i64, seconds: i64, nanoseconds: i64) -> Self {
        Self {
            months,
            days,
            seconds,
            nanoseconds,
        }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            DURATION_TAG,
            vec![
                PackStreamValue::Integer(self.months),
                PackStreamValue::Integer(self.days),
                PackStreamValue::Integer(self.seconds),
                PackStreamValue::Integer(self.nanoseconds),
            ],
        ))
    }

    /// Try to parse from PackStreamValue.
    pub fn from_value(value: &PackStreamValue) -> Result<Self, PackStreamError> {
        let s = value.as_structure().ok_or_else(|| {
            PackStreamError::InvalidStructure("expected Duration structure".into())
        })?;

        if s.tag != DURATION_TAG {
            return Err(PackStreamError::InvalidStructure(
                "expected Duration tag".into(),
            ));
        }

        if s.fields.len() < 4 {
            return Err(PackStreamError::InvalidStructure(
                "Duration requires 4 fields".into(),
            ));
        }

        Ok(Self {
            months: s.fields[0].as_int().unwrap_or(0),
            days: s.fields[1].as_int().unwrap_or(0),
            seconds: s.fields[2].as_int().unwrap_or(0),
            nanoseconds: s.fields[3].as_int().unwrap_or(0),
        })
    }
}

/// 2D Point structure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PackStreamPoint2D {
    /// Spatial Reference System Identifier
    pub srid: i32,
    /// X coordinate
    pub x: f64,
    /// Y coordinate
    pub y: f64,
}

impl PackStreamPoint2D {
    /// Create a new 2D point.
    pub fn new(srid: i32, x: f64, y: f64) -> Self {
        Self { srid, x, y }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            POINT_2D_TAG,
            vec![
                PackStreamValue::Integer(self.srid as i64),
                PackStreamValue::Float(self.x),
                PackStreamValue::Float(self.y),
            ],
        ))
    }
}

/// 3D Point structure.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PackStreamPoint3D {
    /// Spatial Reference System Identifier
    pub srid: i32,
    /// X coordinate
    pub x: f64,
    /// Y coordinate
    pub y: f64,
    /// Z coordinate
    pub z: f64,
}

impl PackStreamPoint3D {
    /// Create a new 3D point.
    pub fn new(srid: i32, x: f64, y: f64, z: f64) -> Self {
        Self { srid, x, y, z }
    }

    /// Convert to PackStreamValue.
    pub fn to_value(&self) -> PackStreamValue {
        PackStreamValue::Structure(PackStreamStructure::new(
            POINT_3D_TAG,
            vec![
                PackStreamValue::Integer(self.srid as i64),
                PackStreamValue::Float(self.x),
                PackStreamValue::Float(self.y),
                PackStreamValue::Float(self.z),
            ],
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_to_value() {
        let mut props = HashMap::new();
        props.insert("name".to_string(), PackStreamValue::String("Alice".into()));

        let node = PackStreamNode::new(1, vec!["Person".to_string()], props);
        let value = node.to_value();

        if let PackStreamValue::Structure(s) = value {
            assert_eq!(s.tag, NODE_TAG);
            assert_eq!(s.fields.len(), 3);
        } else {
            panic!("Expected structure");
        }
    }

    #[test]
    fn test_node_roundtrip() {
        let mut props = HashMap::new();
        props.insert("age".to_string(), PackStreamValue::Integer(30));

        let node = PackStreamNode::new(1, vec!["Person".to_string()], props);
        let value = node.to_value();
        let parsed = PackStreamNode::from_value(&value).unwrap();

        assert_eq!(parsed.id, 1);
        assert_eq!(parsed.labels, vec!["Person"]);
        assert_eq!(parsed.properties.get("age").unwrap().as_int(), Some(30));
    }

    #[test]
    fn test_relationship_to_value() {
        let rel = PackStreamRelationship::new(
            1,
            10,
            20,
            "KNOWS".to_string(),
            HashMap::new(),
        );
        let value = rel.to_value();

        if let PackStreamValue::Structure(s) = value {
            assert_eq!(s.tag, RELATIONSHIP_TAG);
            assert_eq!(s.fields.len(), 5);
        } else {
            panic!("Expected structure");
        }
    }

    #[test]
    fn test_relationship_roundtrip() {
        let mut props = HashMap::new();
        props.insert("since".to_string(), PackStreamValue::Integer(2020));

        let rel = PackStreamRelationship::new(1, 10, 20, "KNOWS".to_string(), props);
        let value = rel.to_value();
        let parsed = PackStreamRelationship::from_value(&value).unwrap();

        assert_eq!(parsed.id, 1);
        assert_eq!(parsed.start_node_id, 10);
        assert_eq!(parsed.end_node_id, 20);
        assert_eq!(parsed.rel_type, "KNOWS");
    }

    #[test]
    fn test_date() {
        let date = PackStreamDate::new(18628); // 2021-01-01
        let value = date.to_value();
        let parsed = PackStreamDate::from_value(&value).unwrap();
        assert_eq!(parsed.days, 18628);
    }

    #[test]
    fn test_duration() {
        let dur = PackStreamDuration::new(12, 30, 3600, 0);
        let value = dur.to_value();
        let parsed = PackStreamDuration::from_value(&value).unwrap();
        assert_eq!(parsed.months, 12);
        assert_eq!(parsed.days, 30);
        assert_eq!(parsed.seconds, 3600);
    }

    #[test]
    fn test_point_2d() {
        let point = PackStreamPoint2D::new(4326, 1.5, 2.5);
        let value = point.to_value();

        if let PackStreamValue::Structure(s) = value {
            assert_eq!(s.tag, POINT_2D_TAG);
            assert_eq!(s.fields.len(), 3);
        } else {
            panic!("Expected structure");
        }
    }

    #[test]
    fn test_point_3d() {
        let point = PackStreamPoint3D::new(4979, 1.0, 2.0, 3.0);
        let value = point.to_value();

        if let PackStreamValue::Structure(s) = value {
            assert_eq!(s.tag, POINT_3D_TAG);
            assert_eq!(s.fields.len(), 4);
        } else {
            panic!("Expected structure");
        }
    }
}
