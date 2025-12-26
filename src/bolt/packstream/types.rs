//! PackStream value types.

use std::collections::HashMap;

/// A PackStream value that can be serialized/deserialized.
#[derive(Debug, Clone, PartialEq)]
pub enum PackStreamValue {
    /// Null value
    Null,
    /// Boolean value
    Boolean(bool),
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point
    Float(f64),
    /// Byte array
    Bytes(Vec<u8>),
    /// UTF-8 string
    String(String),
    /// List of values
    List(Vec<PackStreamValue>),
    /// Map of string keys to values
    Map(HashMap<String, PackStreamValue>),
    /// Structure (tag + fields)
    Structure(PackStreamStructure),
}

/// A PackStream structure with a tag and fields.
#[derive(Debug, Clone, PartialEq)]
pub struct PackStreamStructure {
    /// Structure tag (identifies the type)
    pub tag: u8,
    /// Structure fields
    pub fields: Vec<PackStreamValue>,
}

impl PackStreamStructure {
    /// Create a new structure with given tag and fields.
    pub fn new(tag: u8, fields: Vec<PackStreamValue>) -> Self {
        Self { tag, fields }
    }

    /// Get the number of fields.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the structure has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl PackStreamValue {
    /// Check if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, PackStreamValue::Null)
    }

    /// Try to get as boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PackStreamValue::Boolean(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            PackStreamValue::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as float.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            PackStreamValue::Float(f) => Some(*f),
            PackStreamValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PackStreamValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes reference.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            PackStreamValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get as list reference.
    pub fn as_list(&self) -> Option<&[PackStreamValue]> {
        match self {
            PackStreamValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Try to get as map reference.
    pub fn as_map(&self) -> Option<&HashMap<String, PackStreamValue>> {
        match self {
            PackStreamValue::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Try to get as structure reference.
    pub fn as_structure(&self) -> Option<&PackStreamStructure> {
        match self {
            PackStreamValue::Structure(s) => Some(s),
            _ => None,
        }
    }

    /// Get the type name for debugging.
    pub fn type_name(&self) -> &'static str {
        match self {
            PackStreamValue::Null => "Null",
            PackStreamValue::Boolean(_) => "Boolean",
            PackStreamValue::Integer(_) => "Integer",
            PackStreamValue::Float(_) => "Float",
            PackStreamValue::Bytes(_) => "Bytes",
            PackStreamValue::String(_) => "String",
            PackStreamValue::List(_) => "List",
            PackStreamValue::Map(_) => "Map",
            PackStreamValue::Structure(_) => "Structure",
        }
    }
}

// Conversion traits
impl From<bool> for PackStreamValue {
    fn from(v: bool) -> Self {
        PackStreamValue::Boolean(v)
    }
}

impl From<i64> for PackStreamValue {
    fn from(v: i64) -> Self {
        PackStreamValue::Integer(v)
    }
}

impl From<i32> for PackStreamValue {
    fn from(v: i32) -> Self {
        PackStreamValue::Integer(v as i64)
    }
}

impl From<f64> for PackStreamValue {
    fn from(v: f64) -> Self {
        PackStreamValue::Float(v)
    }
}

impl From<String> for PackStreamValue {
    fn from(v: String) -> Self {
        PackStreamValue::String(v)
    }
}

impl From<&str> for PackStreamValue {
    fn from(v: &str) -> Self {
        PackStreamValue::String(v.to_string())
    }
}

impl From<Vec<u8>> for PackStreamValue {
    fn from(v: Vec<u8>) -> Self {
        PackStreamValue::Bytes(v)
    }
}

impl From<Vec<PackStreamValue>> for PackStreamValue {
    fn from(v: Vec<PackStreamValue>) -> Self {
        PackStreamValue::List(v)
    }
}

impl From<HashMap<String, PackStreamValue>> for PackStreamValue {
    fn from(v: HashMap<String, PackStreamValue>) -> Self {
        PackStreamValue::Map(v)
    }
}

impl From<PackStreamStructure> for PackStreamValue {
    fn from(v: PackStreamStructure) -> Self {
        PackStreamValue::Structure(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_null() {
        let v = PackStreamValue::Null;
        assert!(v.is_null());
        assert_eq!(v.type_name(), "Null");
    }

    #[test]
    fn test_boolean() {
        let v = PackStreamValue::Boolean(true);
        assert_eq!(v.as_bool(), Some(true));
        assert!(!v.is_null());
    }

    #[test]
    fn test_integer() {
        let v = PackStreamValue::Integer(42);
        assert_eq!(v.as_int(), Some(42));
        assert_eq!(v.as_float(), Some(42.0));
    }

    #[test]
    fn test_float() {
        let v = PackStreamValue::Float(3.14);
        assert_eq!(v.as_float(), Some(3.14));
        assert_eq!(v.as_int(), None);
    }

    #[test]
    fn test_string() {
        let v = PackStreamValue::String("hello".to_string());
        assert_eq!(v.as_str(), Some("hello"));
    }

    #[test]
    fn test_bytes() {
        let v = PackStreamValue::Bytes(vec![1, 2, 3]);
        assert_eq!(v.as_bytes(), Some(&[1, 2, 3][..]));
    }

    #[test]
    fn test_list() {
        let v = PackStreamValue::List(vec![
            PackStreamValue::Integer(1),
            PackStreamValue::Integer(2),
        ]);
        let list = v.as_list().unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_map() {
        let mut map = HashMap::new();
        map.insert("key".to_string(), PackStreamValue::Integer(42));
        let v = PackStreamValue::Map(map);
        let m = v.as_map().unwrap();
        assert_eq!(m.get("key").unwrap().as_int(), Some(42));
    }

    #[test]
    fn test_structure() {
        let s = PackStreamStructure::new(0x4E, vec![PackStreamValue::Integer(1)]);
        assert_eq!(s.tag, 0x4E);
        assert_eq!(s.len(), 1);
        assert!(!s.is_empty());

        let v = PackStreamValue::Structure(s);
        assert!(v.as_structure().is_some());
    }

    #[test]
    fn test_from_conversions() {
        let _: PackStreamValue = true.into();
        let _: PackStreamValue = 42i64.into();
        let _: PackStreamValue = 42i32.into();
        let _: PackStreamValue = 3.14f64.into();
        let _: PackStreamValue = "hello".into();
        let _: PackStreamValue = String::from("hello").into();
        let _: PackStreamValue = vec![1u8, 2, 3].into();
    }
}
