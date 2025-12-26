//! PackStream encoder.

use bytes::{BufMut, BytesMut};
use std::collections::HashMap;

use super::marker::*;
use super::types::{PackStreamStructure, PackStreamValue};
use super::PackStreamError;

/// PackStream encoder that writes values to a byte buffer.
pub struct PackStreamEncoder {
    buffer: BytesMut,
}

impl PackStreamEncoder {
    /// Create a new encoder with default buffer capacity.
    pub fn new() -> Self {
        Self::with_capacity(256)
    }

    /// Create a new encoder with specified buffer capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Get the current buffer length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Consume the encoder and return the bytes.
    pub fn into_bytes(self) -> BytesMut {
        self.buffer
    }

    /// Get the bytes as a slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Encode a PackStreamValue.
    pub fn encode(&mut self, value: &PackStreamValue) -> Result<(), PackStreamError> {
        match value {
            PackStreamValue::Null => {
                self.encode_null();
                Ok(())
            }
            PackStreamValue::Boolean(b) => {
                self.encode_bool(*b);
                Ok(())
            }
            PackStreamValue::Integer(i) => {
                self.encode_int(*i);
                Ok(())
            }
            PackStreamValue::Float(f) => {
                self.encode_float(*f);
                Ok(())
            }
            PackStreamValue::Bytes(b) => self.encode_bytes(b),
            PackStreamValue::String(s) => self.encode_string(s),
            PackStreamValue::List(l) => self.encode_list(l),
            PackStreamValue::Map(m) => self.encode_map(m),
            PackStreamValue::Structure(s) => self.encode_structure(s),
        }
    }

    /// Encode null.
    pub fn encode_null(&mut self) {
        self.buffer.put_u8(NULL);
    }

    /// Encode a boolean.
    pub fn encode_bool(&mut self, value: bool) {
        self.buffer.put_u8(if value { TRUE } else { FALSE });
    }

    /// Encode an integer using the smallest representation.
    pub fn encode_int(&mut self, value: i64) {
        if can_encode_tiny_int(value) {
            // Tiny int: -16 to 127, encoded in single byte
            self.buffer.put_u8(value as u8);
        } else if value >= i8::MIN as i64 && value <= i8::MAX as i64 {
            // INT_8
            self.buffer.put_u8(INT_8);
            self.buffer.put_i8(value as i8);
        } else if value >= i16::MIN as i64 && value <= i16::MAX as i64 {
            // INT_16
            self.buffer.put_u8(INT_16);
            self.buffer.put_i16(value as i16);
        } else if value >= i32::MIN as i64 && value <= i32::MAX as i64 {
            // INT_32
            self.buffer.put_u8(INT_32);
            self.buffer.put_i32(value as i32);
        } else {
            // INT_64
            self.buffer.put_u8(INT_64);
            self.buffer.put_i64(value);
        }
    }

    /// Encode a float (always 64-bit).
    pub fn encode_float(&mut self, value: f64) {
        self.buffer.put_u8(FLOAT_64);
        self.buffer.put_f64(value);
    }

    /// Encode bytes.
    pub fn encode_bytes(&mut self, value: &[u8]) -> Result<(), PackStreamError> {
        let len = value.len();
        if len > u32::MAX as usize {
            return Err(PackStreamError::ValueTooLarge("bytes", len));
        }

        if len <= u8::MAX as usize {
            self.buffer.put_u8(BYTES_8);
            self.buffer.put_u8(len as u8);
        } else if len <= u16::MAX as usize {
            self.buffer.put_u8(BYTES_16);
            self.buffer.put_u16(len as u16);
        } else {
            self.buffer.put_u8(BYTES_32);
            self.buffer.put_u32(len as u32);
        }

        self.buffer.put_slice(value);
        Ok(())
    }

    /// Encode a string.
    pub fn encode_string(&mut self, value: &str) -> Result<(), PackStreamError> {
        let bytes = value.as_bytes();
        let len = bytes.len();

        if len > u32::MAX as usize {
            return Err(PackStreamError::ValueTooLarge("string", len));
        }

        if len <= TINY_STRING_MAX_LEN {
            // Tiny string
            self.buffer.put_u8(TINY_STRING_BASE + len as u8);
        } else if len <= u8::MAX as usize {
            self.buffer.put_u8(STRING_8);
            self.buffer.put_u8(len as u8);
        } else if len <= u16::MAX as usize {
            self.buffer.put_u8(STRING_16);
            self.buffer.put_u16(len as u16);
        } else {
            self.buffer.put_u8(STRING_32);
            self.buffer.put_u32(len as u32);
        }

        self.buffer.put_slice(bytes);
        Ok(())
    }

    /// Encode a list.
    pub fn encode_list(&mut self, values: &[PackStreamValue]) -> Result<(), PackStreamError> {
        let len = values.len();

        if len > u32::MAX as usize {
            return Err(PackStreamError::ValueTooLarge("list", len));
        }

        if len <= TINY_LIST_MAX_LEN {
            self.buffer.put_u8(TINY_LIST_BASE + len as u8);
        } else if len <= u8::MAX as usize {
            self.buffer.put_u8(LIST_8);
            self.buffer.put_u8(len as u8);
        } else if len <= u16::MAX as usize {
            self.buffer.put_u8(LIST_16);
            self.buffer.put_u16(len as u16);
        } else {
            self.buffer.put_u8(LIST_32);
            self.buffer.put_u32(len as u32);
        }

        for value in values {
            self.encode(value)?;
        }

        Ok(())
    }

    /// Encode a map.
    pub fn encode_map(
        &mut self,
        map: &HashMap<String, PackStreamValue>,
    ) -> Result<(), PackStreamError> {
        let len = map.len();

        if len > u32::MAX as usize {
            return Err(PackStreamError::ValueTooLarge("map", len));
        }

        if len <= TINY_MAP_MAX_LEN {
            self.buffer.put_u8(TINY_MAP_BASE + len as u8);
        } else if len <= u8::MAX as usize {
            self.buffer.put_u8(MAP_8);
            self.buffer.put_u8(len as u8);
        } else if len <= u16::MAX as usize {
            self.buffer.put_u8(MAP_16);
            self.buffer.put_u16(len as u16);
        } else {
            self.buffer.put_u8(MAP_32);
            self.buffer.put_u32(len as u32);
        }

        for (key, value) in map {
            self.encode_string(key)?;
            self.encode(value)?;
        }

        Ok(())
    }

    /// Encode a structure.
    pub fn encode_structure(&mut self, s: &PackStreamStructure) -> Result<(), PackStreamError> {
        let len = s.fields.len();

        if len > u16::MAX as usize {
            return Err(PackStreamError::ValueTooLarge("structure fields", len));
        }

        if len <= TINY_STRUCT_MAX_FIELDS {
            self.buffer.put_u8(TINY_STRUCT_BASE + len as u8);
        } else if len <= u8::MAX as usize {
            self.buffer.put_u8(STRUCT_8);
            self.buffer.put_u8(len as u8);
        } else {
            self.buffer.put_u8(STRUCT_16);
            self.buffer.put_u16(len as u16);
        }

        self.buffer.put_u8(s.tag);

        for field in &s.fields {
            self.encode(field)?;
        }

        Ok(())
    }
}

impl Default for PackStreamEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to encode a single value.
pub fn encode(value: &PackStreamValue) -> Result<BytesMut, PackStreamError> {
    let mut encoder = PackStreamEncoder::new();
    encoder.encode(value)?;
    Ok(encoder.into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_null() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_null();
        assert_eq!(enc.as_bytes(), &[0xC0]);
    }

    #[test]
    fn test_encode_bool() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_bool(true);
        enc.encode_bool(false);
        assert_eq!(enc.as_bytes(), &[0xC3, 0xC2]);
    }

    #[test]
    fn test_encode_tiny_int() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_int(0);
        enc.encode_int(127);
        enc.encode_int(-16);
        enc.encode_int(-1);
        assert_eq!(enc.as_bytes(), &[0x00, 0x7F, 0xF0, 0xFF]);
    }

    #[test]
    fn test_encode_int8() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_int(-17);
        enc.encode_int(-128);
        assert_eq!(enc.as_bytes(), &[0xC8, 0xEF, 0xC8, 0x80]);
    }

    #[test]
    fn test_encode_int16() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_int(1000);
        assert_eq!(enc.as_bytes(), &[0xC9, 0x03, 0xE8]);
    }

    #[test]
    fn test_encode_int32() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_int(100000);
        assert_eq!(enc.as_bytes(), &[0xCA, 0x00, 0x01, 0x86, 0xA0]);
    }

    #[test]
    fn test_encode_int64() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_int(i64::MAX);
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0xCB);
        assert_eq!(bytes.len(), 9);
    }

    #[test]
    fn test_encode_float() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_float(3.14);
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0xC1);
        assert_eq!(bytes.len(), 9);
    }

    #[test]
    fn test_encode_tiny_string() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_string("hello").unwrap();
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0x85); // tiny string length 5
        assert_eq!(&bytes[1..], b"hello");
    }

    #[test]
    fn test_encode_string_8() {
        let s = "a".repeat(20);
        let mut enc = PackStreamEncoder::new();
        enc.encode_string(&s).unwrap();
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0xD0); // STRING_8
        assert_eq!(bytes[1], 20); // length
    }

    #[test]
    fn test_encode_empty_string() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_string("").unwrap();
        assert_eq!(enc.as_bytes(), &[0x80]);
    }

    #[test]
    fn test_encode_bytes() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_bytes(&[1, 2, 3]).unwrap();
        assert_eq!(enc.as_bytes(), &[0xCC, 0x03, 1, 2, 3]);
    }

    #[test]
    fn test_encode_tiny_list() {
        let list = vec![
            PackStreamValue::Integer(1),
            PackStreamValue::Integer(2),
            PackStreamValue::Integer(3),
        ];
        let mut enc = PackStreamEncoder::new();
        enc.encode_list(&list).unwrap();
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0x93); // tiny list length 3
        assert_eq!(&bytes[1..], &[1, 2, 3]); // tiny ints
    }

    #[test]
    fn test_encode_empty_list() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_list(&[]).unwrap();
        assert_eq!(enc.as_bytes(), &[0x90]);
    }

    #[test]
    fn test_encode_tiny_map() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), PackStreamValue::Integer(1));
        let mut enc = PackStreamEncoder::new();
        enc.encode_map(&map).unwrap();
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0xA1); // tiny map length 1
    }

    #[test]
    fn test_encode_empty_map() {
        let mut enc = PackStreamEncoder::new();
        enc.encode_map(&HashMap::new()).unwrap();
        assert_eq!(enc.as_bytes(), &[0xA0]);
    }

    #[test]
    fn test_encode_structure() {
        let s = PackStreamStructure::new(
            0x4E, // NODE tag
            vec![PackStreamValue::Integer(1)],
        );
        let mut enc = PackStreamEncoder::new();
        enc.encode_structure(&s).unwrap();
        let bytes = enc.as_bytes();
        assert_eq!(bytes[0], 0xB1); // tiny struct 1 field
        assert_eq!(bytes[1], 0x4E); // tag
        assert_eq!(bytes[2], 1); // tiny int 1
    }

    #[test]
    fn test_encode_value() {
        let value = PackStreamValue::String("test".to_string());
        let bytes = encode(&value).unwrap();
        assert_eq!(bytes[0], 0x84); // tiny string length 4
        assert_eq!(&bytes[1..], b"test");
    }

    #[test]
    fn test_encode_nested() {
        let value = PackStreamValue::List(vec![
            PackStreamValue::Map({
                let mut m = HashMap::new();
                m.insert("x".to_string(), PackStreamValue::Integer(1));
                m
            }),
        ]);
        let mut enc = PackStreamEncoder::new();
        enc.encode(&value).unwrap();
        // Should not panic
        assert!(enc.len() > 0);
    }
}
