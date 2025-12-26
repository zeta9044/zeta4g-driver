//! PackStream decoder.

use bytes::Buf;
use std::collections::HashMap;

use super::marker::*;
use super::types::{PackStreamStructure, PackStreamValue};
use super::PackStreamError;

/// PackStream decoder that reads values from a byte buffer.
pub struct PackStreamDecoder<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> PackStreamDecoder<'a> {
    /// Create a new decoder for the given bytes.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Get the current position.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Get remaining bytes count.
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Check if all data has been consumed.
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// Decode the next value.
    pub fn decode(&mut self) -> Result<PackStreamValue, PackStreamError> {
        let marker = self.read_u8()?;

        // Check for tiny types first (most common)
        if is_tiny_int(marker) && !self.is_special_marker(marker) {
            return Ok(PackStreamValue::Integer(decode_tiny_int(marker) as i64));
        }

        if is_tiny_string(marker) {
            let len = tiny_string_len(marker);
            return self.read_string_data(len);
        }

        if is_tiny_list(marker) {
            let len = tiny_list_len(marker);
            return self.read_list_data(len);
        }

        if is_tiny_map(marker) {
            let len = tiny_map_len(marker);
            return self.read_map_data(len);
        }

        if is_tiny_struct(marker) {
            let len = tiny_struct_fields(marker);
            return self.read_struct_data(len);
        }

        // Handle other markers
        match marker {
            NULL => Ok(PackStreamValue::Null),
            TRUE => Ok(PackStreamValue::Boolean(true)),
            FALSE => Ok(PackStreamValue::Boolean(false)),

            FLOAT_64 => Ok(PackStreamValue::Float(self.read_f64()?)),

            INT_8 => Ok(PackStreamValue::Integer(self.read_i8()? as i64)),
            INT_16 => Ok(PackStreamValue::Integer(self.read_i16()? as i64)),
            INT_32 => Ok(PackStreamValue::Integer(self.read_i32()? as i64)),
            INT_64 => Ok(PackStreamValue::Integer(self.read_i64()?)),

            BYTES_8 => {
                let len = self.read_u8()? as usize;
                self.read_bytes_data(len)
            }
            BYTES_16 => {
                let len = self.read_u16()? as usize;
                self.read_bytes_data(len)
            }
            BYTES_32 => {
                let len = self.read_u32()? as usize;
                self.read_bytes_data(len)
            }

            STRING_8 => {
                let len = self.read_u8()? as usize;
                self.read_string_data(len)
            }
            STRING_16 => {
                let len = self.read_u16()? as usize;
                self.read_string_data(len)
            }
            STRING_32 => {
                let len = self.read_u32()? as usize;
                self.read_string_data(len)
            }

            LIST_8 => {
                let len = self.read_u8()? as usize;
                self.read_list_data(len)
            }
            LIST_16 => {
                let len = self.read_u16()? as usize;
                self.read_list_data(len)
            }
            LIST_32 => {
                let len = self.read_u32()? as usize;
                self.read_list_data(len)
            }

            MAP_8 => {
                let len = self.read_u8()? as usize;
                self.read_map_data(len)
            }
            MAP_16 => {
                let len = self.read_u16()? as usize;
                self.read_map_data(len)
            }
            MAP_32 => {
                let len = self.read_u32()? as usize;
                self.read_map_data(len)
            }

            STRUCT_8 => {
                let len = self.read_u8()? as usize;
                self.read_struct_data(len)
            }
            STRUCT_16 => {
                let len = self.read_u16()? as usize;
                self.read_struct_data(len)
            }

            _ => Err(PackStreamError::UnknownMarker(marker)),
        }
    }

    /// Check if a marker is a special non-tiny-int marker in the tiny int range.
    fn is_special_marker(&self, marker: u8) -> bool {
        matches!(
            marker,
            NULL | TRUE
                | FALSE
                | FLOAT_64
                | INT_8
                | INT_16
                | INT_32
                | INT_64
                | BYTES_8
                | BYTES_16
                | BYTES_32
                | STRING_8
                | STRING_16
                | STRING_32
                | LIST_8
                | LIST_16
                | LIST_32
                | MAP_8
                | MAP_16
                | MAP_32
                | STRUCT_8
                | STRUCT_16
        ) || is_tiny_string(marker)
            || is_tiny_list(marker)
            || is_tiny_map(marker)
            || is_tiny_struct(marker)
    }

    fn read_bytes_data(&mut self, len: usize) -> Result<PackStreamValue, PackStreamError> {
        let bytes = self.read_bytes(len)?;
        Ok(PackStreamValue::Bytes(bytes.to_vec()))
    }

    fn read_string_data(&mut self, len: usize) -> Result<PackStreamValue, PackStreamError> {
        let bytes = self.read_bytes(len)?;
        let s = std::str::from_utf8(bytes)
            .map_err(|e| PackStreamError::InvalidUtf8(e.to_string()))?;
        Ok(PackStreamValue::String(s.to_string()))
    }

    fn read_list_data(&mut self, len: usize) -> Result<PackStreamValue, PackStreamError> {
        let mut items = Vec::with_capacity(len.min(1024));
        for _ in 0..len {
            items.push(self.decode()?);
        }
        Ok(PackStreamValue::List(items))
    }

    fn read_map_data(&mut self, len: usize) -> Result<PackStreamValue, PackStreamError> {
        let mut map = HashMap::with_capacity(len.min(1024));
        for _ in 0..len {
            let key = self.decode()?;
            let key_str = match key {
                PackStreamValue::String(s) => s,
                _ => return Err(PackStreamError::InvalidMapKey),
            };
            let value = self.decode()?;
            map.insert(key_str, value);
        }
        Ok(PackStreamValue::Map(map))
    }

    fn read_struct_data(&mut self, field_count: usize) -> Result<PackStreamValue, PackStreamError> {
        let tag = self.read_u8()?;
        let mut fields = Vec::with_capacity(field_count.min(64));
        for _ in 0..field_count {
            fields.push(self.decode()?);
        }
        Ok(PackStreamValue::Structure(PackStreamStructure::new(
            tag, fields,
        )))
    }

    // Low-level read methods

    fn read_u8(&mut self) -> Result<u8, PackStreamError> {
        if self.remaining() < 1 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = self.data[self.pos];
        self.pos += 1;
        Ok(value)
    }

    fn read_i8(&mut self) -> Result<i8, PackStreamError> {
        Ok(self.read_u8()? as i8)
    }

    fn read_u16(&mut self) -> Result<u16, PackStreamError> {
        if self.remaining() < 2 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_u16();
        self.pos += 2;
        Ok(value)
    }

    fn read_i16(&mut self) -> Result<i16, PackStreamError> {
        if self.remaining() < 2 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_i16();
        self.pos += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, PackStreamError> {
        if self.remaining() < 4 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_u32();
        self.pos += 4;
        Ok(value)
    }

    fn read_i32(&mut self) -> Result<i32, PackStreamError> {
        if self.remaining() < 4 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_i32();
        self.pos += 4;
        Ok(value)
    }

    fn read_i64(&mut self) -> Result<i64, PackStreamError> {
        if self.remaining() < 8 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_i64();
        self.pos += 8;
        Ok(value)
    }

    fn read_f64(&mut self) -> Result<f64, PackStreamError> {
        if self.remaining() < 8 {
            return Err(PackStreamError::UnexpectedEof);
        }
        let value = (&self.data[self.pos..]).get_f64();
        self.pos += 8;
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], PackStreamError> {
        if self.remaining() < len {
            return Err(PackStreamError::UnexpectedEof);
        }
        let bytes = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(bytes)
    }
}

/// Convenience function to decode a single value from bytes.
pub fn decode(data: &[u8]) -> Result<PackStreamValue, PackStreamError> {
    let mut decoder = PackStreamDecoder::new(data);
    decoder.decode()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_null() {
        let data = [0xC0];
        let value = decode(&data).unwrap();
        assert!(value.is_null());
    }

    #[test]
    fn test_decode_bool() {
        assert_eq!(decode(&[0xC3]).unwrap(), PackStreamValue::Boolean(true));
        assert_eq!(decode(&[0xC2]).unwrap(), PackStreamValue::Boolean(false));
    }

    #[test]
    fn test_decode_tiny_int() {
        assert_eq!(decode(&[0x00]).unwrap(), PackStreamValue::Integer(0));
        assert_eq!(decode(&[0x7F]).unwrap(), PackStreamValue::Integer(127));
        assert_eq!(decode(&[0xF0]).unwrap(), PackStreamValue::Integer(-16));
        assert_eq!(decode(&[0xFF]).unwrap(), PackStreamValue::Integer(-1));
    }

    #[test]
    fn test_decode_int8() {
        // -17 encoded as INT_8
        assert_eq!(
            decode(&[0xC8, 0xEF]).unwrap(),
            PackStreamValue::Integer(-17)
        );
        // -128
        assert_eq!(
            decode(&[0xC8, 0x80]).unwrap(),
            PackStreamValue::Integer(-128)
        );
    }

    #[test]
    fn test_decode_int16() {
        // 1000 encoded as INT_16
        assert_eq!(
            decode(&[0xC9, 0x03, 0xE8]).unwrap(),
            PackStreamValue::Integer(1000)
        );
    }

    #[test]
    fn test_decode_int32() {
        // 100000 encoded as INT_32
        assert_eq!(
            decode(&[0xCA, 0x00, 0x01, 0x86, 0xA0]).unwrap(),
            PackStreamValue::Integer(100000)
        );
    }

    #[test]
    fn test_decode_int64() {
        // Large value encoded as INT_64
        let data = [0xCB, 0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert_eq!(decode(&data).unwrap(), PackStreamValue::Integer(i64::MAX));
    }

    #[test]
    fn test_decode_float() {
        let data = [0xC1, 0x40, 0x09, 0x1E, 0xB8, 0x51, 0xEB, 0x85, 0x1F];
        let value = decode(&data).unwrap();
        if let PackStreamValue::Float(f) = value {
            assert!((f - 3.14).abs() < 0.001);
        } else {
            panic!("Expected float");
        }
    }

    #[test]
    fn test_decode_tiny_string() {
        let data = [0x85, b'h', b'e', b'l', b'l', b'o'];
        assert_eq!(
            decode(&data).unwrap(),
            PackStreamValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_decode_empty_string() {
        assert_eq!(
            decode(&[0x80]).unwrap(),
            PackStreamValue::String("".to_string())
        );
    }

    #[test]
    fn test_decode_string_8() {
        let mut data = vec![0xD0, 20];
        data.extend_from_slice(&[b'a'; 20]);
        assert_eq!(
            decode(&data).unwrap(),
            PackStreamValue::String("a".repeat(20))
        );
    }

    #[test]
    fn test_decode_bytes() {
        let data = [0xCC, 0x03, 1, 2, 3];
        assert_eq!(
            decode(&data).unwrap(),
            PackStreamValue::Bytes(vec![1, 2, 3])
        );
    }

    #[test]
    fn test_decode_tiny_list() {
        let data = [0x93, 1, 2, 3]; // list of 3 tiny ints
        let value = decode(&data).unwrap();
        if let PackStreamValue::List(l) = value {
            assert_eq!(l.len(), 3);
            assert_eq!(l[0], PackStreamValue::Integer(1));
            assert_eq!(l[1], PackStreamValue::Integer(2));
            assert_eq!(l[2], PackStreamValue::Integer(3));
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_decode_empty_list() {
        assert_eq!(decode(&[0x90]).unwrap(), PackStreamValue::List(vec![]));
    }

    #[test]
    fn test_decode_tiny_map() {
        // Map with 1 entry: "a" -> 1
        let data = [0xA1, 0x81, b'a', 1];
        let value = decode(&data).unwrap();
        if let PackStreamValue::Map(m) = value {
            assert_eq!(m.len(), 1);
            assert_eq!(m.get("a").unwrap(), &PackStreamValue::Integer(1));
        } else {
            panic!("Expected map");
        }
    }

    #[test]
    fn test_decode_empty_map() {
        let value = decode(&[0xA0]).unwrap();
        if let PackStreamValue::Map(m) = value {
            assert!(m.is_empty());
        } else {
            panic!("Expected map");
        }
    }

    #[test]
    fn test_decode_structure() {
        // Structure with tag 0x4E (NODE), 1 field
        let data = [0xB1, 0x4E, 1];
        let value = decode(&data).unwrap();
        if let PackStreamValue::Structure(s) = value {
            assert_eq!(s.tag, 0x4E);
            assert_eq!(s.fields.len(), 1);
            assert_eq!(s.fields[0], PackStreamValue::Integer(1));
        } else {
            panic!("Expected structure");
        }
    }

    #[test]
    fn test_decode_unexpected_eof() {
        let err = decode(&[0xC9]).unwrap_err(); // INT_16 but no data
        assert!(matches!(err, PackStreamError::UnexpectedEof));
    }

    #[test]
    fn test_decode_invalid_utf8() {
        let data = [0x82, 0xFF, 0xFE]; // string with invalid UTF-8
        let err = decode(&data).unwrap_err();
        assert!(matches!(err, PackStreamError::InvalidUtf8(_)));
    }

    #[test]
    fn test_decode_nested() {
        // List containing a map
        let data = [
            0x91, // list of 1
            0xA1, // map of 1
            0x81, b'x', // key "x"
            0x05, // value 5
        ];
        let value = decode(&data).unwrap();
        if let PackStreamValue::List(l) = value {
            if let PackStreamValue::Map(m) = &l[0] {
                assert_eq!(m.get("x").unwrap(), &PackStreamValue::Integer(5));
            } else {
                panic!("Expected map in list");
            }
        } else {
            panic!("Expected list");
        }
    }

    #[test]
    fn test_decoder_position() {
        let data = [0x01, 0x02, 0x03];
        let mut decoder = PackStreamDecoder::new(&data);
        assert_eq!(decoder.position(), 0);
        assert_eq!(decoder.remaining(), 3);

        decoder.decode().unwrap();
        assert_eq!(decoder.position(), 1);
        assert_eq!(decoder.remaining(), 2);
    }
}
