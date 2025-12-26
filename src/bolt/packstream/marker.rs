//! PackStream type markers.
//!
//! PackStream is a binary serialization format used by the Bolt protocol.
//! Each value is prefixed with a marker byte that indicates its type.

/// Null marker
pub const NULL: u8 = 0xC0;

/// Boolean markers
pub const FALSE: u8 = 0xC2;
pub const TRUE: u8 = 0xC3;

/// Float marker (64-bit IEEE 754)
pub const FLOAT_64: u8 = 0xC1;

/// Integer markers
/// Tiny integers (-16 to 127) are encoded inline
pub const TINY_INT_MIN: u8 = 0xF0; // -16
pub const TINY_INT_MAX: u8 = 0x7F; // 127
pub const INT_8: u8 = 0xC8;
pub const INT_16: u8 = 0xC9;
pub const INT_32: u8 = 0xCA;
pub const INT_64: u8 = 0xCB;

/// Bytes markers
pub const BYTES_8: u8 = 0xCC;
pub const BYTES_16: u8 = 0xCD;
pub const BYTES_32: u8 = 0xCE;

/// String markers
/// Tiny strings (0-15 bytes) use 0x80-0x8F
pub const TINY_STRING_BASE: u8 = 0x80;
pub const TINY_STRING_MAX_LEN: usize = 15;
pub const STRING_8: u8 = 0xD0;
pub const STRING_16: u8 = 0xD1;
pub const STRING_32: u8 = 0xD2;

/// List markers
/// Tiny lists (0-15 elements) use 0x90-0x9F
pub const TINY_LIST_BASE: u8 = 0x90;
pub const TINY_LIST_MAX_LEN: usize = 15;
pub const LIST_8: u8 = 0xD4;
pub const LIST_16: u8 = 0xD5;
pub const LIST_32: u8 = 0xD6;

/// Map markers
/// Tiny maps (0-15 entries) use 0xA0-0xAF
pub const TINY_MAP_BASE: u8 = 0xA0;
pub const TINY_MAP_MAX_LEN: usize = 15;
pub const MAP_8: u8 = 0xD8;
pub const MAP_16: u8 = 0xD9;
pub const MAP_32: u8 = 0xDA;

/// Structure markers
/// Tiny structures (0-15 fields) use 0xB0-0xBF
pub const TINY_STRUCT_BASE: u8 = 0xB0;
pub const TINY_STRUCT_MAX_FIELDS: usize = 15;
pub const STRUCT_8: u8 = 0xDC;
pub const STRUCT_16: u8 = 0xDD;

/// Structure tags for graph types
pub const NODE_TAG: u8 = 0x4E; // 'N'
pub const RELATIONSHIP_TAG: u8 = 0x52; // 'R'
pub const UNBOUND_RELATIONSHIP_TAG: u8 = 0x72; // 'r'
pub const PATH_TAG: u8 = 0x50; // 'P'

/// Structure tags for temporal types
pub const DATE_TAG: u8 = 0x44; // 'D'
pub const TIME_TAG: u8 = 0x54; // 'T'
pub const LOCAL_TIME_TAG: u8 = 0x74; // 't'
pub const DATE_TIME_TAG: u8 = 0x46; // 'F' (with offset)
pub const DATE_TIME_ZONE_TAG: u8 = 0x66; // 'f' (with zone name)
pub const LOCAL_DATE_TIME_TAG: u8 = 0x64; // 'd'
pub const DURATION_TAG: u8 = 0x45; // 'E'

/// Structure tags for spatial types
pub const POINT_2D_TAG: u8 = 0x58; // 'X'
pub const POINT_3D_TAG: u8 = 0x59; // 'Y'

/// Check if a byte is a tiny integer marker (-16 to 127)
#[inline]
pub fn is_tiny_int(marker: u8) -> bool {
    marker <= TINY_INT_MAX || marker >= TINY_INT_MIN
}

/// Decode a tiny integer from its marker byte
#[inline]
pub fn decode_tiny_int(marker: u8) -> i8 {
    marker as i8
}

/// Check if an integer can be encoded as a tiny int
#[inline]
pub fn can_encode_tiny_int(value: i64) -> bool {
    value >= -16 && value <= 127
}

/// Check if a byte is a tiny string marker (0x80-0x8F)
#[inline]
pub fn is_tiny_string(marker: u8) -> bool {
    marker >= TINY_STRING_BASE && marker <= (TINY_STRING_BASE + TINY_STRING_MAX_LEN as u8)
}

/// Get the length from a tiny string marker
#[inline]
pub fn tiny_string_len(marker: u8) -> usize {
    (marker - TINY_STRING_BASE) as usize
}

/// Check if a byte is a tiny list marker (0x90-0x9F)
#[inline]
pub fn is_tiny_list(marker: u8) -> bool {
    marker >= TINY_LIST_BASE && marker <= (TINY_LIST_BASE + TINY_LIST_MAX_LEN as u8)
}

/// Get the length from a tiny list marker
#[inline]
pub fn tiny_list_len(marker: u8) -> usize {
    (marker - TINY_LIST_BASE) as usize
}

/// Check if a byte is a tiny map marker (0xA0-0xAF)
#[inline]
pub fn is_tiny_map(marker: u8) -> bool {
    marker >= TINY_MAP_BASE && marker <= (TINY_MAP_BASE + TINY_MAP_MAX_LEN as u8)
}

/// Get the entry count from a tiny map marker
#[inline]
pub fn tiny_map_len(marker: u8) -> usize {
    (marker - TINY_MAP_BASE) as usize
}

/// Check if a byte is a tiny struct marker (0xB0-0xBF)
#[inline]
pub fn is_tiny_struct(marker: u8) -> bool {
    marker >= TINY_STRUCT_BASE && marker <= (TINY_STRUCT_BASE + TINY_STRUCT_MAX_FIELDS as u8)
}

/// Get the field count from a tiny struct marker
#[inline]
pub fn tiny_struct_fields(marker: u8) -> usize {
    (marker - TINY_STRUCT_BASE) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tiny_int_detection() {
        assert!(is_tiny_int(0x00)); // 0
        assert!(is_tiny_int(0x7F)); // 127
        assert!(is_tiny_int(0xF0)); // -16
        assert!(is_tiny_int(0xFF)); // -1
    }

    #[test]
    fn test_tiny_int_decode() {
        assert_eq!(decode_tiny_int(0x00), 0);
        assert_eq!(decode_tiny_int(0x7F), 127);
        assert_eq!(decode_tiny_int(0xF0), -16);
        assert_eq!(decode_tiny_int(0xFF), -1);
    }

    #[test]
    fn test_can_encode_tiny_int() {
        assert!(can_encode_tiny_int(0));
        assert!(can_encode_tiny_int(127));
        assert!(can_encode_tiny_int(-16));
        assert!(can_encode_tiny_int(-1));
        assert!(!can_encode_tiny_int(128));
        assert!(!can_encode_tiny_int(-17));
    }

    #[test]
    fn test_tiny_string() {
        assert!(is_tiny_string(0x80)); // length 0
        assert!(is_tiny_string(0x8F)); // length 15
        assert!(!is_tiny_string(0x90)); // list marker

        assert_eq!(tiny_string_len(0x80), 0);
        assert_eq!(tiny_string_len(0x85), 5);
        assert_eq!(tiny_string_len(0x8F), 15);
    }

    #[test]
    fn test_tiny_list() {
        assert!(is_tiny_list(0x90)); // length 0
        assert!(is_tiny_list(0x9F)); // length 15
        assert!(!is_tiny_list(0xA0)); // map marker

        assert_eq!(tiny_list_len(0x90), 0);
        assert_eq!(tiny_list_len(0x95), 5);
        assert_eq!(tiny_list_len(0x9F), 15);
    }

    #[test]
    fn test_tiny_map() {
        assert!(is_tiny_map(0xA0)); // length 0
        assert!(is_tiny_map(0xAF)); // length 15
        assert!(!is_tiny_map(0xB0)); // struct marker

        assert_eq!(tiny_map_len(0xA0), 0);
        assert_eq!(tiny_map_len(0xA5), 5);
        assert_eq!(tiny_map_len(0xAF), 15);
    }

    #[test]
    fn test_tiny_struct() {
        assert!(is_tiny_struct(0xB0)); // 0 fields
        assert!(is_tiny_struct(0xBF)); // 15 fields
        assert!(!is_tiny_struct(0xC0)); // null marker

        assert_eq!(tiny_struct_fields(0xB0), 0);
        assert_eq!(tiny_struct_fields(0xB3), 3);
        assert_eq!(tiny_struct_fields(0xBF), 15);
    }

    #[test]
    fn test_marker_constants() {
        // Verify non-overlapping ranges
        assert_ne!(NULL, FALSE);
        assert_ne!(NULL, TRUE);
        assert!(TINY_STRING_BASE < TINY_LIST_BASE);
        assert!(TINY_LIST_BASE < TINY_MAP_BASE);
        assert!(TINY_MAP_BASE < TINY_STRUCT_BASE);
    }
}
