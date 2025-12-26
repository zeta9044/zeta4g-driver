//! Bolt protocol version definitions.

use std::fmt;

/// Bolt protocol versions.
///
/// Version numbers are encoded as 4-byte big-endian integers:
/// - Major version in high 2 bytes
/// - Minor version in low 2 bytes
///
/// For example: V4_3 = 0x0004_0003 (major=4, minor=3)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BoltVersion {
    /// Bolt 4.0 (Neo4j 4.0)
    V4_0 = 0x0004_0000,
    /// Bolt 4.1 (Neo4j 4.1)
    V4_1 = 0x0004_0001,
    /// Bolt 4.2 (Neo4j 4.2)
    V4_2 = 0x0004_0002,
    /// Bolt 4.3 (Neo4j 4.3) - Added ROUTE message
    V4_3 = 0x0004_0003,
    /// Bolt 4.4 (Neo4j 4.4)
    V4_4 = 0x0004_0004,
    /// Bolt 5.0 (Neo4j 5.0) - Element IDs, LOGON/LOGOFF
    V5_0 = 0x0005_0000,
}

impl BoltVersion {
    /// All supported versions in order of preference (newest first).
    pub const ALL: [BoltVersion; 6] = [
        BoltVersion::V5_0,
        BoltVersion::V4_4,
        BoltVersion::V4_3,
        BoltVersion::V4_2,
        BoltVersion::V4_1,
        BoltVersion::V4_0,
    ];

    /// Create a BoltVersion from a raw u32 value.
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            0x0004_0000 => Some(BoltVersion::V4_0),
            0x0004_0001 => Some(BoltVersion::V4_1),
            0x0004_0002 => Some(BoltVersion::V4_2),
            0x0004_0003 => Some(BoltVersion::V4_3),
            0x0004_0004 => Some(BoltVersion::V4_4),
            0x0005_0000 => Some(BoltVersion::V5_0),
            _ => None,
        }
    }

    /// Get the raw u32 value.
    pub fn as_u32(self) -> u32 {
        self as u32
    }

    /// Get the major version number.
    pub fn major(self) -> u16 {
        ((self as u32) >> 16) as u16
    }

    /// Get the minor version number.
    pub fn minor(self) -> u16 {
        ((self as u32) & 0xFFFF) as u16
    }

    /// Convert to big-endian bytes.
    pub fn to_bytes(self) -> [u8; 4] {
        (self as u32).to_be_bytes()
    }

    /// Parse from big-endian bytes.
    pub fn from_bytes(bytes: [u8; 4]) -> Option<Self> {
        let value = u32::from_be_bytes(bytes);
        Self::from_u32(value)
    }

    /// Check if this version supports the ROUTE message.
    pub fn supports_route(self) -> bool {
        matches!(self, BoltVersion::V4_3 | BoltVersion::V4_4 | BoltVersion::V5_0)
    }

    /// Check if this version uses element IDs (Neo4j 5.x style).
    pub fn uses_element_ids(self) -> bool {
        matches!(self, BoltVersion::V5_0)
    }

    /// Check if this version supports LOGON/LOGOFF messages.
    pub fn supports_logon(self) -> bool {
        matches!(self, BoltVersion::V5_0)
    }
}

impl fmt::Display for BoltVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major(), self.minor())
    }
}

impl PartialOrd for BoltVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BoltVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_u32().cmp(&other.as_u32())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_values() {
        assert_eq!(BoltVersion::V4_0 as u32, 0x0004_0000);
        assert_eq!(BoltVersion::V4_1 as u32, 0x0004_0001);
        assert_eq!(BoltVersion::V4_2 as u32, 0x0004_0002);
        assert_eq!(BoltVersion::V4_3 as u32, 0x0004_0003);
        assert_eq!(BoltVersion::V4_4 as u32, 0x0004_0004);
        assert_eq!(BoltVersion::V5_0 as u32, 0x0005_0000);
    }

    #[test]
    fn test_version_from_u32() {
        assert_eq!(BoltVersion::from_u32(0x0004_0000), Some(BoltVersion::V4_0));
        assert_eq!(BoltVersion::from_u32(0x0005_0000), Some(BoltVersion::V5_0));
        assert_eq!(BoltVersion::from_u32(0x0003_0000), None);
        assert_eq!(BoltVersion::from_u32(0x0000_0000), None);
    }

    #[test]
    fn test_version_major_minor() {
        assert_eq!(BoltVersion::V4_3.major(), 4);
        assert_eq!(BoltVersion::V4_3.minor(), 3);
        assert_eq!(BoltVersion::V5_0.major(), 5);
        assert_eq!(BoltVersion::V5_0.minor(), 0);
    }

    #[test]
    fn test_version_bytes() {
        let v43 = BoltVersion::V4_3;
        let bytes = v43.to_bytes();
        assert_eq!(bytes, [0x00, 0x04, 0x00, 0x03]);
        assert_eq!(BoltVersion::from_bytes(bytes), Some(BoltVersion::V4_3));
    }

    #[test]
    fn test_version_ordering() {
        assert!(BoltVersion::V5_0 > BoltVersion::V4_4);
        assert!(BoltVersion::V4_4 > BoltVersion::V4_3);
        assert!(BoltVersion::V4_3 > BoltVersion::V4_2);
        assert!(BoltVersion::V4_2 > BoltVersion::V4_1);
        assert!(BoltVersion::V4_1 > BoltVersion::V4_0);
    }

    #[test]
    fn test_version_display() {
        assert_eq!(BoltVersion::V4_0.to_string(), "4.0");
        assert_eq!(BoltVersion::V4_3.to_string(), "4.3");
        assert_eq!(BoltVersion::V5_0.to_string(), "5.0");
    }

    #[test]
    fn test_supports_route() {
        assert!(!BoltVersion::V4_0.supports_route());
        assert!(!BoltVersion::V4_1.supports_route());
        assert!(!BoltVersion::V4_2.supports_route());
        assert!(BoltVersion::V4_3.supports_route());
        assert!(BoltVersion::V4_4.supports_route());
        assert!(BoltVersion::V5_0.supports_route());
    }

    #[test]
    fn test_uses_element_ids() {
        assert!(!BoltVersion::V4_4.uses_element_ids());
        assert!(BoltVersion::V5_0.uses_element_ids());
    }

    #[test]
    fn test_supports_logon() {
        assert!(!BoltVersion::V4_4.supports_logon());
        assert!(BoltVersion::V5_0.supports_logon());
    }

    #[test]
    fn test_all_versions() {
        assert_eq!(BoltVersion::ALL.len(), 6);
        // Should be sorted newest to oldest
        assert_eq!(BoltVersion::ALL[0], BoltVersion::V5_0);
        assert_eq!(BoltVersion::ALL[5], BoltVersion::V4_0);
    }
}
