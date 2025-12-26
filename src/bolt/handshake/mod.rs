//! Bolt protocol handshake implementation.
//!
//! The Bolt handshake consists of:
//! 1. Client sends 4-byte magic number (0x6060B017)
//! 2. Client sends 4 x 4-byte version proposals (highest first)
//! 3. Server responds with 4-byte agreed version (or 0 if none)

mod negotiation;
mod version;

pub use negotiation::{Handshake, HandshakeResult};
pub use version::BoltVersion;

// Re-export error from parent module
pub use super::error::HandshakeError;

/// Bolt protocol magic number: 0x6060B017
/// This identifies a Bolt connection.
pub const BOLT_MAGIC: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Size of the complete handshake message from client (magic + 4 versions)
pub const HANDSHAKE_SIZE: usize = 20;

/// Size of server response (negotiated version)
pub const HANDSHAKE_RESPONSE_SIZE: usize = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_constant() {
        // Verify magic number matches Bolt spec
        assert_eq!(BOLT_MAGIC, [0x60, 0x60, 0xB0, 0x17]);
    }

    #[test]
    fn test_handshake_sizes() {
        assert_eq!(HANDSHAKE_SIZE, 20);
        assert_eq!(HANDSHAKE_RESPONSE_SIZE, 4);
    }
}
