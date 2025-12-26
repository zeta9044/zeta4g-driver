//! Bolt protocol error types.

use std::fmt;
use std::io;

use super::message::FailureMessage;
use super::packstream::PackStreamError;

/// Result type for Bolt operations.
pub type BoltResult<T> = Result<T, BoltError>;

/// Bolt protocol errors.
#[derive(Debug)]
pub enum BoltError {
    /// I/O error
    Io(io::Error),

    /// Handshake error
    Handshake(HandshakeError),

    /// PackStream serialization error
    PackStream(PackStreamError),

    /// Protocol error (invalid message format, etc.)
    Protocol(String),

    /// Authentication error
    Authentication(String),

    /// Connection error
    Connection(String),

    /// Query execution error
    Query(String),

    /// Transaction error
    Transaction(String),

    /// State machine error (invalid state transition)
    InvalidState(String),

    /// Unsupported protocol version
    UnsupportedVersion(u32),

    /// Message too large
    MessageTooLarge { size: usize, max: usize },

    /// Timeout error
    Timeout,

    /// Connection closed
    ConnectionClosed,

    /// Server shutting down
    ServerShutdown,
}

impl fmt::Display for BoltError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BoltError::Io(e) => write!(f, "I/O error: {}", e),
            BoltError::Handshake(e) => write!(f, "Handshake error: {}", e),
            BoltError::PackStream(e) => write!(f, "PackStream error: {}", e),
            BoltError::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            BoltError::Authentication(msg) => write!(f, "Authentication error: {}", msg),
            BoltError::Connection(msg) => write!(f, "Connection error: {}", msg),
            BoltError::Query(msg) => write!(f, "Query error: {}", msg),
            BoltError::Transaction(msg) => write!(f, "Transaction error: {}", msg),
            BoltError::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            BoltError::UnsupportedVersion(v) => write!(f, "Unsupported version: 0x{:08X}", v),
            BoltError::MessageTooLarge { size, max } => {
                write!(f, "Message too large: {} bytes (max: {})", size, max)
            }
            BoltError::Timeout => write!(f, "Operation timed out"),
            BoltError::ConnectionClosed => write!(f, "Connection closed"),
            BoltError::ServerShutdown => write!(f, "Server is shutting down"),
        }
    }
}

impl BoltError {
    /// Convert error to a FAILURE message.
    pub fn to_failure(&self) -> FailureMessage {
        match self {
            BoltError::Authentication(msg) => {
                FailureMessage::auth_error(msg)
            }
            BoltError::Query(msg) => {
                // Determine if syntax or semantic error
                if msg.contains("syntax") || msg.contains("Syntax") {
                    FailureMessage::syntax_error(msg)
                } else {
                    FailureMessage::semantic_error(msg)
                }
            }
            BoltError::Transaction(msg) => {
                FailureMessage::transaction_error(msg)
            }
            BoltError::InvalidState(msg) => {
                FailureMessage::new(
                    "Neo.ClientError.Request.Invalid",
                    msg,
                )
            }
            BoltError::Timeout => {
                FailureMessage::new(
                    BoltErrorCode::TRANSACTION_TIMEOUT,
                    "Operation timed out",
                )
            }
            _ => {
                FailureMessage::general_error(&self.to_string())
            }
        }
    }
}

impl std::error::Error for BoltError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BoltError::Io(e) => Some(e),
            BoltError::Handshake(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for BoltError {
    fn from(err: io::Error) -> Self {
        BoltError::Io(err)
    }
}

impl From<HandshakeError> for BoltError {
    fn from(err: HandshakeError) -> Self {
        BoltError::Handshake(err)
    }
}

impl From<PackStreamError> for BoltError {
    fn from(err: PackStreamError) -> Self {
        BoltError::PackStream(err)
    }
}

/// Handshake-specific errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// Invalid magic number received
    InvalidMagic { expected: [u8; 4], received: [u8; 4] },

    /// No compatible protocol version found
    NoCompatibleVersion,

    /// Invalid handshake data (wrong size, etc.)
    InvalidData(String),

    /// Connection closed during handshake
    ConnectionClosed,

    /// Handshake timeout
    Timeout,
}

impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandshakeError::InvalidMagic { expected, received } => {
                write!(
                    f,
                    "Invalid magic number: expected {:02X?}, received {:02X?}",
                    expected, received
                )
            }
            HandshakeError::NoCompatibleVersion => {
                write!(f, "No compatible protocol version found")
            }
            HandshakeError::InvalidData(msg) => {
                write!(f, "Invalid handshake data: {}", msg)
            }
            HandshakeError::ConnectionClosed => {
                write!(f, "Connection closed during handshake")
            }
            HandshakeError::Timeout => {
                write!(f, "Handshake timed out")
            }
        }
    }
}

impl std::error::Error for HandshakeError {}

/// Bolt 프로토콜 에러 코드 상수
///
/// Bolt 프로토콜 명세에 정의된 에러 코드입니다.
pub struct BoltErrorCode;

impl BoltErrorCode {
    // Client errors (recoverable)
    pub const AUTHENTICATION_FAILED: &'static str = "Neo.ClientError.Security.AuthenticationFailed";
    pub const UNAUTHORIZED: &'static str = "Neo.ClientError.Security.Unauthorized";
    pub const SYNTAX_ERROR: &'static str = "Neo.ClientError.Statement.SyntaxError";
    pub const SEMANTIC_ERROR: &'static str = "Neo.ClientError.Statement.SemanticError";
    pub const PARAMETER_MISSING: &'static str = "Neo.ClientError.Statement.ParameterMissing";
    pub const CONSTRAINT_VIOLATION: &'static str =
        "Neo.ClientError.Schema.ConstraintValidationFailed";
    pub const TRANSACTION_NOT_FOUND: &'static str =
        "Neo.ClientError.Transaction.TransactionNotFound";
    pub const INVALID_BOOKMARK: &'static str = "Neo.ClientError.Transaction.InvalidBookmark";

    // Database errors
    pub const GENERAL_ERROR: &'static str = "Neo.DatabaseError.General.UnknownError";
    pub const EXECUTION_FAILED: &'static str = "Neo.DatabaseError.Statement.ExecutionFailed";

    // Transient errors (retry may succeed)
    pub const TRANSACTION_TIMEOUT: &'static str =
        "Neo.TransientError.Transaction.TransactionTimedOut";
    pub const DEADLOCK_DETECTED: &'static str = "Neo.TransientError.Transaction.DeadlockDetected";
    pub const DATABASE_UNAVAILABLE: &'static str =
        "Neo.TransientError.General.DatabaseUnavailable";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_error_display() {
        let err = HandshakeError::InvalidMagic {
            expected: [0x60, 0x60, 0xB0, 0x17],
            received: [0x00, 0x00, 0x00, 0x00],
        };
        assert!(err.to_string().contains("Invalid magic"));

        let err = HandshakeError::NoCompatibleVersion;
        assert!(err.to_string().contains("No compatible"));
    }

    #[test]
    fn test_bolt_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        let bolt_err: BoltError = io_err.into();
        assert!(matches!(bolt_err, BoltError::Io(_)));
    }

    #[test]
    fn test_bolt_error_from_handshake() {
        let hs_err = HandshakeError::NoCompatibleVersion;
        let bolt_err: BoltError = hs_err.into();
        assert!(matches!(bolt_err, BoltError::Handshake(_)));
    }

    #[test]
    fn test_bolt_error_codes() {
        assert!(BoltErrorCode::SYNTAX_ERROR.starts_with("Neo.ClientError"));
        assert!(BoltErrorCode::DEADLOCK_DETECTED.starts_with("Neo.TransientError"));
        assert!(BoltErrorCode::GENERAL_ERROR.starts_with("Neo.DatabaseError"));
    }
}
