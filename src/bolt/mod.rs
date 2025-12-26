//! # Bolt Protocol Implementation
//!
//! Low-level Bolt protocol client implementation for communication with
//! Bolt-compatible graph databases.
//!
//! ## Overview
//!
//! The Bolt protocol is a binary protocol used for efficient communication
//! between clients and graph database servers. This module provides:
//!
//! - **PackStream** - Binary serialization format for all data types
//! - **Message Types** - Request/response message handling
//! - **Handshake** - Protocol version negotiation
//! - **Codec** - Async message framing for Tokio
//!
//! ## Protocol Versions
//!
//! This implementation supports Bolt protocol version 5.x, which includes:
//!
//! - Enhanced routing capabilities
//! - Improved transaction handling
//! - Better error reporting
//!
//! ## Submodules
//!
//! - [`packstream`] - Binary serialization/deserialization
//! - [`message`] - Bolt message types (HELLO, RUN, PULL, etc.)
//! - [`handshake`] - Version negotiation
//! - [`codec`] - Tokio codec for async I/O
//! - [`error`] - Protocol error types
//!
//! ## Note
//!
//! Most users should use the high-level [`crate::driver`] module instead of
//! interacting with the Bolt protocol directly.

pub mod codec;
pub mod error;
pub mod handshake;
pub mod message;
pub mod packstream;

pub use codec::BoltResponseCodec;
pub use error::{BoltError, BoltResult, BoltErrorCode};
pub use handshake::{BoltVersion, BOLT_MAGIC, HANDSHAKE_RESPONSE_SIZE};
pub use message::{
    AccessMode, AuthToken, BeginMessage, BoltRequest, BoltResponse, DiscardMessage,
    FailureMessage, HelloMessage, LogonMessage, Notification, NotificationSeverity,
    PullMessage, QueryPlan, QueryStats, RecordMessage, RouteMessage, RoutingTable,
    RunMessage, SuccessMessage,
};
pub use packstream::{
    PackStreamDecoder, PackStreamEncoder, PackStreamError, PackStreamNode,
    PackStreamRelationship, PackStreamStructure, PackStreamValue,
};
