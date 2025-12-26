//! Bolt Protocol client implementation.
//!
//! This module implements the Bolt protocol for client-side communication
//! with Bolt-compatible graph databases.

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
