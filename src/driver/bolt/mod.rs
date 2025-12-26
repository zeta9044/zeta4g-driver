//! Bolt protocol client implementation for the driver.
//!
//! This module provides a client-side Bolt protocol implementation
//! that communicates with a Bolt server (Neo4j-compatible).
//!
//! # Architecture
//!
//! The client reuses the server's PackStream and message types, adding
//! only the client-specific connection and handshake logic.
//!
//! ```text
//! Driver
//!   └── BoltClient
//!         ├── BoltConnection (TCP + Framing)
//!         │     └── BoltResponseCodec (from server module)
//!         ├── Handshake (client-side version negotiation)
//!         └── Message Types (from server module)
//! ```

pub mod client;
pub mod connection;

pub use client::{BoltClient, RoutingInfo};
pub use connection::BoltConnection;

// Re-export server bolt types for convenience
pub use crate::bolt::{
    BoltError, BoltRequest, BoltResponse, BoltResult, BoltVersion,
    HelloMessage, RunMessage, PullMessage, DiscardMessage, BeginMessage, RouteMessage,
    SuccessMessage, RecordMessage, FailureMessage,
    PackStreamValue, PackStreamNode, PackStreamRelationship,
    handshake::BOLT_MAGIC,
};
pub use crate::bolt::message::AuthToken as BoltAuthToken;

use super::driver::AuthToken;

/// Client user agent string
pub const CLIENT_USER_AGENT: &str = "Zeta4G-Driver/1.17.0";

/// Supported Bolt versions (highest first)
pub const SUPPORTED_VERSIONS: [(u8, u8); 4] = [
    (5, 0),  // Bolt 5.0
    (4, 4),  // Bolt 4.4
    (4, 3),  // Bolt 4.3
    (4, 0),  // Bolt 4.0
];

/// Convert driver AuthToken to Bolt AuthToken
pub fn to_bolt_auth(auth: &AuthToken) -> BoltAuthToken {
    match auth {
        AuthToken::None => BoltAuthToken::none(),
        AuthToken::Basic { username, password, realm } => {
            let mut token = BoltAuthToken::basic(username, password);
            if let Some(r) = realm {
                token.realm = Some(r.clone());
            }
            token
        }
        AuthToken::Bearer { token } => BoltAuthToken {
            scheme: "bearer".to_string(),
            principal: None,
            credentials: Some(token.clone()),
            realm: None,
            parameters: std::collections::HashMap::new(),
        },
        AuthToken::Kerberos { ticket } => BoltAuthToken {
            scheme: "kerberos".to_string(),
            principal: None,
            credentials: Some(ticket.clone()),
            realm: None,
            parameters: std::collections::HashMap::new(),
        },
        AuthToken::Custom { principal, credentials, realm, scheme, parameters } => {
            let mut params = std::collections::HashMap::new();
            if let Some(p) = parameters {
                for (k, v) in p {
                    params.insert(k.clone(), PackStreamValue::String(v.clone()));
                }
            }
            BoltAuthToken {
                scheme: scheme.clone(),
                principal: Some(principal.clone()),
                credentials: Some(credentials.clone()),
                realm: Some(realm.clone()),
                parameters: params,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_user_agent() {
        assert!(CLIENT_USER_AGENT.starts_with("Zeta4G-Driver"));
    }

    #[test]
    fn test_supported_versions() {
        assert!(!SUPPORTED_VERSIONS.is_empty());
        // Highest version first
        assert!(SUPPORTED_VERSIONS[0] >= SUPPORTED_VERSIONS[SUPPORTED_VERSIONS.len() - 1]);
    }
}
