//! # Driver Module
//!
//! Core driver implementation for Zeta4G graph database.
//!
//! This module provides the primary API for connecting to and interacting with
//! Zeta4G databases using the Bolt protocol.
//!
//! ## Core Types
//!
//! - [`Driver`] - Main entry point for database connections
//! - [`Session`] - Logical container for database operations
//! - [`Transaction`] - Explicit transaction control
//! - [`Record`] - Single row in a query result
//! - [`Value`] - Type-safe representation of database values
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use zeta4g_driver::{Driver, AuthToken, SessionConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create driver
//! let driver = Driver::new("bolt://localhost:7687", AuthToken::basic("zeta4g", "password"))?;
//!
//! // Create session
//! let session = driver.session(SessionConfig::default())?;
//!
//! // Run query
//! let result = session.run("MATCH (n) RETURN n LIMIT 10", None).await?;
//! for record in result {
//!     println!("{:?}", record);
//! }
//!
//! // Clean up
//! session.close().await?;
//! driver.close().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transaction Example
//!
//! ```rust,no_run
//! use zeta4g_driver::{Driver, AuthToken, SessionConfig};
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let driver = Driver::new("bolt://localhost:7687", AuthToken::basic("u", "p"))?;
//! # let session = driver.session(SessionConfig::default())?;
//! // Begin transaction
//! let mut tx = session.begin_transaction(None).await?;
//!
//! // Execute queries within transaction
//! tx.run("CREATE (n:Person {name: 'Alice'})", None).await?;
//! tx.run("CREATE (n:Person {name: 'Bob'})", None).await?;
//!
//! // Commit transaction
//! tx.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Submodules
//!
//! - [`routing`] - Cluster routing support for high availability
//! - [`reactive`] - Reactive stream support using tokio-stream
//! - [`bolt`] - Low-level Bolt client implementation

pub mod bolt;
pub mod reactive;
pub mod routing;
mod driver;
mod error;
mod pool;
mod record;
mod session;
mod transaction;
mod types;

// Re-exports
pub use driver::{
    AuthToken, Driver, DriverConfig, DriverConfigBuilder, DriverMetrics,
    ServerAddress, ServerInfo, TrustStrategy,
};
pub use error::{DriverError, DriverResult, BoltError};
pub use pool::{ConnectionPool, ConnectionState, PoolConfig, PoolConfigBuilder, PoolMetrics, PooledConnection};
pub use record::{Record, RecordStream};
pub use session::{
    AccessMode, Bookmark, Counters, InputPosition, Notification, Query,
    QueryResult, QueryType, ResultSummary, Session, SessionConfig, SessionConfigBuilder,
};
pub use transaction::{Transaction, TransactionConfig, TransactionState};
pub use types::{Duration, Node, Path, Point, Relationship, Value};

/// 파라미터 맵 생성 매크로
#[macro_export]
macro_rules! params {
    () => {
        std::collections::HashMap::new()
    };
    ($($key:expr => $value:expr),+ $(,)?) => {{
        let mut map = std::collections::HashMap::new();
        $(
            map.insert($key.into(), $crate::driver::Value::from($value));
        )+
        map
    }};
}
