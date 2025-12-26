//! # Zeta4G Driver
//!
//! A Rust driver for [Zeta4G](https://github.com/zeta9044/zeta4g) graph database
//! with full Bolt protocol support.
//!
//! ## Features
//!
//! - **Bolt Protocol 5.x** - Full implementation of the Bolt protocol for efficient communication
//! - **Async/Await** - Built on Tokio for high-performance async operations
//! - **Connection Pooling** - Automatic connection management with configurable pool sizes
//! - **Transactions** - Full ACID transaction support with explicit control
//! - **Type Safety** - Strongly typed values with automatic conversion
//!
//! ## Quick Start
//!
//! Add to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! zeta4g-driver = "0.1"
//! tokio = { version = "1", features = ["full"] }
//! ```
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! use zeta4g_driver::{Driver, AuthToken, SessionConfig, Value};
//! use std::collections::HashMap;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create driver
//!     let driver = Driver::new(
//!         "bolt://localhost:7687",
//!         AuthToken::basic("zeta4g", "password"),
//!     )?;
//!
//!     // Create session
//!     let session_config = SessionConfig::builder()
//!         .with_database("zeta4g")
//!         .with_write_access()
//!         .build();
//!     let session = driver.session(session_config)?;
//!
//!     // Run query with parameters
//!     let mut params = HashMap::new();
//!     params.insert("name".to_string(), Value::String("Alice".to_string()));
//!
//!     let result = session.run(
//!         "CREATE (n:Person {name: $name}) RETURN n",
//!         Some(params),
//!     ).await?;
//!
//!     // Iterate results
//!     for record in result {
//!         println!("{:?}", record);
//!     }
//!
//!     // Clean up
//!     session.close().await?;
//!     driver.close().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Transactions
//!
//! For operations requiring atomicity, use explicit transactions:
//!
//! ```rust,no_run
//! # use zeta4g_driver::{Driver, AuthToken, SessionConfig};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let driver = Driver::new("bolt://localhost:7687", AuthToken::basic("u", "p"))?;
//! # let session = driver.session(SessionConfig::default())?;
//! // Begin transaction
//! let mut tx = session.begin_transaction(None).await?;
//!
//! // Execute queries
//! tx.run("CREATE (n:Node {id: 1})", None).await?;
//! tx.run("CREATE (n:Node {id: 2})", None).await?;
//!
//! // Commit (or rollback on error)
//! tx.commit().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Transaction Functions
//!
//! For automatic retry on transient errors:
//!
//! ```rust,no_run
//! # use zeta4g_driver::{Driver, AuthToken, SessionConfig, Value};
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let driver = Driver::new("bolt://localhost:7687", AuthToken::basic("u", "p"))?;
//! # let session = driver.session(SessionConfig::default())?;
//! let result = session.write_transaction(|mut tx| async move {
//!     tx.run("CREATE (n:Node) RETURN n", None).await?;
//!     tx.commit().await?;
//!     Ok(42)
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Authentication
//!
//! Several authentication methods are supported:
//!
//! ```rust
//! use zeta4g_driver::AuthToken;
//!
//! // Basic authentication
//! let auth = AuthToken::basic("username", "password");
//!
//! // Bearer token
//! let auth = AuthToken::bearer("my-token");
//!
//! // No authentication
//! let auth = AuthToken::none();
//! ```
//!
//! ## Value Types
//!
//! The driver provides type-safe value handling:
//!
//! ```rust
//! use zeta4g_driver::Value;
//!
//! // Supported types
//! let null = Value::Null;
//! let boolean = Value::Boolean(true);
//! let integer = Value::Integer(42);
//! let float = Value::Float(3.14);
//! let string = Value::String("hello".to_string());
//! let list = Value::List(vec![Value::Integer(1), Value::Integer(2)]);
//! ```
//!
//! ## Configuration
//!
//! Customize driver behavior with [`DriverConfig`]:
//!
//! ```rust
//! use zeta4g_driver::{DriverConfig, AuthToken};
//! use std::time::Duration;
//!
//! let config = DriverConfig::builder("bolt://localhost:7687", AuthToken::basic("u", "p"))
//!     .unwrap()
//!     .with_max_connection_pool_size(50)
//!     .with_connection_timeout(Duration::from_secs(10))
//!     .with_fetch_size(500)
//!     .build();
//! ```
//!
//! ## Error Handling
//!
//! All operations return [`DriverResult`] for consistent error handling:
//!
//! ```rust,no_run
//! # use zeta4g_driver::{Driver, AuthToken, DriverError};
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let driver = Driver::new("bolt://localhost:7687", AuthToken::basic("u", "p"));
//!
//! match driver {
//!     Ok(d) => println!("Connected!"),
//!     Err(DriverError::Connection(msg)) => eprintln!("Connection failed: {}", msg),
//!     Err(e) => eprintln!("Error: {}", e),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Modules
//!
//! - [`driver`] - Core driver, session, and transaction types
//! - [`bolt`] - Low-level Bolt protocol implementation
//!

#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

pub mod bolt;
pub mod driver;

// Re-exports for convenience
pub use driver::{
    Driver, DriverConfig, DriverConfigBuilder, AuthToken, TrustStrategy,
    Session, SessionConfig, SessionConfigBuilder,
    Transaction, TransactionConfig,
    Record, Value,
    DriverError, DriverResult,
    AccessMode, Bookmark,
    Query, QueryResult, ResultSummary,
    ServerAddress, ServerInfo, DriverMetrics,
};

pub use bolt::{
    BoltError, BoltVersion,
    PackStreamValue,
};

/// Config alias for convenience
pub type Config = DriverConfig;
