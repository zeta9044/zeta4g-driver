//! # Zeta4G Driver
//!
//! Rust driver for Zeta4G graph database with Bolt protocol support.
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use zeta4g_driver::{Driver, Config};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to database
//!     let driver = Driver::new("zeta4g://localhost:7687", Config::default()).await?;
//!
//!     // Create session
//!     let session = driver.session(None).await?;
//!
//!     // Run query
//!     let result = session.run("MATCH (n) RETURN n LIMIT 10", None).await?;
//!
//!     Ok(())
//! }
//! ```

pub mod bolt;
pub mod driver;

// Re-exports for convenience
pub use driver::{
    Driver, DriverConfig, AuthToken,
    Session, SessionConfig,
    Transaction,
    Record, Value,
    DriverError, DriverResult,
};

pub use bolt::{
    BoltError, BoltVersion,
    PackStreamValue,
};

/// Config alias for convenience
pub type Config = DriverConfig;
