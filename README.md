# zeta4g-driver

Rust driver for Zeta4G graph database with Bolt protocol support.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
zeta4g-driver = "0.1"
```

## Quick Start

```rust
use zeta4g_driver::{Driver, Config, AuthToken};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to database
    let driver = Driver::new(
        "bolt://localhost:7687",
        AuthToken::basic("zeta4g", "password"),
    ).await?;

    // Create session
    let session = driver.session(None).await?;

    // Run query
    let mut result = session.run("MATCH (n) RETURN n LIMIT 10", None).await?;

    while let Some(record) = result.next().await {
        println!("{:?}", record);
    }

    // Close
    session.close().await?;
    driver.close().await?;

    Ok(())
}
```

## Features

- Bolt protocol 4.x/5.x support
- Connection pooling
- Transaction support
- Routing driver for cluster
- Reactive streams

## URI Schemes

| Scheme | Description |
|--------|-------------|
| `bolt://` | Direct connection to single server |
| `bolt+s://` | Direct connection with TLS |
| `zeta4g://` | Routing connection to cluster |
| `zeta4g+s://` | Routing connection with TLS |

## License

MIT License
