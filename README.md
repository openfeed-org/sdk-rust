# SDK-Rust
Facilitates client connections and message parsing using the Openfeed Protocol for streaming Barchart market data.

## Installation
The SDK offers a basic blocking streaming client or one compatible with one of the popular async runtimes:
- [Tokio](https://tokio.rs)
- [Smol](https://github.com/smol-rs/smol)

**Blocking**:
```
[dependencies]
sdk-rust = { git = "https://github.com/openfeed-org/sdk-rust", features=["blocking"]}
```

**Non-blocking**:
```
[dependencies]
sdk-rust = { git = "https://github.com/openfeed-org/sdk-rust", features=["tokio-runtime"]}
```

At least one runtime feature must be provided in order to use the client.

## Usage
Upon connection, the client may subscribe to either individual symbols or entire exchanges. More information about the available methods [here]("https://docs.barchart.com/openfeed/#/openfeed_streaming"). Streaming data can be accessed from the client via a `Feed` type which is either an `Iterator` or `Stream` depending on the runtime feature selected. Feed messages can be read off the stream with `read_messages()` or as raw bytes with `read_bytes()`.

```rust
use {
    sdk_rust::{
        client, error,
        openfeed::{Service, SubscriptionType, openfeed_gateway_message::Data::*},
    },
};

fn main() -> Result<(), error::OpenfeedError> {
    let symbols = vec!["GCQ26","GCU26"];

    let mut c = client::OpenfeedClient::new(
        client::OpenfeedConfig::default()
            .username("<user>")
            .password("<pass>"),
    );
    c.connect()?;
    c.subscribe_symbols(symbols, &[SubscriptionType::Quote], Service::RealTime)?;
    Ok(for message in c.read_messages() {
        match message?.data {
            Some(InstrumentDefinition(def)) => println!("definition: {:?}", def),
            Some(MarketUpdate(update))=> println!("update: {:?}", update),
            Some(HeartBeat(_)) => println!("hearbeat"),
            _ => {/* ignore other message types */},
        }
    })
}
```

Provided are some examples of the different client types to try yourself:

**Blocking** ([`sync.rs`](examples/sync.rs))
```
cargo run --example sync --features blocking -- -u <user> -p <pass> GCQ26,GCU26
```
**Tokio** ([`tokio_async.rs`](examples/tokio_async.rs))
```
cargo run --example tokio_async --features tokio-runtime -- -u <user> -p <pass> GCQ26,GCU26
```
