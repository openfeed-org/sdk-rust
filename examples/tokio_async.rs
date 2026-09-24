#[path = "./common/lib.rs"]
mod common;

#[cfg(feature = "tokio-runtime")]
use {
    clap::Parser,
    futures::StreamExt,
    sdk_rust::{
        client::{self, SubscriptionOptions},
        error::OpenfeedResult,
        openfeed::{SubscriptionType, openfeed_gateway_message::Data::*},
    },
};

#[cfg(feature = "tokio-runtime")]
async fn run(config: client::OpenfeedConfig, symbols: Vec<String>) -> OpenfeedResult<()> {
    let mut c = client::OpenfeedClient::new(config);
    c.connect().await?;
    c.subscribe_symbols(
        symbols,
        SubscriptionOptions::default().subscription_types([SubscriptionType::Quote]),
    )
    .await?;

    let mut messages = c.read_messages().await;
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            _ = &mut ctrl_c => {
                drop(messages);
                break;
            }
            Some(message) = messages.next() => {
                // Add handlers for message types here
                match message?.data {
                    Some(InstrumentDefinition(m)) => println!("instrument: {:?}", m),
                    Some(MarketUpdate(m)) => println!("market update: {:?}", m),
                    _ => {}
                }
            }
        }
    }

    c.disconnect().await?;
    Ok(println!("successfully closed connection"))
}

#[cfg(feature = "tokio-runtime")]
#[tokio::main]
async fn main() {
    let args = common::Args::parse();
    let config = client::OpenfeedConfig::default()
        .username(args.username)
        .password(args.password)
        .server(args.server);

    let symbols: Vec<String> = args
        .symbols
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    match run(config, symbols).await {
        Ok(_) => {}
        Err(err) => println!("error! {}", err.to_string()),
    }
}

#[cfg(not(feature = "tokio-runtime"))]
fn main() {
    println!("This example requires the tokio-runtime feature enabled")
}
