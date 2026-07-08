#[path = "./common/lib.rs"]
mod common;

#[cfg(feature = "blocking")]
use {
    clap::Parser,
    sdk_rust::{
        client, error,
        openfeed::{Service, SubscriptionType, openfeed_gateway_message::Data::*},
    },
};

#[cfg(feature = "blocking")]
fn run(config: client::OpenfeedConfig, symbols: Vec<String>) -> error::OpenfeedResult<()> {
    let mut c = client::OpenfeedClient::new(config);
    c.connect()?;
    c.subscribe_symbols(symbols, &[SubscriptionType::Quote], Service::RealTime)?;
    for message in c.read_messages() {
        // Add handlers for message types here
        match message?.data {
            Some(InstrumentDefinition(m)) => println!("instrument: {:?}", m),
            Some(MarketUpdate(m)) => println!("market update: {:?}", m),
            _ => {}
        }
    }
    Ok((/* Sync - Disconnect with ctrl-c */))
}

#[cfg(feature = "blocking")]
fn main() {
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

    match run(config, symbols) {
        Ok(_) => {}
        Err(err) => println!("error! {}", err.to_string()),
    }
}

#[cfg(not(feature = "blocking"))]
fn main() {
    println!("This example requires the blocking feature enabled")
}
