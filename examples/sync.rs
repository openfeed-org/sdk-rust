#[path = "./common/lib.rs"]
mod common;

#[cfg(feature = "blocking")]
use {
    clap::Parser,
    sdk_rust::{client, error},
};

#[cfg(feature = "blocking")]
fn run(config: client::OpenfeedConfig, symbols: Vec<String>) -> error::OpenfeedResult<()> {
    let mut c = client::OpenfeedClient::new(config);
    c.connect()?;
    c.subscribe_symbols(symbols)?;
    for message in c.read_messages() {
        match message?.data {
            Some(m) => println!("{:?}", m),
            _ => {}
        }
    }
    Ok((/* Disconnect with ctrl-c */))
}

#[cfg(feature = "blocking")]
fn main() {
    let args = common::Args::parse();

    let config = client::OpenfeedConfig {
        username: args.username,
        password: args.password,
        server: args.server,
        service: args.service,
    };

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
