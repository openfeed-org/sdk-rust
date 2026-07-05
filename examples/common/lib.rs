use clap::Parser;

/// A client for interacting with the Openfeed Protocol
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Account username
    #[arg(short, long)]
    pub username: String,

    /// Account username
    #[arg(short, long)]
    pub password: String,

    /// Service to access. May be one of available services: REAL_TIME | DELAYED
    #[arg(long, default_value_t={"REAL_TIME".to_string()})]
    pub service: String,

    /// Server to connect to
    #[arg(long, default_value_t={"openfeed.aws.barchart.com".to_string()})]
    pub server: String,

    /// Comma separated list of symbols
    pub symbols: String,
}
