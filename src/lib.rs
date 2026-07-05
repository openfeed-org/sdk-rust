pub mod openfeed {
    include!(concat!(env!("OUT_DIR"), "/org.openfeed.rs"));
}

pub mod error;

use mutually_exclusive_features::exactly_one_of;
exactly_one_of!("blocking", "tokio-runtime", "gio-runtime", "smol-runtime");

// Client requires a runtime to determine a connection type
#[cfg(any(
    feature = "tokio-runtime",
    feature = "gio-runtime",
    feature = "smol-runtime",
    feature = "blocking"
))]
pub mod client;

// Assumes only one `Connection` impl can be used at a time;
// either blocking or nonblocking given async runtime.
#[cfg(any(
    feature = "tokio-runtime",
    feature = "gio-runtime",
    feature = "smol-runtime",
    feature = "blocking"
))]
pub(crate) mod connection;
