use std::string::String;

use tungstenite::{Bytes, Message};

use crate::error::OpenfeedResult;

#[cfg(any(
    feature = "gio-runtime",
    feature = "tokio-runtime",
    feature = "smol-runtime"
))]
pub(crate) mod async_conn;
#[cfg(feature = "blocking")]
pub(crate) mod sync_conn;

#[cfg(any(
    feature = "gio-runtime",
    feature = "tokio-runtime",
    feature = "smol-runtime"
))]
pub(crate) mod runtime {
    pub(crate) use super::async_conn::AsyncConnection as ConnectionType;
    pub(crate) use futures_lite::Stream as Feed;
}
#[cfg(feature = "blocking")]
pub(crate) mod runtime {
    pub(crate) use super::sync_conn::SyncConnection as ConnectionType;
    pub(crate) use std::iter::Iterator as Feed;
}

pub(crate) use runtime::*;

#[maybe_async::maybe_async(?Send)]
pub trait Connection: Sized {
    async fn new(server: String) -> OpenfeedResult<Self>;
    async fn close(&mut self) -> OpenfeedResult<()>;
    async fn send(&mut self, msg: Message) -> OpenfeedResult<()>;
    async fn recv(&mut self) -> impl Feed<Item = OpenfeedResult<Bytes>>;
}
