use std::{iter::from_fn, net::TcpStream};

use parking_lot::Mutex;
use prost::bytes::Buf;
use tungstenite::{Bytes, Error, Message, WebSocket, connect, stream::MaybeTlsStream};

use crate::{
    connection::{Connection, Feed},
    error::{OpenfeedError, OpenfeedResult},
};

/// A `SyncConnection`, having only one stream, may only be used by a single thread
/// at any given time. Attempting to send a message while the feed is being read from
/// will return a `FeedActive` error.
pub(crate) struct SyncConnection {
    stream: Mutex<WebSocket<MaybeTlsStream<TcpStream>>>,
}

#[maybe_async::sync_impl]
impl Connection for SyncConnection {
    #[inline]
    fn new(server: String) -> OpenfeedResult<Self> {
        let (s, _) = connect(server)?;
        let stream = Mutex::new(s);
        Ok(SyncConnection { stream })
    }

    #[inline]
    fn close(&self) -> OpenfeedResult<()> {
        self.stream.lock().close(None).map_err(OpenfeedError::from)
    }

    #[inline]
    fn send(&self, msg: Message) -> OpenfeedResult<()> {
        self.stream
            .try_lock()
            .ok_or(OpenfeedError::FeedActive())?
            .send(msg)
            .map_err(OpenfeedError::from)
    }

    #[inline]
    fn recv(&self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        let mut buff = Bytes::new();
        let mut stream = self.stream.lock();
        from_fn(move || {
            loop {
                if buff.remaining() >= 2 {
                    let len = buff.get_u16() as usize;
                    return Some(Ok(buff.split_to(len)));
                }
                match stream.read() {
                    Ok(Message::Binary(data)) => buff = data,
                    Ok(_) => {} // skip nonbinary messages
                    Err(Error::ConnectionClosed) => return None,
                    Err(err) => return Some(Err(OpenfeedError::from(err))),
                }
            }
        })
    }
}
