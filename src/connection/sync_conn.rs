use std::{iter::from_fn, net::TcpStream};

use prost::bytes::Buf;
use tungstenite::{Bytes, Error, Message, WebSocket, connect, stream::MaybeTlsStream};
use parking_lot::Mutex;

use crate::{
    connection::{Connection, Feed},
    error::{OpenfeedError, OpenfeedResult},
};

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
        self.stream.lock().send(msg).map_err(OpenfeedError::from)
    }

    #[inline]
    fn recv(&self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        let mut buff = Bytes::new();
        from_fn(move || {
            loop {
                if buff.remaining() >= 2 {
                    let len = buff.get_u16() as usize;
                    return Some(Ok(buff.split_to(len)));
                }
                match self.stream.lock().read() {
                    Ok(Message::Binary(data)) => buff = data,
                    Ok(_) => {} // skip nonbinary messages
                    Err(Error::ConnectionClosed) => return None,
                    Err(err) => return Some(Err(OpenfeedError::from(err))),
                }
            }
        })
    }
}
