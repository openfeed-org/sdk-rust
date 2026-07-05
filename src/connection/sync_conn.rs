use std::{iter::from_fn, net::TcpStream};

use prost::bytes::Buf;
use tungstenite::{Bytes, Error, Message, WebSocket, connect, stream::MaybeTlsStream};

use crate::{
    connection::{Connection, Feed},
    error::{OpenfeedError, OpenfeedResult},
};

pub(crate) struct SyncConnection {
    stream: WebSocket<MaybeTlsStream<TcpStream>>,
}

#[maybe_async::sync_impl]
impl Connection for SyncConnection {
    #[inline]
    fn new(server: String) -> OpenfeedResult<Self> {
        let (stream, _) = connect(server)?;
        Ok(SyncConnection { stream })
    }

    #[inline]
    fn close(&mut self) -> OpenfeedResult<()> {
        self.stream.close(None).map_err(OpenfeedError::from)
    }

    #[inline]
    fn send(&mut self, msg: Message) -> OpenfeedResult<()> {
        self.stream.send(msg).map_err(OpenfeedError::from)
    }

    #[inline]
    fn recv(&mut self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        let stream = &mut self.stream;
        let mut buff = Bytes::new();
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
