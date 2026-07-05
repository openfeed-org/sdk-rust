use futures_lite::stream::StreamExt;
use futures_lite::stream::{self};

#[cfg(feature = "gio-runtime")]
use async_tungstenite::gio::{ConnectStream, connect_async};
#[cfg(feature = "smol-runtime")]
use async_tungstenite::smol::{ConnectStream, connect_async};
#[cfg(feature = "tokio-runtime")]
use async_tungstenite::tokio::{ConnectStream, connect_async};
use async_tungstenite::{WebSocketReceiver, WebSocketSender};

use prost::bytes::Buf;
use tungstenite::{Bytes, Error, Message};

use crate::{
    connection::{Connection, Feed},
    error::{OpenfeedError, OpenfeedResult},
};

pub(crate) struct AsyncConnection {
    reader: WebSocketReceiver<ConnectStream>,
    writer: WebSocketSender<ConnectStream>,
}

#[maybe_async::async_impl(?Send)]
impl Connection for AsyncConnection {
    #[inline]
    async fn new(server: String) -> OpenfeedResult<Self> {
        let (stream, _) = connect_async(server).await?;
        let (writer, reader) = stream.split();
        Ok(AsyncConnection { writer, reader })
    }

    #[inline]
    async fn close(&mut self) -> OpenfeedResult<()> {
        self.writer.close(None).await.map_err(OpenfeedError::from)
    }

    #[inline]
    async fn send(&mut self, msg: Message) -> OpenfeedResult<()> {
        self.writer.send(msg).await.map_err(OpenfeedError::from)
    }

    #[inline]
    async fn recv(&mut self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        let conn = &mut self.reader;
        let buff = Bytes::new();
        Box::pin(stream::unfold(
            (conn, buff),
            |(conn, mut buff)| async move {
                loop {
                    if buff.remaining() >= 2 {
                        let len = buff.get_u16() as usize;
                        let msg = buff.split_to(len);
                        return Some((Ok(msg), (conn, buff)));
                    }
                    match conn.next().await {
                        Some(Ok(Message::Binary(data))) => buff = data,
                        Some(Ok(_)) => {} // skip nonbinary messages
                        Some(Err(Error::ConnectionClosed)) | None => return None,
                        Some(Err(err)) => {
                            return Some((Err(OpenfeedError::from(err)), (conn, buff)));
                        }
                    }
                }
            },
        ))
    }
}
