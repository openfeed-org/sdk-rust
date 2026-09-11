use futures::lock::Mutex;
use futures::stream::StreamExt;
use futures::stream::{self};

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

/// Unlike the blocking counterpart, an `AsyncConnection`
/// may be read from and written to simultaneously.
pub(crate) struct AsyncConnection {
    reader: Mutex<WebSocketReceiver<ConnectStream>>,
    writer: Mutex<WebSocketSender<ConnectStream>>,
}

#[maybe_async::async_impl]
impl Connection for AsyncConnection {
    #[inline]
    async fn new(server: String) -> OpenfeedResult<Self> {
        let (stream, _) = connect_async(server).await?;
        let (w, r) = stream.split();
        let writer = Mutex::new(w);
        let reader = Mutex::new(r);
        Ok(AsyncConnection { writer, reader })
    }

    #[inline]
    async fn close(&self) -> OpenfeedResult<()> {
        let mut writer = self.writer.lock().await;
        writer.close(None).await.map_err(OpenfeedError::from)
    }

    #[inline]
    async fn send(&self, msg: Message) -> OpenfeedResult<()> {
        let mut writer = self.writer.lock().await;
        writer.send(msg).await.map_err(OpenfeedError::from)
    }

    #[inline]
    async fn recv(&self) -> impl Feed<Item = OpenfeedResult<Bytes>> {
        let conn = self.reader.lock().await;
        let buff = Bytes::new();
        Box::pin(stream::unfold(
            (conn, buff),
            |(mut conn, mut buff)| async move {
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
