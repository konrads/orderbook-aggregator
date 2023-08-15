use anyhow::{Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tonic::async_trait;

/// Mockable websocket operations.

#[async_trait]
pub trait WSOps: Send + Sync {
    async fn read(&mut self) -> Option<Result<Message>>;
    async fn write(&mut self, msg: Message) -> Result<()>;
}

pub struct WSOpsImpl {
    ws_write: SplitSink<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>, Message>,
    ws_read: SplitStream<WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>>,
}

impl WSOpsImpl {
    pub async fn new(url: &str) -> Result<Self> {
        let (socket, _) = connect_async(url).await?;
        let (ws_write, ws_read) = socket.split();
        Ok(WSOpsImpl { ws_write, ws_read })
    }

    // pub fn writer(
    //     &mut self,
    // ) -> &mut dyn SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> {
    //     &mut self.ws_write
    // }
}

#[async_trait]
impl WSOps for WSOpsImpl {
    async fn read(&mut self) -> Option<Result<Message>> {
        self.ws_read
            .next()
            .await
            .map(|x| x.context("read_ws failure"))
    }

    async fn write(&mut self, msg: Message) -> Result<()> {
        self.ws_write.send(msg).await.context("write_ws failure")
    }
}
