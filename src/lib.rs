use anyhow::{Context, Result};
use log::{debug, error, info, trace, warn};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast::{self, Receiver};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
pub mod collect_till_first_error;
mod consolidate;
pub mod ops_ws;
pub mod publish;
mod types;
use types::*;

/// Functionality common to aggregator server.

/// Import the generated protobuf code into `orderbook` module.
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

#[derive(thiserror::Error, Debug)]
pub enum WsHandlerError {
    #[error("failed to translate between Orderbook types: {0}")]
    TranslateError(String),
    #[error("failed to parse msg")]
    ParseError(#[from] serde_json::Error),
}

/// A trait for handling websocket feeds for a specific exchange.
pub trait WsHandler: Send + Sync {
    fn url(&self) -> &str;
    fn subscribe_msg(&self, symbol: &str) -> Option<String>;
    fn max_ping_roundtrip_ms(&self) -> u128;
    fn handle_msg(&self, msg: &str) -> Result<Option<Orderbook>, WsHandlerError>;
}

/// Binance implementation of the `WsHandler` trait.
#[derive(Debug, Clone)]
pub struct BinanceWsHandler;

impl WsHandler for BinanceWsHandler {
    fn url(&self) -> &str {
        "wss://stream.binance.com:9443/ws"
    }

    /// Given symbol "ethbtc, produces: {"method": "SUBSCRIBE","params": ["ethbtc@depth10"],"id": 1}
    fn subscribe_msg(&self, symbol: &str) -> Option<String> {
        Some(
            serde_json::to_string(&BinanceReq {
                method: "SUBSCRIBE",
                params: vec![&format!("{symbol}@depth10")],
                id: 1,
            })
            .expect("failed to serialize binance subscribe req"),
        )
    }

    fn max_ping_roundtrip_ms(&self) -> u128 {
        1000
    }

    fn handle_msg(&self, msg: &str) -> Result<Option<Orderbook>, WsHandlerError> {
        match serde_json::from_str::<OrderbookMsg<'_>>(msg) {
            Ok(orderbook_msg) => {
                let orderbook: Orderbook = orderbook_msg.try_into().map_err(|e| {
                    WsHandlerError::TranslateError(format!(
                        "could not translate msg: {msg} due to {e}"
                    ))
                })?;
                Ok(Some(orderbook))
            }
            Err(_e) => {
                let resp = serde_json::from_str::<BinanceResp>(msg)?;
                info!("Received expected response: {:?}", resp);
                Ok(None)
            }
        }
    }
}

/// Bitstamp implementation of the `WsHandler` trait.
#[derive(Debug, Clone)]
pub struct BitstampWsHandler;

impl WsHandler for BitstampWsHandler {
    fn url(&self) -> &str {
        "wss://ws.bitstamp.net"
    }

    /// Given symbol "ethbtc, produces: {"event": "bts:subscribe","data": {"channel": "order_book_ethbtc"}}
    fn subscribe_msg(&self, symbol: &str) -> Option<String> {
        Some(
            serde_json::to_string(&BitstampReq {
                event: "bts:subscribe",
                data: BitstampReqData {
                    channel: &format!("order_book_{symbol}"),
                },
            })
            .expect("failed to serialize bitstamp subscribe req"),
        )
    }

    fn max_ping_roundtrip_ms(&self) -> u128 {
        1000
    }

    fn handle_msg(&self, msg: &str) -> Result<Option<Orderbook>, WsHandlerError> {
        match serde_json::from_str::<BitstampOrderbookMsg<'_>>(msg) {
            Ok(orderbook_msg) => {
                let orderbook: Orderbook = orderbook_msg.try_into().map_err(|e| {
                    WsHandlerError::TranslateError(format!(
                        "could not translate msg: {msg} due to {e}"
                    ))
                })?;
                Ok(Some(orderbook))
            }
            Err(_e) => {
                let resp = serde_json::from_str::<BitstampResp>(msg)?;
                info!("Received expected response: {:?}", resp);
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct NormalizedOrderbook {
    exchange: String,
    orderbook: Orderbook,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Control {
    PingDurationExceeded,
    Died(String),
}

/// Handle a websocket feed for a specific exchange via WsHandler.
/// Delivers orderbook updates to downstream_tx and control messages to control_tx.
/// The control messages are:
/// - PingDurationExceeded: the ping-pong roundtrip duration exceeded the max_ping_roundtrip_ms
/// - Died: the websocket handler died due to an error
/// The heartbeat_rx is used to request a ping-pong roundtrip.
pub async fn handle_exchange_feed(
    eas: &ExchangeAndSymbol,
    ws_handler: &dyn WsHandler,
    ws_ops: &mut dyn ops_ws::WSOps,
    mut heartbeat_rx: Receiver<()>,
    downstream_tx: broadcast::Sender<NormalizedOrderbook>,
    control_tx: mpsc::Sender<Control>,
) -> Result<()> {
    let subscribe_msg = ws_handler.subscribe_msg(&eas.symbol);
    info!(
        "
-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
Starting WS handler    {}
url:                   {}
subscribe_msg:         {}
max_ping_roundtrip_ms: {}
-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-",
        eas.to_string(),
        &ws_handler.url(),
        subscribe_msg.as_deref().unwrap_or("NA"),
        &ws_handler.max_ping_roundtrip_ms()
    );

    // Send subscribe command
    if let Some(subscribe_msg) = subscribe_msg {
        ws_ops
            .write(Message::Text(subscribe_msg.to_owned()))
            .await
            .context("Failed to write subscribe_msg")?;
    }

    let mut ping_requested = UNIX_EPOCH;
    let mut ping_acked = UNIX_EPOCH;

    loop {
        tokio::select! {
            biased;

            _ = heartbeat_rx.recv() => {
                trace!("ping requested!");
                if ping_requested > ping_acked {
                    debug!("Ping not ponged, propagating PingDurationExceeded");
                    control_tx.send(Control::PingDurationExceeded).await.context("Failed to send PingDurationExceeded")?;
                } else {
                    ws_ops.write(Message::Ping(b"ping".to_vec())).await.context("Failed to send Ping")?;
                    ping_requested = SystemTime::now();
                }
            }  // Send heartbeat
            incoming = ws_ops.read() => {
                match incoming {
                    Some(Ok(message)) => {
                        if let Message::Text(text) = message {
                            match ws_handler.handle_msg(&text) {
                                Ok(Some(orderbook)) => {
                                    trace!("{}: sending orderbook: {:?}", eas.to_string(), orderbook);
                                    let _ = downstream_tx.send(NormalizedOrderbook{ exchange: eas.exchange.clone(), orderbook }); // ignore failure, due to lack of subscribers
                                }
                                Ok(None) => {} // ignore
                                Err(e) => {
                                    control_tx.send(Control::Died(format!("ws handler error: {e}"))).await?;
                                    break;
                                }
                            }
                        } else if let Message::Ping(body) = message {
                            trace!("Received ping: {:?}", body);
                        } else if let Message::Pong(_) = message {
                            ping_acked = SystemTime::now();
                            let ping_duration = ping_acked.duration_since(ping_requested);
                            debug!("Received pong, duration since last ping: {:?}", ping_duration);
                            let ping_duration_ms = ping_duration.expect("Negative ping duration").as_millis();
                            if ping_duration_ms > ws_handler.max_ping_roundtrip_ms() {
                                warn!("Ping-pong roundtrip duration exceed");
                                control_tx.send(Control::PingDurationExceeded).await?;
                            }
                        } else {
                            warn!("Received unexpected message: {:?}", message);
                        }
                    }
                    Some(Err(e)) => {
                        warn!("Error reading message: {}", e);
                    }
                    None => {
                        warn!("Stream closed");
                        break;
                    }
                }

            }
        }
    }

    Ok(())
}

/// Select WS handler out of the available implementations.
pub fn get_ws_handler(exchange: &str) -> Option<&'static dyn WsHandler> {
    match exchange {
        "binance" => Some(&BinanceWsHandler),
        "bitstamp" => Some(&BitstampWsHandler),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeAndSymbol {
    pub exchange: String,
    pub symbol: String,
}

impl FromStr for ExchangeAndSymbol {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<ExchangeAndSymbol, Self::Err> {
        let mut split = s.split(':').collect::<Vec<_>>();
        if split.len() != 2 {
            Err(anyhow::anyhow!(format!(
                "Invalid format, need exchange:symbol, got {s}"
            )))
        } else {
            let exchange = split.remove(0);
            let symbol = split.remove(0);
            if exchange.is_empty() {
                Err(anyhow::anyhow!("No exchange specified"))
            } else if symbol.is_empty() {
                Err(anyhow::anyhow!("No symbol specified"))
            } else {
                Ok(ExchangeAndSymbol {
                    exchange: exchange.to_owned(),
                    symbol: symbol.to_owned(),
                })
            }
        }
    }
}

impl ToString for ExchangeAndSymbol {
    fn to_string(&self) -> String {
        format!("{}:{}", self.exchange, self.symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ops_ws::WSOps;
    use mockall::{mock, predicate::*};

    #[test]
    fn test_exchange_and_symbol_parsing_roundtrip() {
        let eas: ExchangeAndSymbol = "binance:BTCUSDT".parse().unwrap();
        assert_eq!(eas.to_string(), "binance:BTCUSDT");
        assert_eq!(
            eas,
            ExchangeAndSymbol {
                exchange: "binance".to_string(),
                symbol: "BTCUSDT".to_string()
            }
        );
    }

    #[test]
    fn test_exchange_and_symbol_parsing_invalid() {
        assert_eq!(
            ExchangeAndSymbol::from_str("binance:BTCUSDT:extra")
                .unwrap_err()
                .to_string(),
            "Invalid format, need exchange:symbol, got binance:BTCUSDT:extra"
        );
        assert_eq!(
            ExchangeAndSymbol::from_str("binance")
                .unwrap_err()
                .to_string(),
            "Invalid format, need exchange:symbol, got binance"
        );
        assert_eq!(
            ExchangeAndSymbol::from_str(":BTCUSDT")
                .unwrap_err()
                .to_string(),
            "No exchange specified"
        );
        assert_eq!(
            ExchangeAndSymbol::from_str("binance:")
                .unwrap_err()
                .to_string(),
            "No symbol specified"
        );
    }

    mock! {
        pub TestWsHandler {}
        impl WsHandler for TestWsHandler {
            fn url(&self) -> &str;
            fn subscribe_msg(&self, symbol: &str) -> Option<String>;
            fn max_ping_roundtrip_ms(&self) -> u128;
            fn handle_msg(&self, msg: &str) -> Result<Option<Orderbook>, WsHandlerError>;
        }
    }

    mock! {
        pub TestWsOps {}
        #[tonic::async_trait]
        impl WSOps for TestWsOps {
            async fn read(&mut self) -> Option<Result<Message>>;
            async fn write(&mut self, msg: Message) -> Result<()>;
        }
    }

    fn sleep_ms(ms: u64) {
        std::thread::sleep(std::time::Duration::from_millis(ms));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_handler_flow() -> Result<()> {
        let (heartbeat_tx, _) = broadcast::channel(100);
        let (control_tx, _control_rx) = mpsc::channel(100);
        let (norm_orderbook_tx, mut norm_orderbook_rx) = broadcast::channel(100);

        let eas = ExchangeAndSymbol {
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
        };

        let mut ws_handler = MockTestWsHandler::new();
        ws_handler.expect_subscribe_msg().returning(|_| None);
        ws_handler
            .expect_max_ping_roundtrip_ms()
            .returning(|| 5000_u128); // 5s, but we're not testing heartbeat
        ws_handler
            .expect_handle_msg()
            .returning(|_| Ok(Some(Orderbook::default())));

        let mut ws_ops = MockTestWsOps::new();
        ws_ops
            .expect_read()
            .times(3)
            .returning(|| Some(Ok(Message::Text("--".to_owned()))));
        ws_ops.expect_read().once().returning(|| None);

        let heartbeat_rx2 = heartbeat_tx.subscribe();
        tokio::spawn(async move {
            handle_exchange_feed(
                &eas,
                &ws_handler,
                &mut ws_ops,
                heartbeat_rx2,
                norm_orderbook_tx,
                control_tx,
            )
            .await
        });

        // Drain the broadcast receiver into the Vec
        let mut downstream_msgs = vec![];
        while let Ok(message) = norm_orderbook_rx.recv().await {
            downstream_msgs.push(message);
        }
        // validate received messages
        assert_eq!(3, downstream_msgs.len());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_excessive_ping_pong() -> Result<()> {
        let (heartbeat_tx, _) = broadcast::channel(100);
        let (control_tx, mut control_rx) = mpsc::channel(100);
        let (norm_orderbook_tx, _) = broadcast::channel(100);

        let eas = ExchangeAndSymbol {
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
        };

        let mut ws_handler = MockTestWsHandler::new();
        ws_handler.expect_subscribe_msg().returning(|_| None);
        ws_handler
            .expect_max_ping_roundtrip_ms()
            .returning(|| 50_u128);
        ws_handler.expect_handle_msg().returning(|_| Ok(None));

        let mut ws_ops = MockTestWsOps::new();
        // firstly quick pong response, followed by slow
        ws_ops.expect_read().once().returning(|| {
            sleep_ms(10);
            Some(Ok(Message::Pong(vec![])))
        });
        ws_ops.expect_read().returning(|| {
            sleep_ms(80);
            Some(Ok(Message::Pong(vec![])))
        });
        ws_ops.expect_write().returning(|_| Ok(()));

        let heartbeat_rx2 = heartbeat_tx.subscribe();
        tokio::spawn(async move {
            handle_exchange_feed(
                &eas,
                &ws_handler,
                &mut ws_ops,
                heartbeat_rx2,
                norm_orderbook_tx,
                control_tx,
            )
            .await
        });

        // expect quick pong
        heartbeat_tx.send(())?;
        sleep_ms(60);
        assert!(control_rx.try_recv().is_err());

        // expect long pong, should trigger control message
        heartbeat_tx.send(())?;
        sleep_ms(60);
        assert_eq!(
            Control::PingDurationExceeded,
            control_rx.recv().await.unwrap()
        );

        Ok(())
    }
}
