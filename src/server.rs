use anyhow::Result;
use clap::Parser;
use log::{debug, error};
use orderbook_aggregator::{
    get_ws_handler, handle_exchange_feed, publish, Control, ExchangeAndSymbol,
};
use std::process;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};

/// The main entry point for the orderbook aggregator.
/// It spawns:
/// - websocket handler for each exchange and symbol pair
/// - gRPC publisher to serve updates to consolidated orderbook
/// - periodic heartbeat schedule to check if the websocket handlers are still alive
/// - control message handler to eg. exit the process in case of websocket handler failure, excessively long pin-pong

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    pub exchange_and_symbol: Vec<ExchangeAndSymbol>,
    #[arg(long, default_value_t = 5000)]
    ping_interval_ms: u64,
    #[arg(long, default_value_t = 10)]
    orderbook_depth: usize,
    #[arg(long, default_value_t = 50051)]
    grpc_port: u16,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    pretty_env_logger::init();

    let (heartbeat_tx, _) = broadcast::channel(1);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (norm_orderbook_tx, _) = broadcast::channel(1);

    let _exchange_ws_handlers = args
        .exchange_and_symbol
        .into_iter()
        .map(|eas: ExchangeAndSymbol| {
            let ws_handler = get_ws_handler(&eas.exchange).unwrap_or_else(|| {
                error!("Unsupported exchange {}", eas.exchange);
                process::exit(1)
            });

            let downstream_tx = norm_orderbook_tx.clone();
            let admin_tx = control_tx.clone();
            let heartbeat_rx = heartbeat_tx.subscribe();
            tokio::spawn(async move {
                handle_exchange_feed(&eas, ws_handler, heartbeat_rx, downstream_tx, admin_tx)
                    .await
                    .unwrap_or_else(|e| {
                        error!(
                            "Failed to start the websocket handler for exchange {} due to {}",
                            eas.to_string(),
                            e
                        );
                        process::exit(1)
                    })
            })
        })
        .collect::<Vec<_>>();

    // spawn periodic heartbeat schedule
    tokio::spawn(async move {
        let ping_interval = Duration::from_millis(args.ping_interval_ms);
        let mut interval = time::interval(ping_interval);

        loop {
            interval.tick().await;
            let _ = heartbeat_tx.send(());
        }
    });

    tokio::spawn(async move {
        // process control messages
        while let Some(control) = control_rx.recv().await {
            match control {
                Control::Died(cause) => {
                    debug!("Exiting due to {cause}");
                    process::exit(1)
                }
                Control::PingDurationExceeded => {
                    debug!("Exiting due to PingDurationExceeded");
                    process::exit(1)
                }
            }
        }
    });

    let grpc_address = format!("[::]:{}", args.grpc_port);
    publish::start_grpc_server(&grpc_address, args.orderbook_depth, norm_orderbook_tx).await?;
    Ok(())
}
