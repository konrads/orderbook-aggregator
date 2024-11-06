use clap::Parser;
use futures_util::future;
use orderbook_aggregator::{
    get_ws_handler, handle_exchange_feed, ops_ws::WSOpsImpl, publish, Control, ExchangeAndSymbol,
};
use std::process;
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};
use tracing::debug;

/// Server command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    pub exchange_and_symbol: Vec<ExchangeAndSymbol>,
    #[arg(long, default_value_t = 5000)]
    ping_interval_ms: u64,
    #[arg(long, default_value_t = 10)]
    orderbook_depth: usize,
    #[arg(long, default_value_t = 50051)]
    grpc_port: u16,
}

/// The main entry point for the orderbook aggregator.
/// It spawns:
/// - websocket handler for each exchange and symbol pair
/// - gRPC publisher to serve updates to consolidated orderbook
/// - periodic heartbeat schedule to check if the websocket handlers are still alive
/// - control message handler to eg. exit the process in case of websocket handler failure, excessively long pin-pong
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    pretty_env_logger::init();

    let (heartbeat_tx, _) = broadcast::channel(1);
    let (control_tx, mut control_rx) = mpsc::channel(1);
    let (norm_orderbook_tx, _) = broadcast::channel(args.exchange_and_symbol.len() * 5); // able to handle 5x more updates than exchanges

    // exchange WS handler tasks
    let exchange_ws_handles = args
        .exchange_and_symbol
        .into_iter()
        .map(|eas: ExchangeAndSymbol| {
            let ws_handler = get_ws_handler(&eas.exchange)
                .unwrap_or_else(|| panic!("No handler for exchange {}", eas.exchange));

            let downstream_tx = norm_orderbook_tx.clone();
            let admin_tx = control_tx.clone();
            let heartbeat_rx = heartbeat_tx.subscribe();
            tokio::spawn(async move {
                let mut ws_ops = WSOpsImpl::new(ws_handler.url()).await.unwrap_or_else(|e| {
                    panic!("Failed to connect to url {}: {}", ws_handler.url(), e)
                });

                handle_exchange_feed(
                    &eas,
                    ws_handler,
                    &mut ws_ops,
                    heartbeat_rx,
                    downstream_tx,
                    admin_tx,
                )
                .await
                .unwrap_or_else(|e| panic!("WS handling error {e}"));
            })
        })
        .collect::<Vec<_>>();

    // periodic heartbeat task
    let heartbeat_handle = tokio::spawn(async move {
        let ping_interval = Duration::from_millis(args.ping_interval_ms);
        let mut interval = time::interval(ping_interval);

        loop {
            interval.tick().await;
            let _ = heartbeat_tx.send(());
        }
    });

    // control message handler task
    let control_handle = tokio::spawn(async move {
        if let Some(control) = control_rx.recv().await {
            match control {
                Control::Died(cause) => {
                    debug!(cause, "Exiting");
                    process::exit(1)
                }
                Control::PingDurationExceeded => {
                    debug!("Exiting due to PingDurationExceeded");
                    process::exit(2)
                }
            }
        }
    });

    // grpc publisher task
    let grpc_address = format!("[::]:{}", args.grpc_port);
    let grpc_handle = tokio::spawn(async move {
        publish::start_grpc_server(&grpc_address, args.orderbook_depth, norm_orderbook_tx)
            .await
            .unwrap()
    });

    // await finish of the first task, which will be due to an error that is propagated to the main
    let mut all_handles = vec![heartbeat_handle, control_handle, grpc_handle];
    all_handles.extend(exchange_ws_handles);
    Ok(future::select_all(all_handles).await.0?)
}
