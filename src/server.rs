use clap::Parser;
use futures_util::future;
use orderbook_aggregator::{
    get_ws_handler, handle_exchange_feed, ops_ws::WSOpsImpl, publish, Control, ExchangeAndSymbol,
};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{self, Duration};

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
async fn main() {
    let args = Args::parse();
    pretty_env_logger::init();

    let (heartbeat_tx, _) = broadcast::channel(16);
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
            let control_tx = control_tx.clone();
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
                    control_tx,
                )
                .await
                .unwrap();
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

    // grpc publisher task
    let grpc_address = format!("[::]:{}", args.grpc_port);
    let grpc_server_handle = tokio::spawn(async move {
        publish::start_grpc_server(&grpc_address, args.orderbook_depth, norm_orderbook_tx).await
    });

    // await finish of the tasks
    tokio::select! {
        control = control_rx.recv() => {
            if let Some(control) = control {
                match control {
                    Control::Died(cause) => {
                        panic!("Exiting due to {cause}");
                    }
                    Control::PingDurationExceeded => {
                        panic!("Exiting due to PingDurationExceeded");
                    }
                }
            }
        }

        _ = future::select_all(exchange_ws_handles) => {
            panic!("Unexpected end to exchange processes");
        }

        _ = grpc_server_handle => {
            panic!("Unexpected end to grpc server");
        }

        _ = heartbeat_handle => {
            panic!("Unexpected end of heartbeat");
        }

        _ = tokio::signal::ctrl_c() => {
            panic!("Exiting due to ctrl-c")
        }
    }
}
