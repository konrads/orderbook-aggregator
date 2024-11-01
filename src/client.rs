use anyhow::{Context, Result};
use clap::Parser;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

mod orderbook {
    tonic::include_proto!("orderbook");
}

/// Client command line arguments.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "http://[::1]:50051")]
    server_address: String,
    #[arg(long, default_value_t = false)]
    as_struct: bool,
}

/// Entrypoint for the orderbook aggregator client.
/// Connects to the gRPC server and prints the received summaries.
/// Can print the summaries as JSON or as a struct, defaulting is JSON.
#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    let args = Args::parse();

    let channel = Channel::from_shared(args.server_address)
        .context("Invalid gRPC server address")?
        .connect()
        .await
        .context("Failed to establish server connection")?;
    let mut client =
        orderbook::orderbook_aggregator_client::OrderbookAggregatorClient::new(channel);
    let request = tonic::Request::new(orderbook::Empty {});
    let mut response = client
        .book_summary(request)
        .await
        .context("Failed to subscribe to gRPC server")?
        .into_inner();

    loop {
        match response.message().await {
            Ok(Some(summary)) => {
                let render = if args.as_struct {
                    format!("{:?}", summary)
                } else {
                    serde_json::to_string(&summary)?
                };
                info!(render, "Received summary");
            }
            Ok(None) => {
                warn!("Received EOS, closing");
                break;
            }
            Err(e) => {
                debug!(err = ?e, "Exiting due to error");
                break;
            }
        }
    }

    Ok(())
}
