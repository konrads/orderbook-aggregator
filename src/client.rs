use anyhow::Context;
use clap::Parser;
use log::{debug, info, warn};
use tonic::transport::Channel;

/// Entrypoint for the orderbook aggregator client.
/// It connects to the gRPC server and prints the received summaries.
/// It can print the summaries as JSON or as a struct, defaulting is JSON.

mod orderbook {
    tonic::include_proto!("orderbook");
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, default_value = "http://[::1]:50051")]
    server_address: String,
    #[arg(long, default_value_t = false)]
    as_struct: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let args = Args::parse();

    let channel = Channel::from_shared(args.server_address)
        .with_context(|| "Invalid gRPC server address")?
        .connect()
        .await
        .with_context(|| "Failed to establish server connection")?;
    let mut client =
        orderbook::orderbook_aggregator_client::OrderbookAggregatorClient::new(channel);
    let request = tonic::Request::new(orderbook::Empty {});
    let mut response = client
        .book_summary(request)
        .await
        .with_context(|| "Failed to subscribe to gRPC server")?
        .into_inner();

    loop {
        match response.message().await {
            Ok(Some(summary)) => {
                let render = if args.as_struct {
                    format!("{:?}", summary)
                } else {
                    serde_json::to_string(&summary)?
                };
                info!("Received summary: {render}");
            }
            Ok(None) => {
                warn!("Received EOS, closing");
                break;
            }
            Err(e) => {
                debug!("Exiting due to error: {e}");
                break;
            }
        }
    }

    Ok(())
}
