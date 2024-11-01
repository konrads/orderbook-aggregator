use crate::orderbook::{
    orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
    Empty, Summary,
};
use crate::{consolidate, NormalizedOrderbook};
use anyhow::Context;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{transport::Server, Response, Status};
use tracing::{debug, info, trace};

/// Implementation of the OrderbookAggregator gRPC server.
/// Receives normalized Orderbooks from the websocket handlers and publishes consolidated orderbook updates to the subscribers.
/// Updates are only issued if the consolidated update differs from the previous.

struct OrderbookAggregatorService {
    upstream_tx: broadcast::Sender<NormalizedOrderbook>,
    orderbook_depth: usize,
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    async fn book_summary(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let mut upstream_rx = self.upstream_tx.subscribe();
        let (downstream_tx, downstream_rx) = mpsc::channel(32);

        let orderbook_depth = self.orderbook_depth;
        // Spawn a task to listen to broadcast summaries
        tokio::spawn(async move {
            let mut consolidator = consolidate::Consolidator::new(orderbook_depth);
            while let Ok(NormalizedOrderbook {
                exchange,
                orderbook,
            }) = upstream_rx.recv().await
            {
                debug!(exchange, ?orderbook, "Received orderbook update");
                if let Some(summary) = consolidator.update(exchange, orderbook) {
                    debug!(?summary, "Publishing summary");
                    if let Err(e) = downstream_tx.send(Ok(summary.clone())).await {
                        debug!(err = ?e, "Failed to send summary, cancelling subscription");
                        break;
                    }
                } else {
                    trace!("Orderbook update caused no summary update");
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(downstream_rx))))
    }
}

pub async fn start_grpc_server(
    socket_address: &str,
    orderbook_depth: usize,
    upstream_tx: broadcast::Sender<NormalizedOrderbook>,
) -> Result<(), anyhow::Error> {
    let addr = socket_address
        .parse()
        .with_context(|| format!("Cannot parse server socket address {socket_address}"))?;
    let orderbook_agg_service = OrderbookAggregatorService {
        upstream_tx,
        orderbook_depth,
    };

    info!("Starting gRPC server");
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_agg_service))
        .serve(addr)
        .await
        .context("Failed to start grpc server")
}

#[cfg(test)]
mod tests {
    use crate::types::{Level, Orderbook};

    use super::*;
    use futures_util::StreamExt;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_publish() {
        let req = tonic::Request::new(Empty {});
        let (norm_orderbook_tx, _) = broadcast::channel(100);
        let service = OrderbookAggregatorService {
            upstream_tx: norm_orderbook_tx.clone(),
            orderbook_depth: 10,
        };

        let handle = tokio::spawn(async move {
            service
                .book_summary(req)
                .await
                .unwrap()
                .into_inner()
                .next()
                .await
                .unwrap()
                .unwrap()
        });

        // wait for the publisher inner spawn to succeed
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        norm_orderbook_tx
            .send(NormalizedOrderbook {
                exchange: "binance".to_owned(),
                orderbook: Orderbook {
                    bids: vec![Level {
                        price: 1.0,
                        amount: 1.0,
                    }],
                    asks: vec![
                        Level {
                            price: 1.1,
                            amount: 1.1,
                        },
                        Level {
                            price: 2.1,
                            amount: 2.1,
                        },
                    ],
                },
            })
            .unwrap();

        let summary: Summary = handle.await.unwrap();
        assert_eq!(summary.bids.len(), 1);
        assert_eq!(summary.asks.len(), 2);
    }
}
