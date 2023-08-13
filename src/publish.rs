use crate::orderbook::{
    orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer},
    Empty, Summary,
};
use crate::{consolidate, ExchangeOrderbook};
use anyhow::Context;
use std::pin::Pin;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream};
use tonic::{transport::Server, Response, Status};

pub struct OrderbookAggregatorService {
    subscribers: broadcast::Sender<ExchangeOrderbook>,
    orderbook_depth: usize,
}
use log::{debug, info, trace};

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    type BookSummaryStream = Pin<Box<dyn Stream<Item = Result<Summary, Status>> + Send>>;

    async fn book_summary(
        &self,
        _: tonic::Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let mut receiver = self.subscribers.subscribe();
        let (tx, rx) = mpsc::channel(32);

        let orderbook_depth = self.orderbook_depth;
        // Spawn a task to listen to broadcast summaries
        tokio::spawn(async move {
            let mut consolidator = consolidate::Consolidator::new(orderbook_depth);
            while let Ok(ExchangeOrderbook {
                exchange,
                orderbook,
            }) = receiver.recv().await
            {
                debug!("Received orderbook update from {exchange}: {orderbook:?}");
                if let Some(summary) = consolidator.update(exchange, orderbook) {
                    debug!("Publishing summary: {summary:?}");
                    let _ = tx.send(Ok(summary.clone())).await;
                } else {
                    trace!("Orderbook update caused no summary update");
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

pub async fn start_grpc_server(
    socket_address: &str,
    orderbook_depth: usize,
    tx: broadcast::Sender<ExchangeOrderbook>,
) -> Result<(), anyhow::Error> {
    let addr = socket_address
        .parse()
        .with_context(|| format!("Cannot parse server socket address {socket_address}"))?;
    let orderbook_agg_service = OrderbookAggregatorService {
        subscribers: tx,
        orderbook_depth,
    };

    info!("Starting gRPC server");
    Server::builder()
        .add_service(OrderbookAggregatorServer::new(orderbook_agg_service))
        .serve(addr)
        .await
        .with_context(|| "Failed to start grpc server")?;
    Ok(())
}