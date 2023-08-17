use super::orderbook;
use crate::types::Orderbook;
use std::collections::HashMap;

/// Mechanism for consolidating Orderbooks from multiple exchanges.
/// Restricts the number of levels to the top N bids and asks.
/// Publishes only if the summary has changed.
pub struct Consolidator {
    depth: usize,
    cache: HashMap<String, Orderbook>,
    latest: orderbook::Summary,
}

impl Consolidator {
    pub fn new(depth: usize) -> Self {
        Self {
            depth,
            cache: Default::default(),
            latest: orderbook::Summary::default(),
        }
    }

    /// Update with exchange orderbook, return summary if changed.
    pub fn update(
        &mut self,
        exchange: String,
        orderbook: Orderbook,
    ) -> Option<&orderbook::Summary> {
        self.cache.insert(exchange, orderbook);
        let mut bids = vec![];
        let mut asks = vec![];
        for (e, o) in self.cache.iter() {
            bids.extend(o.bids.iter().map(|l| orderbook::Level {
                exchange: e.clone(),
                price: l.price,
                amount: l.amount,
            }));
            asks.extend(o.asks.iter().map(|l| orderbook::Level {
                exchange: e.clone(),
                price: l.price,
                amount: l.amount,
            }));
        }
        bids.sort_by(|x, y| match y.price.partial_cmp(&x.price) {
            Some(std::cmp::Ordering::Greater) => std::cmp::Ordering::Greater,
            Some(std::cmp::Ordering::Less) => std::cmp::Ordering::Less,
            _ => y
                .amount
                .partial_cmp(&x.amount)
                .unwrap_or(std::cmp::Ordering::Equal),
        });
        asks.sort_by(|x, y| match x.price.partial_cmp(&y.price) {
            Some(std::cmp::Ordering::Greater) => std::cmp::Ordering::Greater,
            Some(std::cmp::Ordering::Less) => std::cmp::Ordering::Less,
            _ => y
                .amount
                .partial_cmp(&x.amount)
                .unwrap_or(std::cmp::Ordering::Equal),
        });
        let bids = bids.into_iter().take(self.depth).collect::<Vec<_>>();
        let asks = asks.into_iter().take(self.depth).collect::<Vec<_>>();
        let spread = match (bids.first(), asks.first()) {
            (Some(bid), Some(ask)) => ask.price - bid.price,
            _ => 0.0,
        };
        let summary = orderbook::Summary { spread, bids, asks };
        if summary == self.latest {
            None
        } else {
            self.latest = summary;
            Some(&self.latest)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types;

    fn validate_update(
        consolidator: &mut Consolidator,
        exchange: &str,
        orderbook: &str,
        exp_summary: Option<&str>,
    ) {
        let orderbook = serde_json::from_str::<types::Orderbook>(orderbook).unwrap();
        let exp_summary =
            exp_summary.map(|s| serde_json::from_str::<orderbook::Summary>(s).unwrap());
        let summary = consolidator
            .update(exchange.to_string(), orderbook)
            .map(|x| x.clone());
        assert_eq!(exp_summary, summary);
    }

    #[test]
    fn test_update() {
        let mut consolidator = Consolidator::new(2);
        validate_update(
            &mut consolidator,
            "binance",
            r#"{"bids": [{"price": 6.0, "amount": 6.0}, {"price": 4.0, "amount": 4.0}, {"price": 5.0, "amount": 5.0}], "asks": [{"price": 7.0, "amount": 7.0}, {"price": 9.0, "amount": 9.0}, {"price": 8.0, "amount": 8.0}]}"#,
            Some(
                r#"{"spread": 1.0, "bids": [{"exchange": "binance", "price": 6.0, "amount": 6.0}, {"exchange": "binance", "price": 5.0, "amount": 5.0}], "asks": [{"exchange": "binance", "price": 7.0, "amount": 7.0}, {"exchange": "binance", "price": 8.0, "amount": 8.0}]}"#,
            ),
        );

        // repeat of the top 2
        validate_update(
            &mut consolidator,
            "binance",
            r#"{"bids": [{"price": 6.0, "amount": 6.0}, {"price": 3.0, "amount": 3.0}, {"price": 5.0, "amount": 5.0}], "asks": [{"price": 7.0, "amount": 7.0}, {"price": 99.0, "amount": 99.0}, {"price": 8.0, "amount": 8.0}]}"#,
            None,
        );

        // add bitstamp, add bids/asks with the same price,
        validate_update(
            &mut consolidator,
            "bitstamp",
            r#"{"bids": [{"price": 6.0, "amount": 60.0}, {"price": 3.0, "amount": 3.0}, {"price": 5.0, "amount": 5.0}], "asks": [{"price": 7.0, "amount": 3.5}, {"price": 99.0, "amount": 99.0}, {"price": 8.0, "amount": 8.0}]}"#,
            Some(
                r#"{"spread": 1.0, "bids": [{"exchange": "bitstamp", "price": 6.0, "amount": 60.0}, {"exchange": "binance", "price": 6.0, "amount": 6.0}], "asks": [{"exchange": "binance", "price": 7.0, "amount": 7.0}, {"exchange": "bitstamp", "price": 7.0, "amount": 3.5}]}"#,
            ),
        );

        // add binance, squeeze out last orders, put in cross (-ve spread)
        validate_update(
            &mut consolidator,
            "binance",
            r#"{"bids": [{"price": 7.0, "amount": 70.0}, {"price": 3.0, "amount": 3.0}, {"price": 5.0, "amount": 5.0}], "asks": [{"price": 6.5, "amount": 6.5}, {"price": 99.0, "amount": 99.0}, {"price": 8.0, "amount": 8.0}]}"#,
            Some(
                r#"{"spread": -0.5, "bids": [{"exchange": "binance", "price": 7.0, "amount": 70.0}, {"exchange": "bitstamp", "price": 6.0, "amount": 60.0}], "asks": [{"exchange": "binance", "price": 6.5, "amount": 6.5}, {"exchange": "bitstamp", "price": 7.0, "amount": 3.5}]}"#,
            ),
        );
    }
}
