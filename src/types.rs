use serde::{Deserialize, Serialize};

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone, Default)]
pub struct Orderbook {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[cfg_attr(test, derive(Deserialize))]
#[derive(Debug, Clone)]
pub struct Level {
    pub price: f64,
    pub amount: f64,
}

#[derive(Debug, Deserialize)]
pub struct OrderbookMsg<'a> {
    #[serde(borrow)]
    bids: Vec<[&'a str; 2]>,
    #[serde(borrow)]
    asks: Vec<[&'a str; 2]>,
}

#[derive(Debug, Serialize)]
pub struct BinanceReq<'a> {
    pub method: &'a str,
    pub params: Vec<&'a str>,
    pub id: u32,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BinanceResp {
    pub result: Option<bool>,
    pub id: u32,
}

impl TryInto<Orderbook> for OrderbookMsg<'_> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Orderbook, Self::Error> {
        let bids = self
            .bids
            .into_iter()
            .map(|[price_str, amount_str]| -> Result<Level, Self::Error> {
                Ok(Level {
                    price: price_str.parse::<f64>()?,
                    amount: amount_str.parse::<f64>()?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let asks = self
            .asks
            .into_iter()
            .map(|[price_str, amount_str]| -> Result<Level, Self::Error> {
                Ok(Level {
                    price: price_str.parse::<f64>()?,
                    amount: amount_str.parse::<f64>()?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Orderbook { bids, asks })
    }
}

impl TryInto<Orderbook> for BitstampOrderbookMsg<'_> {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Orderbook, Self::Error> {
        let bids = self
            .data
            .bids
            .into_iter()
            .map(|[price_str, amount_str]| -> Result<Level, Self::Error> {
                Ok(Level {
                    price: price_str.parse::<f64>()?,
                    amount: amount_str.parse::<f64>()?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let asks = self
            .data
            .asks
            .into_iter()
            .map(|[price_str, amount_str]| -> Result<Level, Self::Error> {
                Ok(Level {
                    price: price_str.parse::<f64>()?,
                    amount: amount_str.parse::<f64>()?,
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Orderbook { bids, asks })
    }
}

#[derive(Debug, Serialize)]
pub struct BitstampReq<'a> {
    #[serde(borrow)]
    pub event: &'a str,
    pub data: BitstampReqData<'a>,
}

#[derive(Debug, Serialize)]
pub struct BitstampReqData<'a> {
    #[serde(borrow)]
    pub channel: &'a str,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct BitstampResp<'a> {
    #[serde(borrow)]
    pub event: &'a str,
    #[serde(borrow)]
    pub channel: &'a str,
}

#[derive(Debug, Deserialize)]
pub struct BitstampOrderbookMsg<'a> {
    #[serde(borrow)]
    pub data: OrderbookMsg<'a>,
}
