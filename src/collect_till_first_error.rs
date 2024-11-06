/// Extension trait for Iterators to collect all items, or return first error.
///
/// Note: deprecated, as can simply do: `.collect::<Result<Vec<_>, _>>()?`
///
/// # Example
/// ```
/// # use orderbook_aggregator::collect_till_first_error::CollectTillFirstError;
/// // success scenario
/// assert_eq!(
///   Ok(vec![1, 2, 3]),
///   vec![Ok::<i32, String>(1), Ok(2), Ok(3)].into_iter().collect_till_first_error()
/// );
///
/// // returns first encountered error
/// assert_eq!(
///   Err("error"),
///   vec![Ok(1), Err("error"), Err("error2")].into_iter().collect_till_first_error()
/// );
/// ```
pub trait CollectTillFirstError<X, E> {
    fn collect_till_first_error(self) -> Result<Vec<X>, E>;
}

impl<I, X, E> CollectTillFirstError<X, E> for I
where
    I: Iterator<Item = Result<X, E>>,
{
    /// Collects either a Vec of X, or the first E.
    fn collect_till_first_error(self) -> Result<Vec<X>, E> {
        let mut result = Vec::new();
        for item in self {
            result.push(item?);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_oks() {
        let v = vec![Ok::<i32, String>(1), Ok(2), Ok(3)];
        let result = v.into_iter().collect_till_first_error();
        assert_eq!(result, Ok(vec![1, 2, 3]));
    }

    #[test]
    fn test_with_err() {
        let v = vec![Ok(1), Err("error"), Ok(3)];
        let result = v.into_iter().collect_till_first_error();
        assert_eq!(result, Err("error"));

        let v = vec![Err("error"), Ok(2), Ok(3)];
        let result = v.into_iter().collect_till_first_error();
        assert_eq!(result, Err("error"));
    }

    #[test]
    fn test_first_err() {
        let v = vec![Err("error1"), Err("error2"), Ok(3)];
        let result = v.into_iter().collect_till_first_error();
        assert_eq!(result, Err("error1"));
    }
}
