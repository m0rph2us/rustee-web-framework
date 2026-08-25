use std::{future::Future, time::Duration};

use tokio::time::timeout;

pub(crate) async fn bounded<T, E, F>(operation_timeout: Duration, operation: F) -> Result<T, ()>
where
    F: Future<Output = Result<T, E>>,
{
    timeout(operation_timeout, operation)
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}
