use crate::error::KafkaError;
use super::record_metadata::RecordMetadata;

/// Callback invoked when a produce request completes.
pub type Callback = Box<dyn FnOnce(std::result::Result<&RecordMetadata, &KafkaError>) + Send + 'static>;

/// A future that resolves when the broker acknowledges a produced record.
pub struct SendFuture {
    rx: tokio::sync::oneshot::Receiver<crate::Result<RecordMetadata>>,
}

impl SendFuture {
    pub fn new(rx: tokio::sync::oneshot::Receiver<crate::Result<RecordMetadata>>) -> Self {
        Self { rx }
    }
}

impl std::future::Future for SendFuture {
    type Output = crate::Result<RecordMetadata>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(Ok(result)) => std::task::Poll::Ready(result),
            std::task::Poll::Ready(Err(_)) => std::task::Poll::Ready(Err(crate::KafkaError::ConnectionFailed {
                broker_id: -1,
                message: "producer shut down".into(),
            })),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_send_future_resolves() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let future = SendFuture::new(rx);
        tx.send(Ok(RecordMetadata {
            topic: "test".into(),
            partition: 0,
            offset: 42,
            timestamp: 1000,
            serialized_key_size: -1,
            serialized_value_size: 5,
        })).unwrap();
        let result = future.await.unwrap();
        assert_eq!(result.offset, 42);
    }

    #[tokio::test]
    async fn test_send_future_dropped_sender() {
        let (tx, rx) = tokio::sync::oneshot::channel::<crate::Result<RecordMetadata>>();
        drop(tx);
        let future = SendFuture::new(rx);
        let result = future.await;
        assert!(result.is_err());
    }
}
