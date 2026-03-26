use super::record_metadata::RecordMetadata;
use crate::error::KafkaError;

/// Callback invoked when a produce request completes.
pub type Callback =
    Box<dyn FnOnce(std::result::Result<&RecordMetadata, &KafkaError>) + Send + 'static>;

/// A future that resolves when the broker acknowledges a produced record.
#[derive(Debug)]
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

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.rx).poll(cx) {
            std::task::Poll::Ready(Ok(result)) => std::task::Poll::Ready(result),
            std::task::Poll::Ready(Err(_)) => {
                std::task::Poll::Ready(Err(crate::KafkaError::ConnectionFailed {
                    broker_id: -1,
                    message: "producer shut down".into(),
                }))
            }
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
        }))
        .unwrap();
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

    #[test]
    fn test_callback_invoked_with_ok_result() {
        use std::sync::{Arc, Mutex};

        let received = Arc::new(Mutex::new(None));
        let received_clone = received.clone();

        let callback: Callback = Box::new(move |result| {
            if let Ok(meta) = result {
                *received_clone.lock().unwrap() =
                    Some((meta.topic.clone(), meta.partition, meta.offset));
            }
        });

        let meta = RecordMetadata {
            topic: "test-topic".into(),
            partition: 5,
            offset: 99,
            timestamp: 2000,
            serialized_key_size: 3,
            serialized_value_size: 10,
        };

        callback(Ok(&meta));

        let captured = received.lock().unwrap().take().unwrap();
        assert_eq!(captured.0, "test-topic");
        assert_eq!(captured.1, 5);
        assert_eq!(captured.2, 99);
    }

    #[test]
    fn test_callback_invoked_with_err_result() {
        use std::sync::{Arc, Mutex};

        let received_err = Arc::new(Mutex::new(false));
        let received_err_clone = received_err.clone();

        let callback: Callback = Box::new(move |result| {
            if result.is_err() {
                *received_err_clone.lock().unwrap() = true;
            }
        });

        let err = crate::KafkaError::Timeout;
        callback(Err(&err));

        assert!(*received_err.lock().unwrap());
    }

    #[tokio::test]
    async fn test_multiple_send_futures_resolve_independently() {
        let (tx1, rx1) = tokio::sync::oneshot::channel();
        let (tx2, rx2) = tokio::sync::oneshot::channel();

        let future1 = SendFuture::new(rx1);
        let future2 = SendFuture::new(rx2);

        // Resolve future2 first, then future1 — order should not matter
        tx2.send(Ok(RecordMetadata {
            topic: "t2".into(),
            partition: 1,
            offset: 200,
            timestamp: 2000,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }))
        .unwrap();

        tx1.send(Ok(RecordMetadata {
            topic: "t1".into(),
            partition: 0,
            offset: 100,
            timestamp: 1000,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }))
        .unwrap();

        let r1 = future1.await.unwrap();
        let r2 = future2.await.unwrap();

        assert_eq!(r1.topic, "t1");
        assert_eq!(r1.offset, 100);
        assert_eq!(r2.topic, "t2");
        assert_eq!(r2.offset, 200);
    }
}
