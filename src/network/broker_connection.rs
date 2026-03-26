use bytes::{Bytes, BytesMut, Buf, BufMut};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Semaphore, OwnedSemaphorePermit};

use crate::error::{KafkaError, Result};

struct InFlightRequest {
    tx: oneshot::Sender<Bytes>,
    _permit: OwnedSemaphorePermit,
}

/// Channel payload: (correlation_id, request_bytes, response_sender, semaphore_permit)
type BrokerRequest = (i32, Bytes, oneshot::Sender<Bytes>, OwnedSemaphorePermit);

/// A per-broker connection that runs as a tokio task.
pub struct BrokerConnection {
    broker_id: i32,
    stream: TcpStream,
    rx: mpsc::Receiver<BrokerRequest>,
    in_flight: HashMap<i32, InFlightRequest>,
}

/// Handle for sending requests to a BrokerConnection.
#[derive(Clone)]
pub struct BrokerSender {
    broker_id: i32,
    tx: mpsc::Sender<BrokerRequest>,
    semaphore: Arc<Semaphore>,
}

impl BrokerConnection {
    /// Connect to a broker and return the connection + sender handle.
    pub async fn connect(
        broker_id: i32,
        addr: String,
        max_in_flight: usize,
    ) -> Result<(Self, BrokerSender)> {
        let stream = TcpStream::connect(&addr).await.map_err(|e| KafkaError::ConnectionFailed {
            broker_id,
            message: format!("failed to connect to {addr}: {e}"),
        })?;

        let semaphore = Arc::new(Semaphore::new(max_in_flight));
        let (tx, rx) = mpsc::channel(max_in_flight * 2);

        let conn = Self {
            broker_id,
            stream,
            rx,
            in_flight: HashMap::new(),
        };

        let sender = BrokerSender {
            broker_id,
            tx,
            semaphore,
        };

        Ok((conn, sender))
    }

    /// Run the connection loop. This should be spawned as a tokio task.
    pub async fn run(mut self) {
        let (mut reader, mut writer) = self.stream.into_split();
        let mut read_buf = BytesMut::with_capacity(8192);

        loop {
            tokio::select! {
                // Receive a request from the channel
                request = self.rx.recv() => {
                    match request {
                        Some((correlation_id, request_bytes, resp_tx, permit)) => {
                            // Write length-prefixed request
                            let len = request_bytes.len() as i32;
                            let mut frame = BytesMut::with_capacity(4 + request_bytes.len());
                            frame.put_i32(len);
                            frame.extend_from_slice(&request_bytes);

                            if let Err(e) = writer.write_all(&frame).await {
                                let _ = resp_tx.send(Bytes::new());
                                tracing::error!(broker_id = self.broker_id, "write error: {e}");
                                break;
                            }

                            self.in_flight.insert(correlation_id, InFlightRequest {
                                tx: resp_tx,
                                _permit: permit,
                            });
                        }
                        None => {
                            // Channel closed, shut down
                            break;
                        }
                    }
                }

                // Read response from the socket
                result = reader.read_buf(&mut read_buf) => {
                    match result {
                        Ok(0) => {
                            // Connection closed
                            tracing::warn!(broker_id = self.broker_id, "broker connection closed");
                            break;
                        }
                        Ok(_) => {
                            // Try to parse complete responses from the buffer
                            while read_buf.len() >= 4 {
                                let response_len = i32::from_be_bytes([
                                    read_buf[0], read_buf[1], read_buf[2], read_buf[3],
                                ]) as usize;

                                if read_buf.len() < 4 + response_len {
                                    break; // Need more data
                                }

                                // Consume the length prefix
                                read_buf.advance(4);

                                // Extract the response body
                                let response_body = read_buf.split_to(response_len).freeze();

                                // Extract correlation_id from the first 4 bytes of the response
                                if response_body.len() >= 4 {
                                    let correlation_id = i32::from_be_bytes([
                                        response_body[0], response_body[1],
                                        response_body[2], response_body[3],
                                    ]);

                                    if let Some(in_flight) = self.in_flight.remove(&correlation_id) {
                                        let _ = in_flight.tx.send(response_body);
                                        // permit is dropped here, releasing the semaphore
                                    } else {
                                        tracing::warn!(
                                            broker_id = self.broker_id,
                                            correlation_id,
                                            "received response for unknown correlation ID"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(broker_id = self.broker_id, "read error: {e}");
                            break;
                        }
                    }
                }
            }
        }

        // Fail all in-flight requests on connection close
        for (_, in_flight) in self.in_flight.drain() {
            let _ = in_flight.tx.send(Bytes::new());
        }
    }
}

impl BrokerSender {
    /// Send a raw request and wait for the response.
    pub async fn send_raw(&self, correlation_id: i32, request: Bytes) -> Result<Bytes> {
        // Acquire in-flight permit — held until response arrives
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|_| {
            KafkaError::ConnectionFailed {
                broker_id: self.broker_id,
                message: "connection closed".into(),
            }
        })?;

        let (resp_tx, resp_rx) = oneshot::channel();

        self.tx.send((correlation_id, request, resp_tx, permit)).await.map_err(|_| {
            KafkaError::ConnectionFailed {
                broker_id: self.broker_id,
                message: "connection closed".into(),
            }
        })?;

        resp_rx.await.map_err(|_| KafkaError::ConnectionFailed {
            broker_id: self.broker_id,
            message: "response channel dropped".into(),
        })
    }

    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn test_broker_connection_roundtrip() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock broker: reads request, echoes it back as response
        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            // Read length-prefixed request
            let len = socket.read_i32().await.unwrap() as usize;
            let mut buf = vec![0u8; len];
            socket.read_exact(&mut buf).await.unwrap();
            // Echo it back as length-prefixed response
            socket.write_i32(buf.len() as i32).await.unwrap();
            socket.write_all(&buf).await.unwrap();
            // Keep connection alive briefly
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });

        let (conn, sender) = BrokerConnection::connect(0, addr.to_string(), 5).await.unwrap();
        let conn_handle = tokio::spawn(conn.run());

        // Build a request with correlation_id = 42
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(42); // correlation_id
        request_bytes.extend_from_slice(b"hello");

        let response = sender.send_raw(42, request_bytes.freeze()).await.unwrap();

        // Verify correlation_id is in response
        assert!(response.len() >= 4);
        let resp_corr_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(resp_corr_id, 42);

        drop(sender);
        let _ = conn_handle.await;
        mock.abort();
    }

    #[tokio::test]
    async fn test_multiple_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mock = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..3 {
                let len = socket.read_i32().await.unwrap() as usize;
                let mut buf = vec![0u8; len];
                socket.read_exact(&mut buf).await.unwrap();
                socket.write_i32(buf.len() as i32).await.unwrap();
                socket.write_all(&buf).await.unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        });

        let (conn, sender) = BrokerConnection::connect(0, addr.to_string(), 5).await.unwrap();
        let conn_handle = tokio::spawn(conn.run());

        for i in 0..3i32 {
            let mut req = BytesMut::new();
            req.put_i32(i);
            req.extend_from_slice(&[i as u8; 4]);
            let resp = sender.send_raw(i, req.freeze()).await.unwrap();
            let resp_id = i32::from_be_bytes([resp[0], resp[1], resp[2], resp[3]]);
            assert_eq!(resp_id, i);
        }

        drop(sender);
        let _ = conn_handle.await;
        mock.abort();
    }
}
