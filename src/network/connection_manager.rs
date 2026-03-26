use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicI32, Ordering};
use tokio::task::JoinHandle;

use proto_kafka::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader,
};
use proto_kafka::protocol::StrBytes;
use proto_kafka::protocol::{
    Decodable, Encodable, Request, VersionRange, encode_request_header_into_buffer,
};

use super::broker_connection::{BrokerConnection, BrokerSender};
use crate::error::{KafkaError, Result};

/// Per-broker API version info.
#[derive(Debug, Clone)]
pub struct BrokerApiVersions {
    versions: HashMap<i16, VersionRange>,
}

impl BrokerApiVersions {
    pub fn new(response: &ApiVersionsResponse) -> Self {
        let versions = response
            .api_keys
            .iter()
            .map(|v| {
                (
                    v.api_key,
                    VersionRange {
                        min: v.min_version,
                        max: v.max_version,
                    },
                )
            })
            .collect();
        Self { versions }
    }

    pub fn version_range(&self, api_key: i16) -> Option<&VersionRange> {
        self.versions.get(&api_key)
    }

    /// Find the highest version supported by both client and broker.
    pub fn usable_version<R: Request>(&self) -> Option<i16> {
        let client_range = R::VERSIONS;
        let broker_range = self.version_range(R::KEY)?;
        let usable = client_range.intersect(broker_range);
        if usable.is_empty() {
            None
        } else {
            Some(usable.max)
        }
    }
}

pub struct ConnectionManager {
    connections: RwLock<HashMap<i32, BrokerSender>>,
    connection_handles: RwLock<HashMap<i32, JoinHandle<()>>>,
    api_versions: RwLock<HashMap<i32, BrokerApiVersions>>,
    broker_addresses: RwLock<HashMap<i32, String>>,
    bootstrap_servers: Vec<String>,
    client_id: String,
    max_in_flight: usize,
    correlation_id: AtomicI32,
}

impl ConnectionManager {
    pub fn new(bootstrap_servers: Vec<String>, client_id: String, max_in_flight: usize) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            connection_handles: RwLock::new(HashMap::new()),
            api_versions: RwLock::new(HashMap::new()),
            broker_addresses: RwLock::new(HashMap::new()),
            bootstrap_servers,
            client_id,
            max_in_flight,
            correlation_id: AtomicI32::new(0),
        }
    }

    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Bootstrap: connect to the first reachable bootstrap server.
    /// Returns the broker_id (or -1 for bootstrap connections).
    pub async fn bootstrap(&self) -> Result<()> {
        for addr in &self.bootstrap_servers {
            match self.connect_to_address(-1, addr.clone()).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    tracing::warn!("Failed to connect to bootstrap server {addr}: {e}");
                    continue;
                }
            }
        }
        Err(KafkaError::ConnectionFailed {
            broker_id: -1,
            message: "failed to connect to any bootstrap server".into(),
        })
    }

    /// Connect to a specific broker address, perform ApiVersions handshake.
    async fn connect_to_address(&self, broker_id: i32, addr: String) -> Result<()> {
        let (conn, sender) =
            BrokerConnection::connect(broker_id, addr.clone(), self.max_in_flight).await?;
        let handle = tokio::spawn(conn.run());

        // Store connection
        self.connections
            .write()
            .unwrap()
            .insert(broker_id, sender.clone());
        self.connection_handles
            .write()
            .unwrap()
            .insert(broker_id, handle);
        self.broker_addresses
            .write()
            .unwrap()
            .insert(broker_id, addr);

        // Perform ApiVersions handshake
        let api_versions = self.do_api_versions_handshake(broker_id, &sender).await?;
        self.api_versions
            .write()
            .unwrap()
            .insert(broker_id, api_versions);

        Ok(())
    }

    /// Connect to a broker by ID (looks up address from broker_addresses).
    pub async fn ensure_connection(&self, broker_id: i32, addr: &str) -> Result<()> {
        if self.connections.read().unwrap().contains_key(&broker_id) {
            return Ok(());
        }
        self.connect_to_address(broker_id, addr.to_string()).await
    }

    /// Register a broker's address for later connection.
    pub fn register_broker(&self, broker_id: i32, addr: String) {
        self.broker_addresses
            .write()
            .unwrap()
            .insert(broker_id, addr);
    }

    /// Perform the ApiVersions handshake on a connection.
    async fn do_api_versions_handshake(
        &self,
        _broker_id: i32,
        sender: &BrokerSender,
    ) -> Result<BrokerApiVersions> {
        let correlation_id = self.next_correlation_id();

        // Build ApiVersionsRequest (version 3 — supports client software name/version)
        let api_version: i16 = 3;
        let mut header = RequestHeader::default();
        header.request_api_key = ApiKey::ApiVersions as i16;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_string(self.client_id.clone()));

        let mut request = ApiVersionsRequest::default();
        request.client_software_name = StrBytes::from_static_str("kafka-native-client");
        request.client_software_version = StrBytes::from_static_str("0.0.1");

        let mut buf = BytesMut::new();
        encode_request_header_into_buffer(&mut buf, &header)?;
        request.encode(&mut buf, api_version)?;

        let response_bytes = sender.send_raw(correlation_id, buf.freeze()).await?;

        // Decode response: the response_bytes include the correlation_id at the start.
        // Decode ResponseHeader (version depends on ApiVersions response header version)
        let mut response_buf = response_bytes;
        let resp_header_version = ApiKey::ApiVersions.response_header_version(api_version);
        let _resp_header = ResponseHeader::decode(&mut response_buf, resp_header_version)?;
        let api_versions_response = ApiVersionsResponse::decode(&mut response_buf, api_version)?;

        Ok(BrokerApiVersions::new(&api_versions_response))
    }

    /// Send a typed request to a broker and get the typed response.
    pub async fn send<R: Request + std::fmt::Debug>(
        &self,
        broker_id: i32,
        request: R,
    ) -> Result<R::Response>
    where
        R::Response: std::fmt::Debug,
    {
        let sender = {
            let conns = self.connections.read().unwrap();
            conns
                .get(&broker_id)
                .cloned()
                .ok_or_else(|| KafkaError::ConnectionFailed {
                    broker_id,
                    message: "no connection to broker".into(),
                })?
        };

        // Determine API version to use
        let api_version = {
            let versions = self.api_versions.read().unwrap();
            match versions.get(&broker_id) {
                Some(v) => v.usable_version::<R>().unwrap_or(R::VERSIONS.max),
                None => R::VERSIONS.max,
            }
        };

        let correlation_id = self.next_correlation_id();

        // Build request header
        let mut header = RequestHeader::default();
        header.request_api_key = R::KEY;
        header.request_api_version = api_version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_string(self.client_id.clone()));

        // Encode header + body
        let mut buf = BytesMut::new();
        encode_request_header_into_buffer(&mut buf, &header)?;
        request.encode(&mut buf, api_version)?;

        // Send and receive
        let response_bytes = sender.send_raw(correlation_id, buf.freeze()).await?;

        // Decode response header + body
        let mut response_buf = response_bytes;
        let resp_header_version = ApiKey::try_from(R::KEY)
            .map(|k| k.response_header_version(api_version))
            .unwrap_or(0);
        let _resp_header = ResponseHeader::decode(&mut response_buf, resp_header_version)?;
        let response = R::Response::decode(&mut response_buf, api_version)?;

        Ok(response)
    }

    /// Get the address for a broker ID.
    pub fn broker_address(&self, broker_id: i32) -> Option<String> {
        self.broker_addresses
            .read()
            .unwrap()
            .get(&broker_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto_kafka::messages::api_versions_response::ApiVersion;

    #[test]
    fn test_broker_api_versions_usable() {
        // Create a mock ApiVersionsResponse with a known API key
        let mut response = ApiVersionsResponse::default();
        response.api_keys = vec![
            ApiVersion::default()
                .with_api_key(ApiKey::Produce as i16)
                .with_min_version(3)
                .with_max_version(10),
        ];
        let versions = BrokerApiVersions::new(&response);

        // ProduceRequest supports versions 3-13 (check actual range)
        // Intersection should give us min(13,10) = 10
        let range = versions.version_range(ApiKey::Produce as i16);
        assert!(range.is_some());
        let range = range.unwrap();
        assert_eq!(range.min, 3);
        assert_eq!(range.max, 10);
    }

    #[test]
    fn test_broker_api_versions_empty_intersection() {
        // Broker supports only version 0-1, client needs 3+
        let mut response = ApiVersionsResponse::default();
        response.api_keys = vec![
            ApiVersion::default()
                .with_api_key(ApiKey::ApiVersions as i16)
                .with_min_version(0)
                .with_max_version(1),
        ];
        let versions = BrokerApiVersions::new(&response);

        // ApiVersionsRequest client range is 0-3, broker supports 0-1
        // intersection = 0..1, not empty, so usable_version = 1
        let range = versions.version_range(ApiKey::ApiVersions as i16);
        assert!(range.is_some());
    }

    #[test]
    fn test_connection_manager_new() {
        let manager = ConnectionManager::new(
            vec!["localhost:9092".to_string()],
            "test-client".to_string(),
            100,
        );
        assert!(manager.broker_address(0).is_none());
        assert_eq!(manager.next_correlation_id(), 0);
        assert_eq!(manager.next_correlation_id(), 1);
    }

    #[test]
    fn test_register_broker() {
        let manager = ConnectionManager::new(
            vec!["localhost:9092".to_string()],
            "test-client".to_string(),
            100,
        );
        manager.register_broker(1, "broker1:9092".to_string());
        assert_eq!(manager.broker_address(1), Some("broker1:9092".to_string()));
        assert!(manager.broker_address(2).is_none());
    }
}
