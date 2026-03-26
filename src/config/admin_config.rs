use std::collections::HashMap;

use crate::{KafkaError, Result};
use super::common_config::CommonConfig;

/// Configuration for the Kafka admin client.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    pub bootstrap_servers: Vec<String>,
    pub client_id: String,
    pub request_timeout_ms: u64,
    pub metadata_max_age_ms: u64,
}

impl AdminConfig {
    /// Create a new builder.
    pub fn builder() -> AdminConfigBuilder {
        AdminConfigBuilder::default()
    }

    /// Return the common subset of configuration.
    pub fn common(&self) -> CommonConfig {
        CommonConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            client_id: self.client_id.clone(),
            request_timeout_ms: self.request_timeout_ms,
            metadata_max_age_ms: self.metadata_max_age_ms,
        }
    }
}

/// Builder for [`AdminConfig`].
#[derive(Default)]
pub struct AdminConfigBuilder {
    bootstrap_servers: Option<Vec<String>>,
    client_id: Option<String>,
    request_timeout_ms: Option<u64>,
    metadata_max_age_ms: Option<u64>,
}

impl AdminConfigBuilder {
    pub fn bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = Some(servers);
        self
    }

    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = Some(id.into());
        self
    }

    pub fn request_timeout_ms(mut self, ms: u64) -> Self {
        self.request_timeout_ms = Some(ms);
        self
    }

    pub fn metadata_max_age_ms(mut self, ms: u64) -> Self {
        self.metadata_max_age_ms = Some(ms);
        self
    }

    pub fn build(self) -> Result<AdminConfig> {
        let bootstrap_servers = self.bootstrap_servers.ok_or_else(|| {
            KafkaError::Config("bootstrap.servers is required".into())
        })?;

        if bootstrap_servers.is_empty() {
            return Err(KafkaError::Config("bootstrap.servers must not be empty".into()));
        }

        Ok(AdminConfig {
            bootstrap_servers,
            client_id: self.client_id.unwrap_or_default(),
            request_timeout_ms: self.request_timeout_ms.unwrap_or(30_000),
            metadata_max_age_ms: self.metadata_max_age_ms.unwrap_or(300_000),
        })
    }
}

impl TryFrom<HashMap<String, String>> for AdminConfig {
    type Error = KafkaError;

    fn try_from(props: HashMap<String, String>) -> Result<Self> {
        let mut builder = AdminConfigBuilder::default();

        for (key, value) in &props {
            match key.as_str() {
                "bootstrap.servers" => {
                    let servers: Vec<String> =
                        value.split(',').map(|s| s.trim().to_owned()).collect();
                    builder = builder.bootstrap_servers(servers);
                }
                "client.id" => {
                    builder = builder.client_id(value.clone());
                }
                "request.timeout.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid request.timeout.ms: {value}"))
                    })?;
                    builder = builder.request_timeout_ms(v);
                }
                "metadata.max.age.ms" => {
                    let v: u64 = value.parse().map_err(|_| {
                        KafkaError::Config(format!("invalid metadata.max.age.ms: {value}"))
                    })?;
                    builder = builder.metadata_max_age_ms(v);
                }
                _ => {} // ignore unknown keys
            }
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_admin_defaults() {
        let config = AdminConfig::builder()
            .bootstrap_servers(vec!["localhost:9092".into()])
            .build()
            .unwrap();
        assert_eq!(config.request_timeout_ms, 30_000);
        assert_eq!(config.metadata_max_age_ms, 300_000);
        assert_eq!(config.client_id, "");
    }

    #[test]
    fn test_admin_missing_bootstrap() {
        let result = AdminConfig::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_admin_from_hashmap() {
        let mut props = std::collections::HashMap::new();
        props.insert("bootstrap.servers".into(), "host1:9092".into());
        props.insert("request.timeout.ms".into(), "5000".into());
        let config = AdminConfig::try_from(props).unwrap();
        assert_eq!(config.request_timeout_ms, 5000);
        assert_eq!(config.bootstrap_servers, vec!["host1:9092"]);
    }
}
