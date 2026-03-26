use crate::error::{KafkaError, Result};
use bytes::Bytes;

/// Serialize a value into bytes for producing to Kafka.
pub trait ToBytes {
    fn to_bytes(&self, topic: &str) -> Result<Bytes>;
}

/// Deserialize bytes from Kafka into a typed value.
pub trait FromBytes: Sized {
    fn from_bytes(topic: &str, data: &[u8]) -> Result<Self>;
}

// --- Built-in implementations ---

impl ToBytes for String {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(Bytes::from(self.clone()))
    }
}

impl FromBytes for String {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        String::from_utf8(data.to_vec()).map_err(|e| KafkaError::Serialization(e.to_string()))
    }
}

impl ToBytes for Vec<u8> {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(Bytes::from(self.clone()))
    }
}

impl FromBytes for Vec<u8> {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        Ok(data.to_vec())
    }
}

impl ToBytes for Bytes {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(self.clone())
    }
}

impl FromBytes for Bytes {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        Ok(Bytes::copy_from_slice(data))
    }
}

impl ToBytes for i32 {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(Bytes::from(self.to_be_bytes().to_vec()))
    }
}

impl FromBytes for i32 {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        let arr: [u8; 4] = data
            .try_into()
            .map_err(|_| KafkaError::Serialization("expected 4 bytes for i32".into()))?;
        Ok(i32::from_be_bytes(arr))
    }
}

impl ToBytes for i64 {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(Bytes::from(self.to_be_bytes().to_vec()))
    }
}

impl FromBytes for i64 {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = data
            .try_into()
            .map_err(|_| KafkaError::Serialization("expected 8 bytes for i64".into()))?;
        Ok(i64::from_be_bytes(arr))
    }
}

impl ToBytes for f64 {
    fn to_bytes(&self, _topic: &str) -> Result<Bytes> {
        Ok(Bytes::from(self.to_be_bytes().to_vec()))
    }
}

impl FromBytes for f64 {
    fn from_bytes(_topic: &str, data: &[u8]) -> Result<Self> {
        let arr: [u8; 8] = data
            .try_into()
            .map_err(|_| KafkaError::Serialization("expected 8 bytes for f64".into()))?;
        Ok(f64::from_be_bytes(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_roundtrip() {
        let original = "hello world".to_string();
        let bytes = original.to_bytes("test").unwrap();
        let decoded = String::from_bytes("test", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_vec_u8_roundtrip() {
        let original: Vec<u8> = vec![1, 2, 3, 4];
        let bytes = original.to_bytes("test").unwrap();
        let decoded = Vec::<u8>::from_bytes("test", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i32_roundtrip() {
        let original: i32 = 42;
        let bytes = original.to_bytes("test").unwrap();
        let decoded = i32::from_bytes("test", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i64_roundtrip() {
        let original: i64 = 123_456_789;
        let bytes = original.to_bytes("test").unwrap();
        let decoded = i64::from_bytes("test", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_f64_roundtrip() {
        let original: f64 = 3.14159;
        let bytes = original.to_bytes("test").unwrap();
        let decoded = f64::from_bytes("test", &bytes).unwrap();
        assert!((original - decoded).abs() < f64::EPSILON);
    }

    #[test]
    fn test_i32_wrong_size() {
        let result = i32::from_bytes("test", &[1, 2]);
        assert!(result.is_err());
    }
}
