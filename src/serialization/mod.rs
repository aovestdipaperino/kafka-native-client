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

    #[test]
    fn test_string_empty() {
        let original = String::new();
        let bytes = original.to_bytes("t").unwrap();
        assert!(bytes.is_empty());
        let decoded = String::from_bytes("t", &bytes).unwrap();
        assert_eq!(decoded, "");
    }

    #[test]
    fn test_string_unicode() {
        let original = "\u{1F600}\u{1F4A9} caf\u{00E9} \u{4E16}\u{754C}".to_string();
        let bytes = original.to_bytes("t").unwrap();
        let decoded = String::from_bytes("t", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_vec_u8_empty() {
        let original: Vec<u8> = vec![];
        let bytes = original.to_bytes("t").unwrap();
        assert!(bytes.is_empty());
        let decoded = Vec::<u8>::from_bytes("t", &bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_bytes_roundtrip() {
        let original = Bytes::from_static(b"kafka-native");
        let serialized = original.to_bytes("t").unwrap();
        let decoded = Bytes::from_bytes("t", &serialized).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_bytes_empty_roundtrip() {
        let original = Bytes::new();
        let serialized = original.to_bytes("t").unwrap();
        let decoded = Bytes::from_bytes("t", &serialized).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i32_min_value() {
        let original = i32::MIN;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = i32::from_bytes("t", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i32_max_value() {
        let original = i32::MAX;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = i32::from_bytes("t", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i64_min_value() {
        let original = i64::MIN;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = i64::from_bytes("t", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_i64_max_value() {
        let original = i64::MAX;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = i64::from_bytes("t", &bytes).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_f64_zero() {
        let original: f64 = 0.0;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = f64::from_bytes("t", &bytes).unwrap();
        assert!((decoded - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_f64_negative() {
        let original: f64 = -273.15;
        let bytes = original.to_bytes("t").unwrap();
        let decoded = f64::from_bytes("t", &bytes).unwrap();
        assert!((original - decoded).abs() < f64::EPSILON);
    }

    #[test]
    fn test_i32_from_bytes_too_short() {
        let result = i32::from_bytes("t", &[1]);
        assert!(result.is_err());
    }

    #[test]
    fn test_i32_from_bytes_too_long() {
        let result = i32::from_bytes("t", &[1, 2, 3, 4, 5]);
        assert!(result.is_err());
    }

    #[test]
    fn test_i64_from_bytes_empty() {
        let result = i64::from_bytes("t", &[]);
        assert!(result.is_err());
    }
}
