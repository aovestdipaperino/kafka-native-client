/// Acknowledgement level for produce requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    One,
    All,
}

impl Acks {
    pub fn to_i16(self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::One => 1,
            Acks::All => -1,
        }
    }

    pub fn parse(s: &str) -> Result<Self, String> {
        match s {
            "0" => Ok(Acks::None),
            "1" => Ok(Acks::One),
            "all" | "-1" => Ok(Acks::All),
            other => Err(format!("invalid acks value: {other}")),
        }
    }
}

/// Configuration shared by all client types.
#[derive(Debug, Clone)]
pub struct CommonConfig {
    pub bootstrap_servers: Vec<String>,
    pub client_id: String,
    pub request_timeout_ms: u64,
    pub metadata_max_age_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acks_to_i16_none() {
        assert_eq!(Acks::None.to_i16(), 0);
    }

    #[test]
    fn test_acks_to_i16_one() {
        assert_eq!(Acks::One.to_i16(), 1);
    }

    #[test]
    fn test_acks_to_i16_all() {
        assert_eq!(Acks::All.to_i16(), -1);
    }

    #[test]
    fn test_acks_parse_zero() {
        assert_eq!(Acks::parse("0").unwrap(), Acks::None);
    }

    #[test]
    fn test_acks_parse_one() {
        assert_eq!(Acks::parse("1").unwrap(), Acks::One);
    }

    #[test]
    fn test_acks_parse_all_string() {
        assert_eq!(Acks::parse("all").unwrap(), Acks::All);
    }

    #[test]
    fn test_acks_parse_minus_one() {
        assert_eq!(Acks::parse("-1").unwrap(), Acks::All);
    }

    #[test]
    fn test_acks_parse_invalid() {
        let result = Acks::parse("2");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("invalid acks value"));
    }

    #[test]
    fn test_acks_parse_invalid_text() {
        let result = Acks::parse("some");
        assert!(result.is_err());
    }
}
