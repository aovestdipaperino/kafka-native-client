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
