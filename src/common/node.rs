/// A Kafka broker node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Node {
    pub id: i32,
    pub host: String,
    pub port: u16,
    pub rack: Option<String>,
}

impl Node {
    pub fn new(id: i32, host: impl Into<String>, port: u16) -> Self {
        Self {
            id,
            host: host.into(),
            port,
            rack: None,
        }
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_address() {
        let node = Node::new(0, "localhost", 9092);
        assert_eq!(node.address(), "localhost:9092");
    }
}
