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

    #[test]
    fn test_node_equality_same_id_host_port() {
        let n1 = Node::new(1, "broker1.example.com", 9092);
        let n2 = Node::new(1, "broker1.example.com", 9092);
        assert_eq!(n1, n2);
    }

    #[test]
    fn test_node_not_equal_different_id() {
        let n1 = Node::new(1, "localhost", 9092);
        let n2 = Node::new(2, "localhost", 9092);
        assert_ne!(n1, n2);
    }

    #[test]
    fn test_node_not_equal_different_host() {
        let n1 = Node::new(1, "host-a", 9092);
        let n2 = Node::new(1, "host-b", 9092);
        assert_ne!(n1, n2);
    }

    #[test]
    fn test_node_not_equal_different_port() {
        let n1 = Node::new(1, "localhost", 9092);
        let n2 = Node::new(1, "localhost", 9093);
        assert_ne!(n1, n2);
    }

    #[test]
    fn test_node_address_format() {
        let node = Node::new(5, "kafka-broker.internal", 19092);
        assert_eq!(node.address(), "kafka-broker.internal:19092");
    }

    #[test]
    fn test_node_address_with_ip() {
        let node = Node::new(0, "192.168.1.100", 9092);
        assert_eq!(node.address(), "192.168.1.100:9092");
    }

    #[test]
    fn test_no_node_pattern() {
        // In Java, Node.noNode() uses id=-1, empty host, port -1.
        // In Rust, port is u16 so we use 0 for no-port. id=-1 signals "no node".
        let no_node = Node::new(-1, "", 0);
        assert_eq!(no_node.id, -1);
        assert_eq!(no_node.host, "");
        assert_eq!(no_node.port, 0);
    }

    #[test]
    fn test_node_with_rack() {
        let mut node = Node::new(0, "localhost", 9092);
        node.rack = Some("us-east-1a".to_string());
        assert_eq!(node.rack, Some("us-east-1a".to_string()));
    }

    #[test]
    fn test_node_default_rack_is_none() {
        let node = Node::new(0, "localhost", 9092);
        assert_eq!(node.rack, None);
    }

    #[test]
    fn test_node_clone_preserves_all_fields() {
        let mut node = Node::new(42, "broker42.example.com", 19092);
        node.rack = Some("rack-a".to_string());
        let cloned = node.clone();
        assert_eq!(node, cloned);
        assert_eq!(cloned.id, 42);
        assert_eq!(cloned.host, "broker42.example.com");
        assert_eq!(cloned.port, 19092);
        assert_eq!(cloned.rack, Some("rack-a".to_string()));
    }

    #[test]
    fn test_node_hash_consistency() {
        use std::collections::HashSet;
        let n1 = Node::new(1, "host", 9092);
        let n2 = Node::new(1, "host", 9092);
        let n3 = Node::new(2, "host", 9092);

        let mut set = HashSet::new();
        set.insert(n1);
        set.insert(n2); // duplicate, should not increase size
        set.insert(n3);
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_node_equality_ignores_rack() {
        // Rack is part of the struct and derived PartialEq, so it IS included
        // in equality. This test documents that behavior.
        let mut n1 = Node::new(1, "host", 9092);
        n1.rack = Some("rack-a".to_string());
        let mut n2 = Node::new(1, "host", 9092);
        n2.rack = Some("rack-b".to_string());
        // With derived PartialEq, different racks make nodes not equal
        assert_ne!(n1, n2);
    }
}
