/// Re-export OffsetResetStrategy from config (canonical definition).
pub use crate::config::consumer_config::OffsetResetStrategy;

/// An offset and associated metadata for a committed consumer position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: Option<String>,
    pub leader_epoch: Option<i32>,
}

impl OffsetAndMetadata {
    pub fn new(offset: i64) -> Self {
        Self {
            offset,
            metadata: None,
            leader_epoch: None,
        }
    }
}

/// An offset and timestamp returned by offsets_for_times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetAndTimestamp {
    pub offset: i64,
    pub timestamp: i64,
    pub leader_epoch: Option<i32>,
}

/// Consumer group metadata.
#[derive(Debug, Clone)]
pub struct ConsumerGroupMetadata {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub group_instance_id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `OffsetAndMetadata::new` sets offset and leaves metadata/leader_epoch as None.
    #[test]
    fn test_offset_and_metadata_new_defaults() {
        let oam = OffsetAndMetadata::new(42);
        assert_eq!(oam.offset, 42);
        assert!(oam.metadata.is_none());
        assert!(oam.leader_epoch.is_none());
    }

    /// OffsetAndMetadata with an explicit metadata string.
    #[test]
    fn test_offset_and_metadata_with_metadata_string() {
        let oam = OffsetAndMetadata {
            offset: 100,
            metadata: Some("committed-by-consumer-1".into()),
            leader_epoch: Some(3),
        };
        assert_eq!(oam.offset, 100);
        assert_eq!(oam.metadata.as_deref(), Some("committed-by-consumer-1"));
        assert_eq!(oam.leader_epoch, Some(3));
    }

    /// OffsetAndMetadata equality: same values are equal; different values are not.
    #[test]
    fn test_offset_and_metadata_equality() {
        let a = OffsetAndMetadata::new(10);
        let b = OffsetAndMetadata::new(10);
        assert_eq!(a, b);

        let c = OffsetAndMetadata {
            offset: 10,
            metadata: Some("m".into()),
            leader_epoch: None,
        };
        let d = OffsetAndMetadata {
            offset: 10,
            metadata: Some("m".into()),
            leader_epoch: None,
        };
        assert_eq!(c, d);

        // Different offset
        assert_ne!(a, OffsetAndMetadata::new(20));

        // Different metadata
        let e = OffsetAndMetadata {
            offset: 10,
            metadata: Some("x".into()),
            leader_epoch: None,
        };
        assert_ne!(c, e);

        // Different leader_epoch
        let f = OffsetAndMetadata {
            offset: 10,
            metadata: None,
            leader_epoch: Some(1),
        };
        assert_ne!(a, f);
    }

    /// OffsetAndTimestamp stores offset and timestamp correctly.
    #[test]
    fn test_offset_and_timestamp() {
        let oat = OffsetAndTimestamp {
            offset: 500,
            timestamp: 1_700_000_000,
            leader_epoch: Some(5),
        };
        assert_eq!(oat.offset, 500);
        assert_eq!(oat.timestamp, 1_700_000_000);
        assert_eq!(oat.leader_epoch, Some(5));
    }

    /// OffsetAndTimestamp without leader epoch.
    #[test]
    fn test_offset_and_timestamp_no_epoch() {
        let oat = OffsetAndTimestamp {
            offset: 0,
            timestamp: 0,
            leader_epoch: None,
        };
        assert_eq!(oat.offset, 0);
        assert_eq!(oat.timestamp, 0);
        assert!(oat.leader_epoch.is_none());
    }

    /// OffsetAndTimestamp equality.
    #[test]
    fn test_offset_and_timestamp_equality() {
        let a = OffsetAndTimestamp {
            offset: 1,
            timestamp: 2,
            leader_epoch: None,
        };
        let b = OffsetAndTimestamp {
            offset: 1,
            timestamp: 2,
            leader_epoch: None,
        };
        assert_eq!(a, b);

        let c = OffsetAndTimestamp {
            offset: 1,
            timestamp: 999,
            leader_epoch: None,
        };
        assert_ne!(a, c);
    }

    /// ConsumerGroupMetadata stores all fields.
    #[test]
    fn test_consumer_group_metadata() {
        let meta = ConsumerGroupMetadata {
            group_id: "my-group".into(),
            generation_id: 7,
            member_id: "consumer-1-uuid".into(),
            group_instance_id: Some("instance-1".into()),
        };
        assert_eq!(meta.group_id, "my-group");
        assert_eq!(meta.generation_id, 7);
        assert_eq!(meta.member_id, "consumer-1-uuid");
        assert_eq!(meta.group_instance_id.as_deref(), Some("instance-1"));
    }

    /// ConsumerGroupMetadata with no static group instance id.
    #[test]
    fn test_consumer_group_metadata_no_instance_id() {
        let meta = ConsumerGroupMetadata {
            group_id: "g".into(),
            generation_id: -1,
            member_id: "".into(),
            group_instance_id: None,
        };
        assert_eq!(meta.group_id, "g");
        assert_eq!(meta.generation_id, -1);
        assert!(meta.member_id.is_empty());
        assert!(meta.group_instance_id.is_none());
    }

    /// ConsumerGroupMetadata clone produces independent copy.
    #[test]
    fn test_consumer_group_metadata_clone() {
        let original = ConsumerGroupMetadata {
            group_id: "grp".into(),
            generation_id: 3,
            member_id: "mem".into(),
            group_instance_id: Some("inst".into()),
        };
        let cloned = original.clone();
        assert_eq!(cloned.group_id, original.group_id);
        assert_eq!(cloned.generation_id, original.generation_id);
        assert_eq!(cloned.member_id, original.member_id);
        assert_eq!(cloned.group_instance_id, original.group_instance_id);
    }

    /// OffsetResetStrategy variants are distinct and can be compared.
    #[test]
    fn test_offset_reset_strategy_variants() {
        assert_ne!(OffsetResetStrategy::Latest, OffsetResetStrategy::Earliest);
        assert_ne!(OffsetResetStrategy::Latest, OffsetResetStrategy::None);
        assert_ne!(OffsetResetStrategy::Earliest, OffsetResetStrategy::None);

        let a = OffsetResetStrategy::Earliest;
        let b = OffsetResetStrategy::Earliest;
        assert_eq!(a, b);
    }

    /// OffsetResetStrategy is Copy.
    #[test]
    fn test_offset_reset_strategy_copy() {
        let strategy = OffsetResetStrategy::Latest;
        let copied = strategy; // Copy
        assert_eq!(strategy, copied);
    }

    /// OffsetResetStrategy::parse round-trips.
    #[test]
    fn test_offset_reset_strategy_parse() {
        assert_eq!(
            OffsetResetStrategy::parse("latest").unwrap(),
            OffsetResetStrategy::Latest
        );
        assert_eq!(
            OffsetResetStrategy::parse("earliest").unwrap(),
            OffsetResetStrategy::Earliest
        );
        assert_eq!(
            OffsetResetStrategy::parse("none").unwrap(),
            OffsetResetStrategy::None
        );
        assert!(OffsetResetStrategy::parse("invalid").is_err());
    }
}
