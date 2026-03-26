use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, RwLock};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::task::JoinHandle;

use proto_kafka::error::ParseResponseErrorCode;
use proto_kafka::messages::offset_commit_request::{
    OffsetCommitRequestPartition, OffsetCommitRequestTopic,
};
use proto_kafka::messages::offset_fetch_request::OffsetFetchRequestTopic;
use proto_kafka::messages::sync_group_request::SyncGroupRequestAssignment;
use proto_kafka::messages::{
    FindCoordinatorRequest, GroupId, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest,
    OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest, TopicName,
};
use proto_kafka::protocol::StrBytes;

use crate::common::TopicPartition;
use crate::consumer::offset::OffsetAndMetadata;
use crate::consumer::rebalance_listener::ConsumerRebalanceListener;
use crate::consumer::subscription_state::SubscriptionState;
use crate::error::{KafkaError, Result};
use crate::network::connection_manager::ConnectionManager;

/// State of the consumer group membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupState {
    Unjoined,
    Stable,
}

/// Manages consumer group membership: finding the coordinator, joining,
/// heartbeating, syncing, committing offsets, and leaving.
pub struct GroupCoordinator {
    connection_manager: Arc<ConnectionManager>,
    group_id: String,
    member_id: RwLock<String>,
    generation_id: AtomicI32,
    group_instance_id: Option<String>,
    coordinator_id: AtomicI32,
    heartbeat_interval_ms: u64,
    session_timeout_ms: u64,
    rebalance_timeout_ms: u64,
    rebalance_listener: Option<Box<dyn ConsumerRebalanceListener>>,
    state: RwLock<GroupState>,
    heartbeat_handle: RwLock<Option<JoinHandle<()>>>,
    needs_rejoin: AtomicBool,
}

impl GroupCoordinator {
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        group_id: String,
        session_timeout_ms: u64,
        heartbeat_interval_ms: u64,
        rebalance_timeout_ms: u64,
        group_instance_id: Option<String>,
        rebalance_listener: Option<Box<dyn ConsumerRebalanceListener>>,
    ) -> Self {
        Self {
            connection_manager,
            group_id,
            member_id: RwLock::new(String::new()),
            generation_id: AtomicI32::new(-1),
            group_instance_id,
            coordinator_id: AtomicI32::new(-1),
            heartbeat_interval_ms,
            session_timeout_ms,
            rebalance_timeout_ms,
            rebalance_listener,
            state: RwLock::new(GroupState::Unjoined),
            heartbeat_handle: RwLock::new(None),
            needs_rejoin: AtomicBool::new(true),
        }
    }

    /// Discover which broker is the group coordinator.
    pub async fn ensure_coordinator_known(&self) -> Result<()> {
        if self.coordinator_id.load(Ordering::Relaxed) >= 0 {
            return Ok(());
        }

        let request = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(self.group_id.clone()))
            .with_key_type(0); // 0 = GROUP

        let response = self.connection_manager.send(-1, request).await?;

        if response.error_code.is_err() {
            return Err(KafkaError::GroupCoordinator(format!(
                "FindCoordinator failed: {:?}",
                response.error_code.err()
            )));
        }

        let coordinator_id = response.node_id.0;
        let addr = format!("{}:{}", response.host.as_str(), response.port);
        self.connection_manager
            .register_broker(coordinator_id, addr.clone());
        self.connection_manager
            .ensure_connection(coordinator_id, &addr)
            .await?;
        self.coordinator_id.store(coordinator_id, Ordering::Relaxed);

        Ok(())
    }

    /// Join the consumer group and receive partition assignments.
    pub async fn join_group(
        &self,
        subscription: &SubscriptionState,
    ) -> Result<Vec<TopicPartition>> {
        self.ensure_coordinator_known().await?;
        let coordinator = self.coordinator_id.load(Ordering::Relaxed);

        let member_id = self.member_id.read().unwrap().clone();
        let topics: Vec<String> = subscription.subscribed_topics().into_iter().collect();

        // Build subscription metadata (Kafka consumer protocol)
        let metadata = encode_subscription_metadata(&topics);

        let protocol =
            proto_kafka::messages::join_group_request::JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(metadata);

        let request = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_session_timeout_ms(self.session_timeout_ms as i32)
            .with_rebalance_timeout_ms(self.rebalance_timeout_ms as i32)
            .with_member_id(StrBytes::from_string(member_id))
            .with_group_instance_id(
                self.group_instance_id
                    .as_ref()
                    .map(|id| StrBytes::from_string(id.clone())),
            )
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_protocols(vec![protocol]);

        let join_response = self.connection_manager.send(coordinator, request).await?;

        if join_response.error_code.is_err() {
            return Err(KafkaError::GroupCoordinator(format!(
                "JoinGroup failed: {:?}",
                join_response.error_code.err()
            )));
        }

        *self.member_id.write().unwrap() = join_response.member_id.to_string();
        self.generation_id
            .store(join_response.generation_id, Ordering::Relaxed);

        let is_leader = join_response.leader.as_str() == join_response.member_id.as_str();

        // SyncGroup — leader performs assignment, followers get theirs
        let assignments = if is_leader {
            self.perform_assignment(&join_response.members, &topics, subscription)
        } else {
            Vec::new()
        };

        let current_member_id = self.member_id.read().unwrap().clone();
        let sync_request = SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_generation_id(join_response.generation_id)
            .with_member_id(StrBytes::from_string(current_member_id))
            .with_assignments(assignments);

        let sync_response = self
            .connection_manager
            .send(coordinator, sync_request)
            .await?;

        if sync_response.error_code.is_err() {
            return Err(KafkaError::GroupCoordinator(format!(
                "SyncGroup failed: {:?}",
                sync_response.error_code.err()
            )));
        }

        let assigned = decode_assignment(&sync_response.assignment);

        *self.state.write().unwrap() = GroupState::Stable;
        self.needs_rejoin.store(false, Ordering::Relaxed);

        // Start heartbeat
        self.start_heartbeat();

        Ok(assigned)
    }

    /// Perform range assignment as the group leader.
    fn perform_assignment(
        &self,
        members: &[proto_kafka::messages::join_group_response::JoinGroupResponseMember],
        topics: &[String],
        subscription: &SubscriptionState,
    ) -> Vec<SyncGroupRequestAssignment> {
        let cluster = crate::common::Cluster::empty(); // Simplified — real impl uses metadata
        let _ = subscription; // Would use for more complex assignment

        let member_ids: Vec<String> = members.iter().map(|m| m.member_id.to_string()).collect();
        let num_members = member_ids.len();

        let mut member_assignments: HashMap<String, Vec<TopicPartition>> = HashMap::new();
        for id in &member_ids {
            member_assignments.insert(id.clone(), Vec::new());
        }

        // Simple range assignment: divide partitions evenly per topic
        // NOTE: In a real implementation, we'd get partition count from metadata
        let _ = cluster;
        for topic in topics {
            // Default to 1 partition if metadata unavailable
            let num_partitions = 1;
            for p in 0..num_partitions {
                let member_idx = p as usize % num_members;
                if let Some(assigned) = member_assignments.get_mut(&member_ids[member_idx]) {
                    assigned.push(TopicPartition::new(topic.clone(), p));
                }
            }
        }

        member_assignments
            .into_iter()
            .map(|(member_id, partitions)| {
                SyncGroupRequestAssignment::default()
                    .with_member_id(StrBytes::from_string(member_id))
                    .with_assignment(encode_assignment(&partitions))
            })
            .collect()
    }

    /// Send heartbeat to the coordinator.
    pub async fn heartbeat(&self) -> Result<()> {
        let coordinator = self.coordinator_id.load(Ordering::Relaxed);
        let member_id = self.member_id.read().unwrap().clone();
        let generation = self.generation_id.load(Ordering::Relaxed);

        let request = HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_generation_id(generation)
            .with_member_id(StrBytes::from_string(member_id));

        let response = self.connection_manager.send(coordinator, request).await?;

        if response.error_code.is_err() {
            let err = response.error_code.err().unwrap();
            if err == proto_kafka::ResponseError::RebalanceInProgress {
                self.needs_rejoin.store(true, Ordering::Relaxed);
            }
            return Err(KafkaError::Response(err));
        }

        Ok(())
    }

    /// Commit offsets synchronously.
    pub async fn commit_offsets_sync(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        let coordinator = self.coordinator_id.load(Ordering::Relaxed);
        let member_id = self.member_id.read().unwrap().clone();
        let generation = self.generation_id.load(Ordering::Relaxed);

        // Group offsets by topic
        let mut by_topic: HashMap<String, Vec<OffsetCommitRequestPartition>> = HashMap::new();
        for (tp, oam) in &offsets {
            by_topic.entry(tp.topic.clone()).or_default().push(
                OffsetCommitRequestPartition::default()
                    .with_partition_index(tp.partition)
                    .with_committed_offset(oam.offset)
                    .with_committed_metadata(
                        oam.metadata
                            .as_deref()
                            .map(|m| StrBytes::from_string(m.to_string())),
                    ),
            );
        }

        let topics: Vec<OffsetCommitRequestTopic> = by_topic
            .into_iter()
            .map(|(topic, parts)| {
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic)))
                    .with_partitions(parts)
            })
            .collect();

        let request = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_generation_id_or_member_epoch(generation)
            .with_member_id(StrBytes::from_string(member_id))
            .with_topics(topics);

        let response = self.connection_manager.send(coordinator, request).await?;

        for topic in &response.topics {
            for partition in &topic.partitions {
                if partition.error_code.is_err() {
                    return Err(KafkaError::GroupCoordinator(format!(
                        "OffsetCommit failed for {}-{}: {:?}",
                        topic.name.0.as_str(),
                        partition.partition_index,
                        partition.error_code.err()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Fetch committed offsets for partitions.
    pub async fn fetch_committed_offsets(
        &self,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>> {
        let coordinator = self.coordinator_id.load(Ordering::Relaxed);

        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for tp in partitions {
            by_topic
                .entry(tp.topic.clone())
                .or_default()
                .push(tp.partition);
        }

        let topics: Vec<OffsetFetchRequestTopic> = by_topic
            .into_iter()
            .map(|(topic, partitions)| {
                OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic)))
                    .with_partition_indexes(partitions)
            })
            .collect();

        let request = OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_topics(Some(topics));

        let response = self.connection_manager.send(coordinator, request).await?;

        let mut result = HashMap::new();
        for topic in &response.topics {
            let topic_name = topic.name.0.to_string();
            for partition in &topic.partitions {
                if partition.committed_offset >= 0 {
                    let tp = TopicPartition::new(topic_name.clone(), partition.partition_index);
                    result.insert(
                        tp,
                        OffsetAndMetadata {
                            offset: partition.committed_offset,
                            metadata: if partition.metadata.is_none()
                                || partition
                                    .metadata
                                    .as_ref()
                                    .is_some_and(|m| m.as_str().is_empty())
                            {
                                None
                            } else {
                                partition.metadata.as_ref().map(|m| m.to_string())
                            },
                            leader_epoch: None,
                        },
                    );
                }
            }
        }

        Ok(result)
    }

    /// Leave the consumer group.
    pub async fn leave_group(&self) -> Result<()> {
        // Stop heartbeat
        if let Some(handle) = self.heartbeat_handle.write().unwrap().take() {
            handle.abort();
        }

        let coordinator = self.coordinator_id.load(Ordering::Relaxed);
        if coordinator < 0 {
            return Ok(());
        }

        let member_id = self.member_id.read().unwrap().clone();
        let request = LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(self.group_id.clone())))
            .with_member_id(StrBytes::from_string(member_id));

        let _ = self.connection_manager.send(coordinator, request).await;

        *self.state.write().unwrap() = GroupState::Unjoined;
        self.needs_rejoin.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub fn needs_rejoin(&self) -> bool {
        self.needs_rejoin.load(Ordering::Relaxed)
    }

    pub fn generation_id(&self) -> i32 {
        self.generation_id.load(Ordering::Relaxed)
    }

    pub fn member_id(&self) -> String {
        self.member_id.read().unwrap().clone()
    }

    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    fn start_heartbeat(&self) {
        // Abort previous heartbeat if any
        if let Some(handle) = self.heartbeat_handle.write().unwrap().take() {
            handle.abort();
        }

        let cm = self.connection_manager.clone();
        let group_id = self.group_id.clone();
        let coordinator_id = self.coordinator_id.load(Ordering::Relaxed);
        let member_id = self.member_id.read().unwrap().clone();
        let generation = self.generation_id.load(Ordering::Relaxed);
        let interval = self.heartbeat_interval_ms;

        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(interval)).await;

                let request = HeartbeatRequest::default()
                    .with_group_id(GroupId(StrBytes::from_string(group_id.clone())))
                    .with_generation_id(generation)
                    .with_member_id(StrBytes::from_string(member_id.clone()));

                if let Err(e) = cm.send::<HeartbeatRequest>(coordinator_id, request).await {
                    tracing::warn!("Heartbeat failed: {e}");
                    break;
                }
            }
        });

        *self.heartbeat_handle.write().unwrap() = Some(handle);
    }
}

/// Encode a list of subscribed topics into Kafka consumer protocol metadata bytes.
fn encode_subscription_metadata(topics: &[String]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(0); // version
    buf.put_i32(topics.len() as i32);
    for topic in topics {
        let topic_bytes = topic.as_bytes();
        buf.put_i16(topic_bytes.len() as i16);
        buf.put_slice(topic_bytes);
    }
    buf.put_i32(0); // empty user data length (as -1 or 0 bytes)
    buf.freeze()
}

/// Encode partition assignments into Kafka consumer protocol assignment bytes.
fn encode_assignment(partitions: &[TopicPartition]) -> Bytes {
    let mut by_topic: HashMap<&str, Vec<i32>> = HashMap::new();
    for tp in partitions {
        by_topic.entry(&tp.topic).or_default().push(tp.partition);
    }

    let mut buf = BytesMut::new();
    buf.put_i16(0); // version
    buf.put_i32(by_topic.len() as i32);
    for (topic, parts) in &by_topic {
        let topic_bytes = topic.as_bytes();
        buf.put_i16(topic_bytes.len() as i16);
        buf.put_slice(topic_bytes);
        buf.put_i32(parts.len() as i32);
        for &p in parts {
            buf.put_i32(p);
        }
    }
    buf.put_i32(0); // empty user data
    buf.freeze()
}

/// Decode Kafka consumer protocol assignment bytes into TopicPartitions.
fn decode_assignment(data: &Bytes) -> Vec<TopicPartition> {
    if data.len() < 6 {
        return Vec::new();
    }

    let mut offset = 0;
    let _version = i16::from_be_bytes([data[offset], data[offset + 1]]);
    offset += 2;

    let num_topics = i32::from_be_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]) as usize;
    offset += 4;

    let mut result = Vec::new();
    for _ in 0..num_topics {
        if offset + 2 > data.len() {
            break;
        }
        let topic_len = i16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + topic_len > data.len() {
            break;
        }
        let topic = String::from_utf8_lossy(&data[offset..offset + topic_len]).to_string();
        offset += topic_len;

        if offset + 4 > data.len() {
            break;
        }
        let num_parts = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4;

        for _ in 0..num_parts {
            if offset + 4 > data.len() {
                break;
            }
            let partition = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;
            result.push(TopicPartition::new(topic.clone(), partition));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_assignment_roundtrip() {
        let partitions = vec![
            TopicPartition::new("topic-a", 0),
            TopicPartition::new("topic-a", 1),
            TopicPartition::new("topic-b", 0),
        ];

        let encoded = encode_assignment(&partitions);
        let decoded = decode_assignment(&encoded);

        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(&TopicPartition::new("topic-a", 0)));
        assert!(decoded.contains(&TopicPartition::new("topic-a", 1)));
        assert!(decoded.contains(&TopicPartition::new("topic-b", 0)));
    }

    #[test]
    fn test_decode_empty_assignment() {
        let data = Bytes::new();
        let decoded = decode_assignment(&data);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_encode_subscription_metadata() {
        let topics = vec!["topic1".to_string(), "topic2".to_string()];
        let encoded = encode_subscription_metadata(&topics);
        assert!(!encoded.is_empty());
        // Version (2 bytes) + count (4 bytes) + topics + user data
        assert!(encoded.len() > 6);
    }

    #[test]
    fn test_group_coordinator_initial_state() {
        let cm = Arc::new(ConnectionManager::new(vec![], String::new(), 5));
        let gc = GroupCoordinator::new(cm, "test-group".into(), 10000, 3000, 300000, None, None);

        assert!(gc.needs_rejoin());
        assert_eq!(gc.generation_id(), -1);
        assert_eq!(gc.member_id(), "");
        assert_eq!(gc.group_id(), "test-group");
    }

    /// Encode assignment with a single partition and decode it back.
    #[test]
    fn test_encode_assignment_single_partition() {
        let partitions = vec![TopicPartition::new("my-topic", 0)];
        let encoded = encode_assignment(&partitions);
        let decoded = decode_assignment(&encoded);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], TopicPartition::new("my-topic", 0));
    }

    /// Encode assignment with an empty partition list.
    #[test]
    fn test_encode_assignment_empty_list() {
        let partitions: Vec<TopicPartition> = vec![];
        let encoded = encode_assignment(&partitions);
        let decoded = decode_assignment(&encoded);
        assert!(decoded.is_empty());

        // The encoded format should still be valid: version(2) + count(4)=0 + userdata(4)
        assert_eq!(encoded.len(), 2 + 4 + 4);
    }

    /// Decode assignment with truncated data returns empty (robustness).
    #[test]
    fn test_decode_assignment_truncated_data() {
        // Less than 6 bytes: should return empty
        let data = Bytes::from_static(&[0, 0, 0]);
        assert!(decode_assignment(&data).is_empty());

        // Exactly 5 bytes: still too short
        let data = Bytes::from_static(&[0, 0, 0, 0, 1]);
        assert!(decode_assignment(&data).is_empty());
    }

    /// Decode assignment with header claiming topics but truncated mid-topic.
    #[test]
    fn test_decode_assignment_truncated_mid_topic() {
        // version=0, num_topics=1, then truncated before topic length can be read
        let mut buf = BytesMut::new();
        buf.put_i16(0); // version
        buf.put_i32(1); // 1 topic
        // No topic data follows — truncated
        let data = buf.freeze();
        let decoded = decode_assignment(&data);
        assert!(decoded.is_empty());
    }

    /// Decode assignment with header claiming topics but topic length exceeds data.
    #[test]
    fn test_decode_assignment_truncated_topic_name() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // version
        buf.put_i32(1); // 1 topic
        buf.put_i16(100); // topic name length = 100 but no data follows
        let data = buf.freeze();
        let decoded = decode_assignment(&data);
        assert!(decoded.is_empty());
    }

    /// Decode assignment truncated mid-partition-count.
    #[test]
    fn test_decode_assignment_truncated_partition_count() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // version
        buf.put_i32(1); // 1 topic
        buf.put_i16(1); // topic name length = 1
        buf.put_u8(b't'); // topic name "t"
        // Missing the 4-byte partition count
        let data = buf.freeze();
        let decoded = decode_assignment(&data);
        assert!(decoded.is_empty());
    }

    /// Decode assignment truncated mid-partition-data.
    #[test]
    fn test_decode_assignment_truncated_partition_data() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // version
        buf.put_i32(1); // 1 topic
        buf.put_i16(1); // topic name length = 1
        buf.put_u8(b't'); // topic name "t"
        buf.put_i32(2); // claims 2 partitions
        buf.put_i32(0); // partition 0
        // Missing partition 1 data — truncated
        let data = buf.freeze();
        let decoded = decode_assignment(&data);
        // Should get the one partition it could fully read
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], TopicPartition::new("t", 0));
    }

    /// Roundtrip with many partitions across 3 topics.
    #[test]
    fn test_roundtrip_many_partitions_three_topics() {
        let mut partitions = Vec::new();
        for p in 0..5 {
            partitions.push(TopicPartition::new("alpha", p));
        }
        for p in 0..3 {
            partitions.push(TopicPartition::new("beta", p));
        }
        for p in 0..4 {
            partitions.push(TopicPartition::new("gamma", p));
        }
        assert_eq!(partitions.len(), 12);

        let encoded = encode_assignment(&partitions);
        let decoded = decode_assignment(&encoded);

        assert_eq!(decoded.len(), 12);

        // Verify all original partitions are present in decoded
        for tp in &partitions {
            assert!(
                decoded.contains(tp),
                "Missing partition after roundtrip: {tp}"
            );
        }
    }

    /// Roundtrip preserves topic names with special characters.
    #[test]
    fn test_roundtrip_special_topic_names() {
        let partitions = vec![
            TopicPartition::new("topic.with.dots", 0),
            TopicPartition::new("topic-with-dashes", 1),
            TopicPartition::new("topic_with_underscores", 2),
        ];

        let encoded = encode_assignment(&partitions);
        let decoded = decode_assignment(&encoded);

        assert_eq!(decoded.len(), 3);
        assert!(decoded.contains(&TopicPartition::new("topic.with.dots", 0)));
        assert!(decoded.contains(&TopicPartition::new("topic-with-dashes", 1)));
        assert!(decoded.contains(&TopicPartition::new("topic_with_underscores", 2)));
    }

    /// Subscription metadata encoding produces a non-empty byte buffer.
    #[test]
    fn test_encode_subscription_metadata_structure() {
        let topics = vec!["a".to_string()];
        let encoded = encode_subscription_metadata(&topics);
        // version(2) + count(4) + topic_len(2) + "a"(1) + user_data(4) = 13
        assert_eq!(encoded.len(), 13);

        // Check version = 0
        assert_eq!(encoded[0], 0);
        assert_eq!(encoded[1], 0);
        // Check topic count = 1
        assert_eq!(
            i32::from_be_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]),
            1
        );
    }

    /// Subscription metadata with multiple topics.
    #[test]
    fn test_encode_subscription_metadata_multiple_topics() {
        let topics = vec!["topicA".to_string(), "topicB".to_string()];
        let encoded = encode_subscription_metadata(&topics);
        // version(2) + count(4) + 2*(len(2)+name) + userdata(4)
        // = 2 + 4 + (2+6) + (2+6) + 4 = 26
        assert_eq!(encoded.len(), 26);

        // Check topic count = 2
        assert_eq!(
            i32::from_be_bytes([encoded[2], encoded[3], encoded[4], encoded[5]]),
            2
        );
    }
}
