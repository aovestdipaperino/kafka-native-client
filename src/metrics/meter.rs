use std::sync::Mutex;
use crate::metrics::stat::Stat;

/// A named metric backed by a Stat.
pub struct KafkaMetric {
    pub group: String,
    pub name: String,
    pub tags: Vec<(String, String)>,
    stat: Mutex<Box<dyn Stat>>,
}

impl KafkaMetric {
    pub fn new(group: String, name: String, tags: Vec<(String, String)>, stat: Box<dyn Stat>) -> Self {
        Self { group, name, tags, stat: Mutex::new(stat) }
    }

    pub fn record(&self, value: f64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.stat.lock().unwrap().record(value, now);
    }

    pub fn value(&self) -> f64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        self.stat.lock().unwrap().measure(now)
    }

    pub fn key(&self) -> String {
        format!("{}.{}", self.group, self.name)
    }
}

// Debug impl that doesn't require Stat: Debug
impl std::fmt::Debug for KafkaMetric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaMetric")
            .field("group", &self.group)
            .field("name", &self.name)
            .field("tags", &self.tags)
            .finish()
    }
}
