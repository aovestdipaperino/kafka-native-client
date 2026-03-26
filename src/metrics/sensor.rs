use crate::metrics::meter::KafkaMetric;
use crate::metrics::stat::Stat;
use std::sync::Arc;

/// A named measurement point that updates one or more metrics.
pub struct Sensor {
    pub name: String,
    pub(crate) metrics: std::sync::Mutex<Vec<Arc<KafkaMetric>>>,
}

impl Sensor {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            metrics: std::sync::Mutex::new(vec![]),
        }
    }

    pub fn add(
        &self,
        group: &str,
        name: &str,
        tags: &[(&str, &str)],
        stat: Box<dyn Stat>,
    ) -> Arc<KafkaMetric> {
        let tags = tags
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let metric = Arc::new(KafkaMetric::new(
            group.to_string(),
            name.to_string(),
            tags,
            stat,
        ));
        self.metrics.lock().unwrap().push(metric.clone());
        metric
    }

    pub fn record(&self, value: f64) {
        for metric in self.metrics.lock().unwrap().iter() {
            metric.record(value);
        }
    }
}
