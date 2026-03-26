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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::stat::{Avg, Count};

    #[test]
    fn test_sensor_name_is_preserved() {
        let sensor = Sensor::new("my-sensor");
        assert_eq!(sensor.name, "my-sensor");
    }

    #[test]
    fn test_sensor_name_from_string() {
        let name = String::from("dynamic-name");
        let sensor = Sensor::new(name);
        assert_eq!(sensor.name, "dynamic-name");
    }

    #[test]
    fn test_add_creates_metric() {
        let sensor = Sensor::new("s");
        let metric = sensor.add("group", "metric-name", &[], Box::new(Count::new()));
        assert_eq!(metric.group, "group");
        assert_eq!(metric.name, "metric-name");
        assert_eq!(metric.key(), "group.metric-name");
        // The sensor should now have 1 metric
        assert_eq!(sensor.metrics.lock().unwrap().len(), 1);
    }

    #[test]
    fn test_add_creates_metric_with_tags() {
        let sensor = Sensor::new("s");
        let metric = sensor.add(
            "g",
            "m",
            &[("env", "prod"), ("region", "us")],
            Box::new(Avg::new()),
        );
        assert_eq!(metric.tags.len(), 2);
        assert_eq!(metric.tags[0], ("env".to_string(), "prod".to_string()));
        assert_eq!(metric.tags[1], ("region".to_string(), "us".to_string()));
    }

    #[test]
    fn test_record_updates_all_metrics() {
        let sensor = Sensor::new("s");
        let avg_metric = sensor.add("g", "avg", &[], Box::new(Avg::new()));
        let count_metric = sensor.add("g", "count", &[], Box::new(Count::new()));

        sensor.record(10.0);
        sensor.record(20.0);

        // Avg of 10 and 20 = 15
        assert!((avg_metric.value() - 15.0).abs() < 0.001);
        // Count should be 2
        assert!((count_metric.value() - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_record_with_no_metrics_does_not_panic() {
        let sensor = Sensor::new("empty");
        sensor.record(42.0); // should not panic
    }
}
