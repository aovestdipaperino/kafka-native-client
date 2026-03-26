use crate::metrics::meter::KafkaMetric;
use crate::metrics::sensor::Sensor;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Central storage for all metrics.
pub struct MetricsRegistry {
    sensors: RwLock<HashMap<String, Arc<Sensor>>>,
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            sensors: RwLock::new(HashMap::new()),
        }
    }

    pub fn sensor(&self, name: &str) -> Arc<Sensor> {
        let mut sensors = self.sensors.write().unwrap();
        sensors
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Sensor::new(name)))
            .clone()
    }

    pub fn metrics(&self) -> HashMap<String, Arc<KafkaMetric>> {
        let sensors = self.sensors.read().unwrap();
        let mut result = HashMap::new();
        for sensor in sensors.values() {
            for metric in sensor.metrics.lock().unwrap().iter() {
                result.insert(metric.key(), metric.clone());
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::stat::{Avg, Count, Max};

    #[test]
    fn test_sensor_record_and_measure() {
        let registry = MetricsRegistry::new();
        let sensor = registry.sensor("test-sensor");
        sensor.add("test-group", "test-metric", &[], Box::new(Avg::new()));
        sensor.record(10.0);
        sensor.record(20.0);

        let metrics = registry.metrics();
        let metric = metrics.get("test-group.test-metric").unwrap();
        assert!((metric.value() - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_count_stat() {
        let registry = MetricsRegistry::new();
        let sensor = registry.sensor("counter");
        sensor.add("grp", "count", &[], Box::new(Count::new()));
        sensor.record(1.0);
        sensor.record(1.0);
        sensor.record(1.0);

        let metrics = registry.metrics();
        let m = metrics.get("grp.count").unwrap();
        assert!((m.value() - 3.0).abs() < 0.001);
    }

    #[test]
    fn test_max_stat() {
        let registry = MetricsRegistry::new();
        let sensor = registry.sensor("max-sensor");
        sensor.add("grp", "max", &[], Box::new(Max::new()));
        sensor.record(5.0);
        sensor.record(15.0);
        sensor.record(10.0);

        let metrics = registry.metrics();
        let m = metrics.get("grp.max").unwrap();
        assert!((m.value() - 15.0).abs() < 0.001);
    }

    #[test]
    fn test_multiple_sensors() {
        let registry = MetricsRegistry::new();
        let s1 = registry.sensor("s1");
        let s2 = registry.sensor("s2");
        s1.add("g", "m1", &[], Box::new(Count::new()));
        s2.add("g", "m2", &[], Box::new(Count::new()));
        s1.record(1.0);
        s2.record(1.0);

        let metrics = registry.metrics();
        assert_eq!(metrics.len(), 2);
    }
}
