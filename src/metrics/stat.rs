/// A statistic that can be recorded and measured.
pub trait Stat: Send + Sync {
    fn record(&mut self, value: f64, time_ms: i64);
    fn measure(&self, time_ms: i64) -> f64;
}

pub struct Avg {
    sum: f64,
    count: u64,
}

impl Default for Avg {
    fn default() -> Self {
        Self::new()
    }
}

impl Avg {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Stat for Avg {
    fn record(&mut self, value: f64, _time_ms: i64) {
        self.sum += value;
        self.count += 1;
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

pub struct Count {
    count: f64,
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    pub fn new() -> Self {
        Self { count: 0.0 }
    }
}

impl Stat for Count {
    fn record(&mut self, _value: f64, _time_ms: i64) {
        self.count += 1.0;
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        self.count
    }
}

pub struct Max {
    max: f64,
}

impl Default for Max {
    fn default() -> Self {
        Self::new()
    }
}

impl Max {
    pub fn new() -> Self {
        Self {
            max: f64::NEG_INFINITY,
        }
    }
}

impl Stat for Max {
    fn record(&mut self, value: f64, _time_ms: i64) {
        if value > self.max {
            self.max = value;
        }
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        self.max
    }
}

pub struct Min {
    min: f64,
}

impl Default for Min {
    fn default() -> Self {
        Self::new()
    }
}

impl Min {
    pub fn new() -> Self {
        Self { min: f64::INFINITY }
    }
}

impl Stat for Min {
    fn record(&mut self, value: f64, _time_ms: i64) {
        if value < self.min {
            self.min = value;
        }
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        self.min
    }
}

pub struct Rate {
    count: f64,
    start_ms: i64,
    last_ms: i64,
}

impl Default for Rate {
    fn default() -> Self {
        Self::new()
    }
}

impl Rate {
    pub fn new() -> Self {
        Self {
            count: 0.0,
            start_ms: 0,
            last_ms: 0,
        }
    }
}

impl Stat for Rate {
    fn record(&mut self, value: f64, time_ms: i64) {
        if self.start_ms == 0 {
            self.start_ms = time_ms;
        }
        self.last_ms = time_ms;
        self.count += value;
    }
    fn measure(&self, _time_ms: i64) -> f64 {
        let elapsed = (self.last_ms - self.start_ms) as f64 / 1000.0;
        if elapsed <= 0.0 {
            0.0
        } else {
            self.count / elapsed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_avg_single_value() {
        let mut avg = Avg::new();
        avg.record(42.0, 0);
        assert!((avg.measure(0) - 42.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_no_values_returns_zero() {
        let avg = Avg::new();
        assert!((avg.measure(0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_avg_multiple_values() {
        let mut avg = Avg::new();
        avg.record(10.0, 0);
        avg.record(20.0, 0);
        avg.record(30.0, 0);
        assert!((avg.measure(0) - 20.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_count_increments() {
        let mut count = Count::new();
        assert!((count.measure(0) - 0.0).abs() < f64::EPSILON);
        count.record(999.0, 0); // value is ignored
        assert!((count.measure(0) - 1.0).abs() < f64::EPSILON);
        count.record(0.0, 0);
        assert!((count.measure(0) - 2.0).abs() < f64::EPSILON);
        count.record(1.0, 0);
        assert!((count.measure(0) - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_with_decreasing_values_keeps_max() {
        let mut max = Max::new();
        max.record(100.0, 0);
        max.record(50.0, 0);
        max.record(25.0, 0);
        assert!((max.measure(0) - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_max_initial_is_neg_infinity() {
        let max = Max::new();
        assert!(max.measure(0).is_infinite());
        assert!(max.measure(0) < 0.0);
    }

    #[test]
    fn test_min_with_increasing_values_keeps_min() {
        let mut min = Min::new();
        min.record(10.0, 0);
        min.record(20.0, 0);
        min.record(30.0, 0);
        assert!((min.measure(0) - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_min_initial_is_pos_infinity() {
        let min = Min::new();
        assert!(min.measure(0).is_infinite());
        assert!(min.measure(0) > 0.0);
    }

    #[test]
    fn test_rate_with_time_progression() {
        let mut rate = Rate::new();
        // Record 10 events at t=1000ms
        rate.record(10.0, 1000);
        // Record 10 more events at t=3000ms (2 seconds later)
        rate.record(10.0, 3000);
        // Total: 20 events over 2 seconds = 10 per second
        let measured = rate.measure(0);
        assert!((measured - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rate_zero_elapsed_returns_zero() {
        let mut rate = Rate::new();
        rate.record(5.0, 1000);
        // Only one measurement, so elapsed = 0
        let measured = rate.measure(0);
        assert!((measured - 0.0).abs() < f64::EPSILON);
    }
}
