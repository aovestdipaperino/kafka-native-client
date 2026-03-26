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
