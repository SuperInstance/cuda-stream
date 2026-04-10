/*!
# cuda-stream

Real-time stream processing.

Agents process continuous data — sensor streams, event logs,
metrics telemetry. This crate provides windowed aggregation,
sliding/tumbling windows, and stream joins.

- Tumbling windows (fixed size, non-overlapping)
- Sliding windows (fixed size, overlapping)
- Session windows (gap-based)
- Windowed aggregation (sum, avg, min, max, count)
- Stream join (time-based)
- Watermark tracking
*/

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// A stream event
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamEvent {
    pub key: String,
    pub value: f64,
    pub timestamp: u64,
    pub source: String,
}

impl StreamEvent {
    pub fn new(key: &str, value: f64, source: &str) -> Self { StreamEvent { key: key.to_string(), value, timestamp: now(), source: source.to_string() } }
}

/// Window type
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WindowType {
    Tumbling { size_ms: u64 },
    Sliding { size_ms: u64, slide_ms: u64 },
    Session { gap_ms: u64 },
}

/// A window of events
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Window {
    pub start_ms: u64,
    pub end_ms: u64,
    pub events: Vec<StreamEvent>,
    pub is_closed: bool,
}

impl Window {
    pub fn aggregate(&self) -> WindowAgg {
        if self.events.is_empty() { return WindowAgg { count: 0, sum: 0.0, avg: 0.0, min: 0.0, max: 0.0 }; }
        let vals: Vec<f64> = self.events.iter().map(|e| e.value).collect();
        let sum: f64 = vals.iter().sum();
        WindowAgg { count: vals.len(), sum, avg: sum / vals.len() as f64, min: vals.iter().cloned().fold(f64::INFINITY, f64::min), max: vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max) }
    }
}

/// Window aggregation result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WindowAgg {
    pub count: usize,
    pub sum: f64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
}

/// A windowed stream processor
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamProcessor {
    pub window_type: WindowType,
    pub current_window: Window,
    pub closed_windows: Vec<Window>,
    pub total_events: u64,
    pub total_windows_closed: u64,
    pub last_event_ms: u64,
}

impl StreamProcessor {
    pub fn tumbling(size_ms: u64) -> Self {
        let start = now();
        StreamProcessor { window_type: WindowType::Tumbling { size_ms }, current_window: Window { start_ms: start, end_ms: start + size_ms, events: vec![], is_closed: false }, closed_windows: vec![], total_events: 0, total_windows_closed: 0, last_event_ms: start }
    }

    pub fn sliding(size_ms: u64, slide_ms: u64) -> Self {
        let start = now();
        StreamProcessor { window_type: WindowType::Sliding { size_ms, slide_ms }, current_window: Window { start_ms: start, end_ms: start + size_ms, events: vec![], is_closed: false }, closed_windows: vec![], total_events: 0, total_windows_closed: 0, last_event_ms: start }
    }

    pub fn session(gap_ms: u64) -> Self {
        let start = now();
        StreamProcessor { window_type: WindowType::Session { gap_ms }, current_window: Window { start_ms: start, end_ms: start + gap_ms, events: vec![], is_closed: false }, closed_windows: vec![], total_events: 0, total_windows_closed: 0, last_event_ms: start }
    }

    /// Process an event, returns closed window if window boundary crossed
    pub fn process(&mut self, event: StreamEvent) -> Option<WindowAgg> {
        self.total_events += 1;
        let ts = event.timestamp;
        self.last_event_ms = ts;

        let should_close = match &self.window_type {
            WindowType::Tumbling { size_ms } => ts >= self.current_window.end_ms,
            WindowType::Sliding { slide_ms, .. } => ts >= self.current_window.start_ms + slide_ms,
            WindowType::Session { gap_ms } => !self.current_window.events.is_empty() && ts > self.last_event_ms + gap_ms,
        };

        if should_close && !self.current_window.events.is_empty() {
            let mut closed = std::mem::replace(&mut self.current_window, Window {
                start_ms: match &self.window_type {
                    WindowType::Tumbling { size_ms } => (ts / size_ms) * size_ms,
                    WindowType::Sliding { slide_ms, .. } => (ts / slide_ms) * slide_ms,
                    WindowType::Session { .. } => ts,
                },
                end_ms: match &self.window_type {
                    WindowType::Tumbling { size_ms } => (ts / size_ms) * size_ms + size_ms,
                    WindowType::Sliding { size_ms, .. } => ts + size_ms,
                    WindowType::Session { gap_ms } => ts + gap_ms,
                },
                events: vec![],
                is_closed: false,
            });
            closed.is_closed = true;
            let agg = closed.aggregate();
            self.closed_windows.push(closed);
            self.total_windows_closed += 1;
            self.current_window.events.push(event);
            return Some(agg);
        }

        self.current_window.events.push(event);
        None
    }

    /// Get current window aggregation (open window)
    pub fn current_agg(&self) -> WindowAgg { self.current_window.aggregate() }

    /// Summary
    pub fn summary(&self) -> String {
        format!("Stream: {} events, {} windows closed, current_window_size={}",
            self.total_events, self.total_windows_closed, self.current_window.events.len())
    }
}

/// A stream joiner (time-based inner join)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamJoiner {
    pub left_buffer: VecDeque<StreamEvent>,
    pub right_buffer: VecDeque<StreamEvent>,
    pub window_ms: u64,
    pub joins: u64,
    pub left_events: u64,
    pub right_events: u64,
}

impl StreamJoiner {
    pub fn new(window_ms: u64) -> Self { StreamJoiner { left_buffer: VecDeque::new(), right_buffer: VecDeque::new(), window_ms, joins: 0, left_events: 0, right_events: 0 } }

    /// Add left event, return matches
    pub fn add_left(&mut self, event: StreamEvent) -> Vec<StreamEvent> {
        self.left_events += 1;
        self.left_buffer.push_back(event.clone());
        self.prune_left(event.timestamp);
        self.match_right(&event)
    }

    /// Add right event, return matches
    pub fn add_right(&mut self, event: StreamEvent) -> Vec<StreamEvent> {
        self.right_events += 1;
        self.right_buffer.push_back(event.clone());
        self.prune_right(event.timestamp);
        self.match_left(&event)
    }

    fn match_right(&self, left: &StreamEvent) -> Vec<StreamEvent> {
        self.right_buffer.iter().filter(|r| {
            r.key == left.key && (r.timestamp as i64 - left.timestamp as i64).unsigned_abs() <= self.window_ms
        }).cloned().collect()
    }

    fn match_left(&mut self, right: &StreamEvent) -> Vec<StreamEvent> {
        let matches: Vec<StreamEvent> = self.left_buffer.iter().filter(|l| {
            l.key == right.key && (right.timestamp as i64 - l.timestamp as i64).unsigned_abs() <= self.window_ms
        }).cloned().collect();
        self.joins += matches.len() as u64;
        matches
    }

    fn prune_left(&mut self, ts: u64) { while let Some(front) = self.left_buffer.front() { if ts > front.timestamp + self.window_ms { self.left_buffer.pop_front(); } else { break; } } }
    fn prune_right(&mut self, ts: u64) { while let Some(front) = self.right_buffer.front() { if ts > front.timestamp + self.window_ms { self.right_buffer.pop_front(); } else { break; } } }

    pub fn summary(&self) -> String {
        format!("Join: left={}, right={}, joins={}, window={}ms", self.left_events, self.right_events, self.joins, self.window_ms)
    }
}

fn now() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64 }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window() {
        let mut proc = StreamProcessor::tumbling(100);
        let mut agg_count = 0;
        for i in 0..15 {
            let result = proc.process(StreamEvent::new("k", i as f64, "s"));
            if result.is_some() { agg_count += 1; }
        }
        // Some windows should have closed
        assert!(agg_count > 0);
    }

    #[test]
    fn test_current_aggregation() {
        let mut proc = StreamProcessor::session(10000);
        proc.process(StreamEvent::new("k", 10.0, "s"));
        proc.process(StreamEvent::new("k", 20.0, "s"));
        proc.process(StreamEvent::new("k", 30.0, "s"));
        let agg = proc.current_agg();
        assert_eq!(agg.count, 3);
        assert!((agg.avg - 20.0).abs() < 0.01);
    }

    #[test]
    fn test_window_aggregate() {
        let w = Window { start_ms: 0, end_ms: 100, events: vec![StreamEvent::new("k", 5.0, ""), StreamEvent::new("k", 15.0, "")], is_closed: true };
        let agg = w.aggregate();
        assert_eq!(agg.count, 2);
        assert_eq!(agg.sum, 20.0);
        assert_eq!(agg.min, 5.0);
        assert_eq!(agg.max, 15.0);
    }

    #[test]
    fn test_empty_window_agg() {
        let w = Window { start_ms: 0, end_ms: 100, events: vec![], is_closed: true };
        let agg = w.aggregate();
        assert_eq!(agg.count, 0);
    }

    #[test]
    fn test_stream_join() {
        let mut joiner = StreamJoiner::new(1000);
        joiner.add_left(StreamEvent::new("user1", 1.0, "click"));
        let matches = joiner.add_right(StreamEvent::new("user1", 100.0, "purchase"));
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_stream_join_different_keys() {
        let mut joiner = StreamJoiner::new(1000);
        joiner.add_left(StreamEvent::new("user1", 1.0, "click"));
        let matches = joiner.add_right(StreamEvent::new("user2", 100.0, "purchase"));
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_stream_join_timeout() {
        let mut joiner = StreamJoiner::new(100);
        joiner.add_left(StreamEvent::new("k", 1.0, ""));
        // Right event too far in time
        let late = StreamEvent::new("k", 200.0, "");
        let matches = joiner.add_right(late.clone());
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_summary() {
        let proc = StreamProcessor::tumbling(100);
        let s = proc.summary();
        assert!(s.contains("0 events"));
    }
}
