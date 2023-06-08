use std::time::{Duration, Instant};

pub struct RateLimiter {
    window_size: usize,
    max_requests: usize,
    requests: Vec<Instant>,
    message_counter: usize,
}
impl RateLimiter {
    pub fn new(window_size: usize, max_requests: usize) -> Self {
        RateLimiter {
            window_size,
            max_requests,
            requests: Vec::with_capacity(max_requests),
            message_counter: 0,
        }
    }

    pub fn allow_request(&mut self) -> bool {
        let now = Instant::now();
        self.requests.retain(|&t| now.duration_since(t) < Duration::from_secs(self.window_size as u64));
        if self.requests.len() >= self.max_requests || self.message_counter >= self.max_requests {
            return false;
        }
        self.requests.push(now);
        self.message_counter += 1;
        true
    }

    pub fn decrease_message_counter(&mut self) {
        self.message_counter -= 1;
    }
}