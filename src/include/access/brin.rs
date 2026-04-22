use serde::{Deserialize, Serialize};

pub const BRIN_DEFAULT_PAGES_PER_RANGE: u32 = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrinOptions {
    pub pages_per_range: u32,
}

impl Default for BrinOptions {
    fn default() -> Self {
        Self {
            pages_per_range: BRIN_DEFAULT_PAGES_PER_RANGE,
        }
    }
}
