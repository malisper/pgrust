use serde::{Deserialize, Serialize};

use crate::datum::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanKeyData {
    pub attribute_number: i16,
    pub strategy: u16,
    pub argument: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BtreeOptions {
    pub fillfactor: u16,
    pub deduplicate_items: bool,
}

impl Default for BtreeOptions {
    fn default() -> Self {
        Self {
            fillfactor: 90,
            deduplicate_items: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrinOptions {
    pub pages_per_range: u32,
}

impl Default for BrinOptions {
    fn default() -> Self {
        Self {
            pages_per_range: 128,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GistBufferingMode {
    Auto,
    On,
    Off,
}

impl Default for GistBufferingMode {
    fn default() -> Self {
        Self::Auto
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct GistOptions {
    #[serde(default = "default_gist_fillfactor")]
    pub fillfactor: u16,
    #[serde(default)]
    pub buffering_mode: GistBufferingMode,
}

impl Default for GistOptions {
    fn default() -> Self {
        Self {
            fillfactor: default_gist_fillfactor(),
            buffering_mode: GistBufferingMode::Auto,
        }
    }
}

const fn default_gist_fillfactor() -> u16 {
    90
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GinOptions {
    pub fastupdate: bool,
    pub pending_list_limit_kb: u32,
}

impl Default for GinOptions {
    fn default() -> Self {
        Self {
            fastupdate: true,
            pending_list_limit_kb: 4096,
        }
    }
}

impl GinOptions {
    pub fn pending_list_limit_bytes(&self) -> usize {
        (self.pending_list_limit_kb as usize).saturating_mul(1024)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HashOptions {
    pub fillfactor: u16,
}

impl Default for HashOptions {
    fn default() -> Self {
        Self { fillfactor: 75 }
    }
}
