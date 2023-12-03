use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const BENCHES_OUT_DIR: &str = "benches";

#[derive(Debug, Serialize, Deserialize)]
pub struct Bench {
    pub id: String,
    pub title: String,
    pub cache_state: CacheState,
    pub timestamp: u128,
    pub duration: f32,
    pub remote_cache_stats_before: Option<Value>,
    pub remote_cache_stats_after: Option<Value>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[allow(clippy::enum_variant_names)]
pub enum CacheState {
    /// Local execution from scratch
    LocalCold,
    /// Local execution zero-check
    LocalWarm,
    /// CI execution from scratch
    LocalColdRemoteCold,
    /// CI execution on new node
    LocalColdRemoteWarm,
}

impl CacheState {
    pub fn is_remote_cache_used(&self) -> bool {
        match self {
            CacheState::LocalCold | CacheState::LocalWarm => false,
            CacheState::LocalColdRemoteCold | CacheState::LocalColdRemoteWarm => true,
        }
    }
}
