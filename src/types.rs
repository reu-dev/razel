use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum CacheHit {
    Local,
    Remote,
    /// AC from local cache, some files from remote cache
    Mixed,
}
