use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Tag {
    #[serde(rename = "razel:quiet")]
    Quiet,
    #[serde(rename = "razel:verbose")]
    Verbose,
    Custom(String),
}
