use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Tag {
    #[serde(rename = "razel:quiet")]
    Quiet,
    #[serde(rename = "razel:verbose")]
    Verbose,
    Custom(String),
}

impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag = String::deserialize(de)?;
        if let Some(x) = tag.strip_prefix("razel:") {
            match x {
                "quiet" => Ok(Tag::Quiet),
                "verbose" => Ok(Tag::Verbose),
                _ => Err(Error::custom(format!(
                    "unknown tag (razel prefix is reserved): {tag}"
                ))),
            }
        } else {
            Ok(Tag::Custom(tag))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize() {
        assert_eq!(
            serde_json::from_str::<Tag>("\"razel:verbose\"").unwrap(),
            Tag::Verbose
        );
        assert_eq!(
            serde_json::from_str::<Tag>("\"anything\"").unwrap(),
            Tag::Custom("anything".into())
        );
        assert!(serde_json::from_str::<Tag>("\"razel:xxx\"").is_err());
    }
}
