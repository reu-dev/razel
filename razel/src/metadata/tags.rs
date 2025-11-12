use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Debug, PartialEq)]
pub enum Tag {
    Quiet,
    Verbose,
    Condition,
    Timeout(u16),
    NoCache,
    NoRemoteCache,
    NoSandbox,
    Custom(String),
}

impl Serialize for Tag {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let x = match self {
            Tag::Quiet => "razel:quiet",
            Tag::Verbose => "razel:verbose",
            Tag::Condition => "razel:condition",
            Tag::Timeout(x) => &format!("razel:timeout:{x}"),
            Tag::NoCache => "razel:no-cache",
            Tag::NoRemoteCache => "razel:no-remote-cache",
            Tag::NoSandbox => "razel:no-sandbox",
            Tag::Custom(x) => x,
        };
        serializer.serialize_str(x)
    }
}

impl<'de> Deserialize<'de> for Tag {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tag = String::deserialize(de)?;
        if let Some(key_value) = tag.strip_prefix("razel:") {
            let (key, value) = key_value
                .split_once(':')
                .map_or((key_value, None), |(k, v)| (k, Some(v)));
            match (key, value) {
                ("quiet", None) => Ok(Tag::Quiet),
                ("verbose", None) => Ok(Tag::Verbose),
                ("condition", None) => Ok(Tag::Condition),
                ("timeout", Some(x)) => {
                    let secs = x
                        .parse()
                        .map_err(|x| Error::custom(format!("failed to parse timeout: {x}")))?;
                    Ok(Tag::Timeout(secs))
                }
                ("timeout", None) => Err(Error::custom(format!("timeout value missing: {tag}"))),
                ("no-cache", None) => Ok(Tag::NoCache),
                ("no-remote-cache", None) => Ok(Tag::NoRemoteCache),
                ("no-sandbox", None) => Ok(Tag::NoSandbox),
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
            serde_json::from_str::<Tag>("\"razel:timeout:13\"").unwrap(),
            Tag::Timeout(13)
        );
        assert!(serde_json::from_str::<Tag>("\"razel:timeout:13m\"").is_err());
        assert_eq!(
            serde_json::from_str::<Tag>("\"razel:no-sandbox\"").unwrap(),
            Tag::NoSandbox
        );
        assert_eq!(
            serde_json::from_str::<Tag>("\"anything\"").unwrap(),
            Tag::Custom("anything".into())
        );
        assert!(serde_json::from_str::<Tag>("\"razel:xxx\"").is_err());
    }
}
