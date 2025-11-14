use serde::Deserialize;
use std::collections::HashMap;

type Domain = String;
type Host = String;
type Slots = usize;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct HttpRemoteExecConfig(pub HashMap<Domain, HashMap<Host, Slots>>);

impl std::str::FromStr for HttpRemoteExecConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|e| e.to_string())
    }
}
