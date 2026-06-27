use super::AuthState;
use anyhow::{Context, Result};
use base64::prelude::*;
use jsonwebtoken::DecodingKey;
use serde::Deserialize;
use std::collections::HashMap;

pub(super) struct JwksCache {
    pub jwks_url: String,
    keys: HashMap<String, DecodingKey>,
}

impl JwksCache {
    pub fn new(jwks_url: String) -> Self {
        Self {
            jwks_url,
            keys: Default::default(),
        }
    }
}

#[derive(Deserialize)]
struct JwksDoc {
    keys: Vec<JwkEntry>,
}

#[derive(Deserialize)]
struct JwkEntry {
    kid: Option<String>,
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

impl AuthState {
    pub(super) fn peek_jwt_iss_without_verification(token: &str) -> Option<String> {
        let payload = token.split('.').nth(1)?;
        let bytes = BASE64_URL_SAFE_NO_PAD.decode(payload).ok()?;
        #[derive(Deserialize)]
        struct IssClaim {
            iss: String,
        }
        serde_json::from_slice::<IssClaim>(&bytes)
            .ok()
            .map(|c| c.iss)
    }

    pub(super) async fn resolve_key(
        &self,
        iss: &str,
        kid: &str,
        jwks_url: &str,
    ) -> Result<Option<DecodingKey>> {
        if let Some(key) = self.lookup_key(iss, kid) {
            return Ok(Some(key));
        }
        self.fetch_and_cache(iss, jwks_url).await?;
        Ok(self.lookup_key(iss, kid))
    }

    fn lookup_key(&self, iss: &str, kid: &str) -> Option<DecodingKey> {
        self.jwt_iss_to_jwks_cache
            .read()
            .unwrap()
            .get(iss)?
            .keys
            .get(kid)
            .cloned()
    }

    pub(super) async fn fetch_and_cache(&self, iss: &str, jwks_url: &str) -> Result<()> {
        let doc: JwksDoc = self
            .http
            .get(jwks_url)
            .send()
            .await
            .with_context(|| format!("GET {jwks_url} failed"))?
            .error_for_status()
            .context("JWKS endpoint returned error status")?
            .json()
            .await
            .context("failed to parse JWKS response")?;

        let mut keys = HashMap::new();
        for jwk in doc.keys {
            if jwk.kty != "RSA" {
                continue;
            }
            let (Some(n), Some(e)) = (jwk.n, jwk.e) else {
                continue;
            };
            match DecodingKey::from_rsa_components(&n, &e) {
                Ok(key) => {
                    keys.insert(jwk.kid.unwrap_or_default(), key);
                }
                Err(e) => {
                    tracing::warn!(iss, "skipping JWK with invalid RSA components: {e}");
                }
            }
        }

        let count = keys.len();
        self.jwt_iss_to_jwks_cache
            .write()
            .unwrap()
            .entry(iss.to_string())
            .or_insert_with(|| JwksCache::new(jwks_url.into()))
            .keys = keys;
        tracing::debug!(issuer = iss, count, "JWKS refreshed");
        Ok(())
    }
}
