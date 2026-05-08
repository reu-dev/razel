mod gitlab;
mod interactive;
mod jwt;

use anyhow::Result;
pub use gitlab::GitLabCiIdToken;
use itertools::Itertools;
use jwt::JwksCache;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

/// - GitLab CI: `jwt/<iss_no_scheme>/<project_id>`
/// - interactive user: `user/<user>/<sha256_hex(token)>`
pub type AuthId = String;

/// Authentication for `CreateJobRequest`.
///
/// Two flows, dispatched by the request kind:
///
/// - GitLab CI: the token is verified as a GitLab id_token JWT against the issuer's JWKS; audience must equal `razel`.
///   Identity comes from the verified `iss` and `project_id` claims.
/// - Interactive: the untrusted username is bound on first use to `sha256(token)`,
///   so a later request with the same username and a different token hash is rejected.
pub struct AuthState {
    pub(in crate::auth) http: reqwest::Client,
    pub(in crate::auth) jwt_iss_to_jwks_cache: RwLock<HashMap<String, JwksCache>>,
    pub(in crate::auth) user_to_token_hash: RwLock<HashMap<String, String>>,
}

impl AuthState {
    pub fn new() -> Arc<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("failed to build reqwest client");
        Arc::new(Self {
            http,
            jwt_iss_to_jwks_cache: Default::default(),
            user_to_token_hash: Default::default(),
        })
    }

    pub fn spawn_jwks_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            loop {
                interval.tick().await;
                let entries = self
                    .jwt_iss_to_jwks_cache
                    .read()
                    .unwrap()
                    .iter()
                    .map(|(iss, c)| (iss.clone(), c.jwks_url.clone()))
                    .collect_vec();
                for (iss, jwks_url) in entries {
                    if let Err(e) = self.fetch_and_cache(&iss, &jwks_url).await {
                        tracing::warn!(%iss, "periodic JWKS refresh failed: {e:#}");
                    }
                }
            }
        });
    }

    /// Pre-register a GitLab CI issuer to fetch JWKS in the background.
    pub fn push_gitlab_ci_instance(&self, iss_no_scheme: &str) {
        let iss = format!("https://{iss_no_scheme}");
        let jwks_url = format!("{iss}{}", gitlab::JWKS_PATH);
        self.jwt_iss_to_jwks_cache
            .write()
            .unwrap()
            .entry(iss)
            .or_insert_with(|| JwksCache::new(jwks_url));
    }

    /// Pre-register a user→token-hash binding from an existing project tree.
    pub fn push_interactive_user(&self, user: &str, token_hash: &str) {
        self.user_to_token_hash
            .write()
            .unwrap()
            .insert(user.to_string(), token_hash.to_string());
    }

    /// Verify a JWT token from a `GitLabJobRequest`.
    ///
    /// On a cache miss for the issuer, this fetches JWKS over HTTP.
    /// JWKS rotation refreshes happen in the background (`spawn_jwks_refresh`).
    pub async fn verify_gitlab_ci_id_token(
        &self,
        token: &str,
    ) -> Result<(AuthId, GitLabCiIdToken)> {
        gitlab::verify(self, token).await
    }

    /// Verify an opaque-token request from an `InteractiveJobRequest`.
    pub fn verify_interactive_user(&self, user: &str, token: &str) -> Result<AuthId> {
        interactive::verify(self, user, token)
    }
}
