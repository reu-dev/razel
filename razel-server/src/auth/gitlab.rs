use super::{AuthId, AuthState};
use anyhow::{Result, anyhow, bail};
use jsonwebtoken::{Algorithm, Validation, decode, decode_header};
use serde::Deserialize;

/// Hardcoded `aud` claim that any accepted JWT must carry.
const JWT_AUDIENCE: &str = "razel";
pub(super) const JWKS_PATH: &str = "/oauth/discovery/keys";

#[derive(Debug)]
pub struct GitLabCiIdToken {
    pub iss: String,
    pub project_path: String,
    pub pipeline_id: u64,
    pub job_id: u64,
    pub user_login: String,
}

#[derive(Deserialize)]
struct RawClaims {
    iss: String,
    project_id: String,
    project_path: String,
    pipeline_id: String,
    job_id: String,
    user_login: String,
}

/// Verify JWT and parse GitLab specific data.
///
/// See <https://docs.gitlab.com/ci/secrets/id_token_authentication/>.
pub(super) async fn verify(state: &AuthState, token: &str) -> Result<(AuthId, GitLabCiIdToken)> {
    if token.is_empty() {
        bail!("missing token");
    }
    let header = decode_header(token).map_err(|_| anyhow!("invalid JWT: header"))?;
    let kid = header.kid.as_deref().unwrap_or("");
    let iss = AuthState::peek_jwt_iss_without_verification(token)
        .ok_or_else(|| anyhow!("invalid JWT: missing iss"))?;
    let jwks_url = format!("{}{}", iss.trim_end_matches('/'), JWKS_PATH);
    let key = state
        .resolve_key(&iss, kid, &jwks_url)
        .await
        .map_err(|_| anyhow!("JWKS unavailable for issuer {iss}"))?
        .ok_or_else(|| anyhow!("invalid JWT: unknown kid"))?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[JWT_AUDIENCE]);
    validation.set_required_spec_claims(&["exp", "iss", "aud"]);
    validation.set_issuer(&[iss.as_str()]);
    let data = decode::<RawClaims>(token, &key, &validation)
        .map_err(|_| anyhow!("invalid JWT: verification failed"))?;
    let iss_no_scheme = strip_scheme(&data.claims.iss);
    if iss_no_scheme.is_empty() || iss_no_scheme.contains('/') || iss_no_scheme.contains("..") {
        bail!("invalid JWT: iss");
    }
    let project_id: u64 = data
        .claims
        .project_id
        .parse()
        .map_err(|_| anyhow!("invalid JWT: project_id"))?;
    let pipeline_id: u64 = data
        .claims
        .pipeline_id
        .parse()
        .map_err(|_| anyhow!("invalid JWT: pipeline_id"))?;
    let job_id: u64 = data
        .claims
        .job_id
        .parse()
        .map_err(|_| anyhow!("invalid JWT: job_id"))?;
    let project_path = data.claims.project_path;
    if project_path.is_empty() || project_path.contains("..") || project_path.starts_with('/') {
        bail!("invalid JWT: project_path");
    }
    let user_login = data.claims.user_login;
    if user_login.is_empty() {
        bail!("invalid JWT: user_login");
    }
    Ok((
        format!("jwt/{iss_no_scheme}/{project_id}"),
        GitLabCiIdToken {
            iss,
            project_path,
            pipeline_id,
            job_id,
            user_login,
        },
    ))
}

fn strip_scheme(s: &str) -> String {
    let rest = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
        .unwrap_or(s);
    rest.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn empty_token_rejected_jwt() {
        let s = AuthState::new();
        let err = s.verify_gitlab_ci_id_token("").await.unwrap_err();
        assert_eq!(err.to_string(), "missing token");
    }

    #[test]
    fn strip_scheme_strips_https() {
        assert_eq!(strip_scheme("https://gitlab.com"), "gitlab.com");
        assert_eq!(strip_scheme("https://gitlab.com/"), "gitlab.com");
        assert_eq!(strip_scheme("http://x"), "x");
    }
}
