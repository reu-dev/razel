use super::{AuthId, AuthState};
use anyhow::{Result, bail};
use sha2::{Digest, Sha256};

pub(super) fn verify(state: &AuthState, user: &str, token: &str) -> Result<AuthId> {
    sanitize_user(user)?;
    if token.is_empty() {
        bail!("missing token");
    }
    let token_hash = sha256_hex(token);
    use std::collections::hash_map::Entry;
    match state.user_to_token_hash.write().unwrap().entry(user.into()) {
        Entry::Occupied(e) if *e.get() != token_hash => {
            bail!("user is bound to a different token");
        }
        Entry::Occupied(_) => {}
        Entry::Vacant(e) => {
            e.insert(token_hash.clone());
        }
    }
    Ok(format!("user/{user}/{token_hash}"))
}

fn sanitize_user(user: &str) -> Result<()> {
    if !(1..=64).contains(&user.len()) {
        bail!("invalid user name");
    }
    if !user
        .bytes()
        .all(|b| b.is_ascii_graphic() && b != b'/' && b != b'\\')
    {
        bail!("invalid user name");
    }
    Ok(())
}

fn sha256_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_token_rejected() {
        let s = AuthState::new();
        let err = s.verify_interactive_user("alice", "").unwrap_err();
        assert_eq!(err.to_string(), "missing token");
    }

    #[test]
    fn first_use_binds_user() {
        let s = AuthState::new();
        let id = s.verify_interactive_user("alice", "secret-1").unwrap();
        assert!(id.starts_with("user/alice/"), "id={id}");
    }

    #[test]
    fn same_user_different_token_rejected() {
        let s = AuthState::new();
        s.verify_interactive_user("alice", "secret-1").unwrap();
        let err = s.verify_interactive_user("alice", "secret-2").unwrap_err();
        assert_eq!(err.to_string(), "user is bound to a different token");
    }

    #[test]
    fn same_user_same_token_ok() {
        let s = AuthState::new();
        let a = s.verify_interactive_user("alice", "secret-1").unwrap();
        let b = s.verify_interactive_user("alice", "secret-1").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn invalid_user_rejected() {
        let s = AuthState::new();
        for bad in [
            "",
            "has space",
            "../etc",
            "has/slash",
            "back\\slash",
            "ctrl\x01char",
        ] {
            let err = s.verify_interactive_user(bad, "secret").unwrap_err();
            assert_eq!(err.to_string(), "invalid user name", "user={bad}");
        }
    }

    #[test]
    fn valid_user() {
        let s = AuthState::new();
        for u in [
            "a",
            "alice",
            "alice.bob",
            "user_1",
            "x-y",
            "Z9",
            "_www",
            "_nobody",
            "-leading-dash",
            "trailing.",
        ] {
            assert!(s.verify_interactive_user(u, "t").is_ok(), "rejected {u}");
        }
    }
}
