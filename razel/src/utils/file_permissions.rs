use anyhow::{Context, Result};
use std::fs::Metadata;
use std::path::Path;
use tokio::fs::File;

#[cfg(target_family = "unix")]
pub async fn is_file_executable(file: &File) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    let permissions = file.metadata().await?.permissions();
    Ok(permissions.mode() & 0o100 != 0)
}

#[cfg(not(target_family = "unix"))]
pub async fn is_file_executable(_file: &File) -> Result<bool> {
    Ok(false)
}

#[cfg(target_family = "unix")]
pub async fn make_file_executable(file: &File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = file.metadata().await?.permissions();
    let mode = permissions.mode() | 0o700;
    permissions.set_mode(mode);
    file.set_permissions(permissions).await?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
pub async fn make_file_executable(_file: &File) -> Result<()> {
    Ok(())
}

pub async fn set_file_readonly(path: &Path) -> Result<()> {
    let mut perms = tokio::fs::metadata(path).await?.permissions();
    perms.set_readonly(true);
    tokio::fs::set_permissions(path, perms).await?;
    Ok(())
}

pub async fn force_remove_file(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if tokio::fs::remove_file(path).await.is_err() {
        if let Ok(metadata) = tokio::fs::metadata(path).await {
            drop_readonly_flag(path, metadata)
                .await
                .with_context(|| format!("drop_readonly_flag() {path:?}"))?;
            tokio::fs::remove_file(path)
                .await
                .with_context(|| format!("remove_file() {path:?}"))?;
        } else {
            // file does not exist
        }
    }
    Ok(())
}

pub fn force_remove_file_std(path: impl AsRef<Path>) -> Result<()> {
    let path = path.as_ref();
    if std::fs::remove_file(path).is_err() {
        if let Ok(metadata) = std::fs::metadata(path) {
            drop_readonly_flag_std(path, metadata)
                .with_context(|| format!("drop_readonly_flag() {path:?}"))?;
            std::fs::remove_file(path).with_context(|| format!("remove_file() {path:?}"))?;
        } else {
            // file does not exist
        }
    }
    Ok(())
}

#[cfg(target_family = "unix")]
pub async fn drop_readonly_flag(path: &Path, metadata: Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = metadata.permissions();
    let mode = permissions.mode() | 0o600;
    permissions.set_mode(mode);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
pub async fn drop_readonly_flag(path: &Path, metadata: Metadata) -> Result<()> {
    let mut permissions = metadata.permissions();
    #[allow(clippy::permissions_set_readonly_false)] // this is the non-unix code
    permissions.set_readonly(false);
    tokio::fs::set_permissions(path, permissions).await?;
    Ok(())
}

#[cfg(target_family = "unix")]
pub fn drop_readonly_flag_std(path: &Path, metadata: Metadata) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = metadata.permissions();
    let mode = permissions.mode() | 0o600;
    permissions.set_mode(mode);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
pub fn drop_readonly_flag_std(path: &Path, metadata: Metadata) -> Result<()> {
    let mut permissions = metadata.permissions();
    #[allow(clippy::permissions_set_readonly_false)] // this is the non-unix code
    permissions.set_readonly(false);
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(target_family = "unix")]
pub fn set_file_permissions(file: &std::fs::File, executable: bool, readonly: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = file.metadata()?.permissions();
    if executable {
        let mode = permissions.mode() | 0o700;
        permissions.set_mode(mode);
    }
    permissions.set_readonly(readonly);
    file.set_permissions(permissions)?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
pub fn set_file_permissions(file: &std::fs::File, _executable: bool, readonly: bool) -> Result<()> {
    let mut permissions = file.metadata()?.permissions();
    permissions.set_readonly(readonly);
    file.set_permissions(permissions)?;
    Ok(())
}
