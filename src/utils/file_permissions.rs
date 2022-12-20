use std::path::Path;
use tokio::fs::File;

#[cfg(target_family = "unix")]
pub async fn make_file_executable(file: &File) -> Result<(), anyhow::Error> {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = file.metadata().await?.permissions();
    let mode = permissions.mode() | 0o700;
    permissions.set_mode(mode);
    file.set_permissions(permissions).await?;
    Ok(())
}

#[cfg(not(target_family = "unix"))]
pub async fn make_file_executable(_file: &File) -> Result<(), anyhow::Error> {
    Ok(())
}

pub async fn set_file_readonly(path: &Path) -> Result<(), anyhow::Error> {
    let mut perms = tokio::fs::metadata(path).await?.permissions();
    perms.set_readonly(true);
    tokio::fs::set_permissions(path, perms).await?;
    Ok(())
}
