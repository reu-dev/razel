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
