use anyhow::{Result, bail};
use std::path::{Path, PathBuf};
use std::{env, fs};

pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_new(
                std::env::var("RUST_LOG")
                    .unwrap_or("info,cranelift=info,wasmtime=info".to_string()),
            )
            .expect("failed to parse tracing directives"),
        )
        .with_test_writer()
        .finish();
}

pub fn ensure_files_are_equal(file1: PathBuf, file2: PathBuf) -> Result<()> {
    let file1_bytes = fs::read(&file1)?;
    let file2_bytes = fs::read(&file2)?;
    if file1_bytes != file2_bytes {
        bail!("Files {:?} and {:?} differ!", file1, file2);
    }
    Ok(())
}

/// Returns a unique test name to be used for temp file/directories
#[macro_export]
macro_rules! unique_test_name {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        format!(
            "{}_l{}_pid{}",
            name[..name.len() - 3]
                .replace("::{{closure}}", "")
                .replace(':', "_"),
            line!(),
            std::process::id()
        )
    }};
}

#[allow(unused_imports)]
pub use unique_test_name;

/// Changes the current directory when created and restores the original one when dropped.
pub struct ChangeDir {
    original_dir: PathBuf,
}

impl ChangeDir {
    pub fn new(path: &Path) -> Self {
        let original_dir = env::current_dir().unwrap();
        env::set_current_dir(path).unwrap();
        Self { original_dir }
    }
}

impl Drop for ChangeDir {
    fn drop(&mut self) {
        env::set_current_dir(&self.original_dir).unwrap();
    }
}

/// Creates a <TempDir> with unique relative path
#[macro_export]
macro_rules! new_tmp_dir {
    () => {{
        $crate::test_utils::TempDir::with_dir(
            std::path::Path::new(".tmp").join($crate::test_utils::unique_test_name!()),
        )
    }};
}

/// Temp directory/file tool for tests
pub struct TempDir {
    dir: PathBuf,
}

impl TempDir {
    pub fn with_dir(dir: PathBuf) -> Self {
        fs::create_dir_all(&dir).unwrap();
        Self { dir }
    }

    pub fn dir(&self) -> &PathBuf {
        &self.dir
    }

    /// Return a path within the temp dir
    pub fn join(&self, path: &str) -> PathBuf {
        self.dir.join(path)
    }

    /// Return a path within the temp dir
    pub fn join_and_create_parent(&self, path: &str) -> PathBuf {
        let child = self.dir.join(path);
        fs::create_dir_all(child.parent().unwrap()).unwrap();
        child
    }

    /// Write a file within the temp dir and return its path
    pub fn join_and_write_file(&self, path: &str, contents: &str) -> PathBuf {
        let child = self.dir.join(path);
        fs::create_dir_all(child.parent().unwrap()).unwrap();
        fs::write(&child, contents).unwrap();
        child
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.dir).ok();
    }
}
