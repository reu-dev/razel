use crate::config::OUT_DIR;
use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::{config, FileId};
use anyhow::{Context, Result};
use cap_std::ambient_authority;
use cap_std::fs::Dir;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use wasmtime::component::ResourceTable;
use wasmtime::{Config, Engine, Linker, Module, Store};
use wasmtime_wasi::pipe::MemoryOutputPipe;
use wasmtime_wasi::preview1::{add_to_linker_async, WasiPreview1Adapter, WasiPreview1View};
use wasmtime_wasi::{DirPerms, FilePerms, I32Exit, WasiCtx, WasiCtxBuilder, WasiView};

struct Ctx {
    table: ResourceTable,
    wasi: WasiCtx,
    adapter: WasiPreview1Adapter,
}

impl WasiView for Ctx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl WasiPreview1View for Ctx {
    fn adapter(&self) -> &WasiPreview1Adapter {
        &self.adapter
    }
    fn adapter_mut(&mut self) -> &mut WasiPreview1Adapter {
        &mut self.adapter
    }
}

/// WASI filesystem:
/// - preopen sandbox_dir for reading
/// - preopen sandbox_dir/razel-out for writing
/// - input files from cache: hardlink into sandbox
/// - input files outside cache: preopen parent dirs for reading
#[derive(Clone, Default)]
pub struct WasiExecutor {
    /// WASM module, is internally shared between executors to compile just once
    pub module: Option<Module>,
    pub module_file_id: Option<FileId>,
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
    pub read_dirs: Vec<PathBuf>,
    pub write_dir: bool,
}

impl WasiExecutor {
    pub fn create_engine() -> Result<Engine> {
        let mut config = Config::new();
        config.async_support(true);
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config).context("create WASM engine")?;
        Ok(engine)
    }

    pub fn create_module(engine: &Engine, file: impl AsRef<Path>) -> Result<Module> {
        Module::from_file(engine, &file)
            .with_context(|| format!("create WASM module: {:?}", file.as_ref()))
    }

    pub async fn exec(&self, cwd: &Path, sandbox_dir: &Path) -> ExecutionResult {
        match self.wasi_exec(cwd, sandbox_dir).await {
            Ok(execution_result) => execution_result,
            Err(error) => ExecutionResult {
                status: ExecutionStatus::FailedToStart,
                error: Some(error),
                ..Default::default()
            },
        }
    }

    async fn wasi_exec(&self, cwd: &Path, sandbox_dir: &Path) -> Result<ExecutionResult> {
        assert!(self.module.is_some());
        let engine = self.module.as_ref().unwrap().engine();
        let mut linker = Linker::new(engine);
        add_to_linker_async(&mut linker, |x| x)?;

        let (ctx, stdout, stderr) = self
            .create_wasi_ctx(cwd, sandbox_dir)
            .with_context(|| format!("cwd: {cwd:?}, sandbox_dir: {sandbox_dir:?}"))
            .context("Error in create_wasi_ctx()")?;
        let mut store = Store::new(engine, ctx);
        let instance = linker
            .instantiate_async(&mut store, self.module.as_ref().unwrap())
            .await
            .context("linker.instantiate()")?;
        let func = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .context("instance.get_typed_func(_start)")?;

        let mut execution_result: ExecutionResult = Default::default();
        let execution_start = Instant::now();
        match func.call_async(&mut store, ()).await {
            Ok(()) => {
                execution_result.status = ExecutionStatus::Success;
                execution_result.exit_code = Some(0);
            }
            Err(error) => {
                if let Some(exit_code) = error.downcast_ref::<I32Exit>() {
                    execution_result.status = ExecutionStatus::Failed;
                    execution_result.exit_code = Some(exit_code.0);
                } else {
                    execution_result.status = ExecutionStatus::Crashed;
                    execution_result.error = Some(error);
                }
            }
        }
        execution_result.exec_duration = Some(execution_start.elapsed());

        drop(store);
        execution_result.stdout = stdout.try_into_inner().unwrap().into();
        execution_result.stderr = stderr.try_into_inner().unwrap().into();
        Ok(execution_result)
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        [
            config::EXECUTABLE.into(),
            "command".into(),
            "--".into(),
            self.executable.clone(),
        ]
        .iter()
        .chain(self.args.iter())
        .cloned()
        .collect()
    }

    pub fn command_line_with_redirects(&self, razel_executable: &str) -> Vec<String> {
        [
            razel_executable.into(),
            "command".into(),
            "--".into(),
            self.executable.clone(),
        ]
        .iter()
        .chain(self.args.iter())
        .chain(
            self.stdout_file
                .as_ref()
                .map(|x| [">".to_string(), x.to_str().unwrap().to_string()])
                .iter()
                .flatten(),
        )
        .chain(
            self.stderr_file
                .as_ref()
                .map(|x| ["2>".to_string(), x.to_str().unwrap().to_string()])
                .iter()
                .flatten(),
        )
        .cloned()
        .collect()
    }

    fn create_wasi_ctx(
        &self,
        cwd: &Path,
        sandbox_dir: &Path,
    ) -> Result<(Ctx, MemoryOutputPipe, MemoryOutputPipe)> {
        let stdout = MemoryOutputPipe::new(4096);
        let stderr = MemoryOutputPipe::new(4096);
        let mut builder = WasiCtxBuilder::new();
        builder.stdout(stdout.clone()).stderr(stderr.clone());
        builder.arg(&self.executable);
        for arg in &self.args {
            builder.arg(wasi_path(arg));
        }
        for (k, v) in &self.env {
            builder.env(k, v);
        }
        for dir in self.read_dirs.iter().filter(|x| !x.starts_with(OUT_DIR)) {
            assert!(dir.is_relative());
            preopen_dir_for_read(
                &mut builder,
                &cwd.join(dir),
                &wasi_path(dir.to_str().unwrap()),
            )?;
        }
        if self.write_dir {
            preopen_dir_for_write(&mut builder, &sandbox_dir.join(OUT_DIR), OUT_DIR)?;
        }
        let ctx = Ctx {
            table: ResourceTable::new(),
            wasi: builder.build(),
            adapter: WasiPreview1Adapter::new(),
        };
        Ok((ctx, stdout, stderr))
    }
}

#[cfg(target_family = "windows")]
fn wasi_path(path: &str) -> String {
    path.replace("\\", "/")
}

#[cfg(not(target_family = "windows"))]
fn wasi_path(path: &str) -> String {
    path.into()
}

fn preopen_dir_for_read(
    builder: &mut WasiCtxBuilder,
    host_dir: &Path,
    guest_dir: &str,
) -> Result<()> {
    log::debug!("preopen_dir_for_read() host: {host_dir:?}, guest: {guest_dir:?}");
    let cap_dir = Dir::open_ambient_dir(host_dir, ambient_authority()).with_context(|| {
        format!("preopen_dir_for_read() host: {host_dir:?}, guest: {guest_dir:?}")
    })?;
    builder.preopened_dir(cap_dir, DirPerms::READ, FilePerms::READ, guest_dir);
    Ok(())
}

fn preopen_dir_for_write(
    builder: &mut WasiCtxBuilder,
    host_dir: &Path,
    guest_dir: &str,
) -> Result<()> {
    log::debug!("preopen_dir_for_write() host: {host_dir:?}");
    let cap_dir = Dir::open_ambient_dir(host_dir, ambient_authority()).with_context(|| {
        format!("preopen_dir_for_write() host: {host_dir:?}, guest: {guest_dir:?}")
    })?;
    builder.preopened_dir(cap_dir, DirPerms::all(), FilePerms::all(), guest_dir);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use crate::tasks::ensure_equal;
    use std::fs;

    static CP_MODULE_PATH: &str = "examples/bin/wasm32-wasi/cp.wasm";
    static SRC_PATH: &str = "src-file";
    static DST_PATH: &str = "dst-file";
    const SOURCE_CONTENTS: &str = "SOURCE_CONTENTS";

    fn create_cp_module() -> Module {
        let engine = WasiExecutor::create_engine().unwrap();
        WasiExecutor::create_module(&engine, CP_MODULE_PATH).unwrap()
    }

    #[tokio::test]
    async fn cp_help() {
        let workspace_dir = Path::new(".");
        let sandbox_dir = new_tmp_dir!();
        let mut x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec!["-h".into()],
            ..Default::default()
        }
        .exec(workspace_dir, sandbox_dir.dir())
        .await;
        println!("{x:?}");
        x.assert_success();
        assert!(std::str::from_utf8(&x.stdout).unwrap().contains("Usage"));
    }

    #[tokio::test]
    async fn cp() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let src = workspace_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        let dst = sandbox_dir.join_and_create_parent(&out_file);
        let mut x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), out_file],
            read_dirs: vec![".".into()],
            write_dir: true,
            ..Default::default()
        }
        .exec(workspace_dir.dir(), sandbox_dir.dir())
        .await;
        println!("{x:?}");
        x.assert_success();
        ensure_equal(src, dst).unwrap();
    }

    #[tokio::test]
    async fn cp_not_existing_input_file() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let _dst = sandbox_dir.join_and_create_parent(&out_file);
        // not writing source file
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), out_file],
            read_dirs: vec![".".into()],
            write_dir: true,
            ..Default::default()
        }
        .exec(workspace_dir.dir(), sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(!x.success());
        assert_eq!(x.exit_code, Some(1));
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[tokio::test]
    async fn cp_read_outside_sandbox() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let file_outside_sandbox = fs::canonicalize("README.md").unwrap();
        let _dst = sandbox_dir.join_and_create_parent(&out_file);
        assert!(file_outside_sandbox.exists());
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![file_outside_sandbox.to_str().unwrap().into(), out_file],
            read_dirs: vec![".".into()],
            write_dir: true,
            ..Default::default()
        }
        .exec(workspace_dir.dir(), sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[tokio::test]
    async fn cp_write_outside_write_dir() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = DST_PATH; // writing outside razel-out should fail
        workspace_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        sandbox_dir.join_and_create_parent(&format!("{OUT_DIR}/{DST_PATH}"));
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), out_file.into()],
            read_dirs: vec![".".into()],
            write_dir: true,
            ..Default::default()
        }
        .exec(workspace_dir.dir(), sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening output file"));
    }
}
