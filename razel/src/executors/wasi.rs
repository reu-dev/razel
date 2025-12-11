use crate::config::OUT_DIR;
use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::types::CommandTarget;
use crate::SandboxDir;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use wasmtime::{Config, Engine, Linker, Module, Store};
use wasmtime_wasi::pipe::MemoryOutputPipe;
use wasmtime_wasi::preview1::{add_to_linker_async, WasiP1Ctx};
use wasmtime_wasi::{DirPerms, FilePerms, I32Exit, WasiCtxBuilder};

struct WasiExecutorState {
    engine: Option<Engine>,
    modules: HashMap<PathBuf, Module>,
}

#[derive(Clone)]
pub struct SharedWasiExecutorState(Arc<Mutex<WasiExecutorState>>);

impl Default for SharedWasiExecutorState {
    fn default() -> Self {
        Self::new()
    }
}

impl SharedWasiExecutorState {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(WasiExecutorState {
            engine: None,
            modules: Default::default(),
        })))
    }

    fn new_engine() -> Result<Engine> {
        let mut config = Config::new();
        config.async_support(true);
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config).context("create WASM engine")?;
        Ok(engine)
    }

    async fn get_or_create_module(&self, file: impl AsRef<Path>) -> Result<Module> {
        let file = file.as_ref();
        let mut state = self.0.lock().await;
        if let Some(module) = state.modules.get(file) {
            return Ok(module.clone());
        }
        let engine = match &state.engine {
            Some(x) => x,
            None => state.engine.insert(Self::new_engine()?),
        };
        let module = Module::from_file(engine, file)
            .with_context(|| format!("create WASM module: {file:?}"))?;
        state.modules.insert(file.into(), module.clone());
        Ok(module)
    }
}

/// WASI filesystem:
/// - preopen sandbox_dir for reading
/// - preopen sandbox_dir/razel-out for writing
/// - input files from cache: hardlink into sandbox
/// - input files outside cache: preopen parent dirs for reading
pub struct WasiExecutor {
    state: SharedWasiExecutorState,
    command: CommandTarget,
    /// WASM module, is internally shared between executors to compile just once
    read_dirs: Vec<PathBuf>,
    write_dir: bool,
}

impl WasiExecutor {
    pub fn new(
        state: SharedWasiExecutorState,
        command: CommandTarget,
        read_dirs: Vec<PathBuf>,
        write_dir: bool,
    ) -> Self {
        Self {
            state,
            command,
            read_dirs,
            write_dir,
        }
    }

    pub async fn exec(&self, cwd: &Path, sandbox_dir: &SandboxDir) -> ExecutionResult {
        let module = match self
            .state
            .get_or_create_module(&self.command.executable)
            .await
        {
            Ok(module) => module,
            Err(e) => {
                return ExecutionResult {
                    status: ExecutionStatus::FailedToStart,
                    error: Some(e.to_string()),
                    ..Default::default()
                };
            }
        };
        self.wasi_exec(module, cwd, sandbox_dir)
            .await
            .unwrap_or_else(|error| ExecutionResult {
                status: ExecutionStatus::FailedToStart,
                error: Some(error.to_string()),
                ..Default::default()
            })
    }

    async fn wasi_exec(
        &self,
        module: Module,
        cwd: &Path,
        sandbox_dir: &SandboxDir,
    ) -> Result<ExecutionResult> {
        let engine = module.engine();
        let mut linker = Linker::new(engine);
        add_to_linker_async(&mut linker, |x| x)?;

        let (ctx, stdout, stderr) = self
            .create_wasi_ctx(cwd, sandbox_dir)
            .with_context(|| format!("cwd: {cwd:?}, sandbox_dir: {sandbox_dir:?}"))
            .context("Error in create_wasi_ctx()")?;
        let mut store = Store::new(engine, ctx);
        let instance = linker
            .instantiate_async(&mut store, &module)
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
                    execution_result.error = Some(error.to_string());
                }
            }
        }
        execution_result.exec_duration = Some(execution_start.elapsed());

        drop(store);
        execution_result.stdout = stdout.try_into_inner().unwrap().into();
        execution_result.stderr = stderr.try_into_inner().unwrap().into();
        Ok(execution_result)
    }

    fn create_wasi_ctx(
        &self,
        cwd: &Path,
        sandbox_dir: &SandboxDir,
    ) -> Result<(WasiP1Ctx, MemoryOutputPipe, MemoryOutputPipe)> {
        let stdout = MemoryOutputPipe::new(4096);
        let stderr = MemoryOutputPipe::new(4096);
        let mut builder = WasiCtxBuilder::new();
        builder.stdout(stdout.clone()).stderr(stderr.clone());
        builder.arg(&self.command.executable);
        for arg in &self.command.args {
            builder.arg(wasi_path(arg));
        }
        for (k, v) in &self.command.env {
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
        let ctx = builder.build_p1();
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
    builder
        .preopened_dir(host_dir, guest_dir, DirPerms::READ, FilePerms::READ)
        .with_context(|| {
            format!("preopen_dir_for_read() host: {host_dir:?}, guest: {guest_dir:?}")
        })?;
    Ok(())
}

fn preopen_dir_for_write(
    builder: &mut WasiCtxBuilder,
    host_dir: &Path,
    guest_dir: &str,
) -> Result<()> {
    log::debug!("preopen_dir_for_write() host: {host_dir:?}, guest: {guest_dir:?}");
    builder
        .preopened_dir(host_dir, guest_dir, DirPerms::all(), FilePerms::all())
        .with_context(|| {
            format!("preopen_dir_for_write() host: {host_dir:?}, guest: {guest_dir:?}")
        })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use crate::test_utils::ensure_files_are_equal;
    use std::fs;

    static CP_MODULE_PATH: &str = "../examples/bin/wasm32-wasi/cp.wasm";
    static SRC_PATH: &str = "src-file";
    static DST_PATH: &str = "dst-file";
    const SOURCE_CONTENTS: &str = "SOURCE_CONTENTS";

    #[tokio::test]
    async fn cp_help() {
        let workspace_dir = Path::new(".");
        let sandbox_dir = new_tmp_dir!();
        let executor = WasiExecutor {
            state: SharedWasiExecutorState::new(),
            command: CommandTarget {
                executable: CP_MODULE_PATH.into(),
                args: vec!["-h".into()],
                ..Default::default()
            },
            read_dirs: vec![],
            write_dir: false,
        };
        let result = executor
            .exec(workspace_dir, &sandbox_dir.dir().into())
            .await;
        println!("{result:?}");
        result.assert_success();
        assert!(std::str::from_utf8(&result.stdout)
            .unwrap()
            .contains("Usage"));
    }

    #[tokio::test]
    async fn cp() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let src = workspace_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        let dst = sandbox_dir.join_and_create_parent(&out_file);
        let executor = WasiExecutor {
            state: SharedWasiExecutorState::new(),
            command: CommandTarget {
                executable: CP_MODULE_PATH.into(),
                args: vec![SRC_PATH.into(), out_file],
                ..Default::default()
            },
            read_dirs: vec![".".into()],
            write_dir: true,
        };
        let result = executor
            .exec(workspace_dir.dir(), &sandbox_dir.dir().into())
            .await;
        println!("{result:?}");
        result.assert_success();
        ensure_files_are_equal(src, dst).unwrap();
    }

    #[tokio::test]
    async fn cp_not_existing_input_file() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let _dst = sandbox_dir.join_and_create_parent(&out_file);
        // not writing source file
        let executor = WasiExecutor {
            state: SharedWasiExecutorState::new(),
            command: CommandTarget {
                executable: CP_MODULE_PATH.into(),
                args: vec![SRC_PATH.into(), out_file],
                ..Default::default()
            },
            read_dirs: vec![".".into()],
            write_dir: true,
        };
        let result = executor
            .exec(workspace_dir.dir(), &sandbox_dir.dir().into())
            .await;
        println!("{result:?}");
        assert!(!result.success());
        assert_eq!(result.exit_code, Some(1));
        assert!(std::str::from_utf8(&result.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[tokio::test]
    async fn cp_read_outside_sandbox() {
        let workspace_dir = new_tmp_dir!();
        let sandbox_dir = new_tmp_dir!();
        let out_file = format!("{OUT_DIR}/{DST_PATH}");
        let file_outside_sandbox = fs::canonicalize("Cargo.toml").unwrap();
        let _dst = sandbox_dir.join_and_create_parent(&out_file);
        assert!(file_outside_sandbox.exists());
        let executor = WasiExecutor {
            state: SharedWasiExecutorState::new(),
            command: CommandTarget {
                executable: CP_MODULE_PATH.into(),
                args: vec![file_outside_sandbox.to_str().unwrap().into(), out_file],
                ..Default::default()
            },
            read_dirs: vec![".".into()],
            write_dir: true,
        };
        let result = executor
            .exec(workspace_dir.dir(), &sandbox_dir.dir().into())
            .await;
        println!("{result:?}");
        assert!(!result.success());
        assert!(std::str::from_utf8(&result.stderr)
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
        let executor = WasiExecutor {
            state: SharedWasiExecutorState::new(),
            command: CommandTarget {
                executable: CP_MODULE_PATH.into(),
                args: vec![SRC_PATH.into(), out_file.into()],
                ..Default::default()
            },
            read_dirs: vec![".".into()],
            write_dir: true,
        };
        let result = executor
            .exec(workspace_dir.dir(), &sandbox_dir.dir().into())
            .await;
        println!("{result:?}");
        assert!(!result.success());
        assert!(std::str::from_utf8(&result.stderr)
            .unwrap()
            .contains("error opening output file"));
    }
}
