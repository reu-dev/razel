use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::{config, FileId};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::time::Instant;
use wasi_common::dir::DirCaps;
use wasi_common::file::FileCaps;
use wasi_common::pipe::WritePipe;
use wasi_common::WasiCtx;
use wasmtime::*;
use wasmtime_wasi::sync::WasiCtxBuilder;
use wasmtime_wasi::I32Exit;

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
}

impl WasiExecutor {
    pub fn create_engine() -> Result<Engine> {
        let mut config = Config::new();
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config).context("create WASM engine")?;
        Ok(engine)
    }

    pub fn create_module(engine: &Engine, file: impl AsRef<Path>) -> Result<Module> {
        Module::from_file(engine, &file)
            .with_context(|| format!("create WASM module: {:?}", file.as_ref()))
    }

    pub fn exec(&self, sandbox_dir: &Path) -> ExecutionResult {
        match self.wasi_exec(sandbox_dir) {
            Ok(execution_result) => execution_result,
            Err(error) => ExecutionResult {
                status: ExecutionStatus::FailedToStart,
                error: Some(error),
                ..Default::default()
            },
        }
    }

    fn wasi_exec(&self, sandbox_dir: &Path) -> Result<ExecutionResult> {
        assert!(self.module.is_some());
        let engine = self.module.as_ref().unwrap().engine();
        let mut linker = Linker::new(engine);
        wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;

        let stdout_pipe = WritePipe::new_in_memory();
        let stderr_pipe = WritePipe::new_in_memory();

        let wasi_ctx = self
            .create_wasi_ctx(&stdout_pipe, &stderr_pipe, sandbox_dir)
            .with_context(|| format!("create_wasi_ctx() sandbox_dir: {sandbox_dir:?}"))?;
        let mut store = Store::new(engine, wasi_ctx);
        let instance = linker
            .instantiate(&mut store, self.module.as_ref().unwrap())
            .context("linker.instantiate()")?;
        let func = instance
            .get_typed_func::<(), ()>(&mut store, "_start")
            .context("instance.get_typed_func(_start)")?;

        let mut execution_result: ExecutionResult = Default::default();
        let execution_start = Instant::now();
        match func.call(&mut store, ()) {
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
        execution_result.duration = Some(execution_start.elapsed());

        drop(store);
        execution_result.stdout = stdout_pipe
            .try_into_inner()
            .map_err(|err| anyhow!("failed to take stdout_pipe: {err:?}"))?
            .into_inner();
        execution_result.stderr = stderr_pipe
            .try_into_inner()
            .map_err(|err| anyhow!("failed to take stderr_pipe: {err:?}"))?
            .into_inner();
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
        stdout_pipe: &WritePipe<Cursor<Vec<u8>>>,
        stderr_pipe: &WritePipe<Cursor<Vec<u8>>>,
        sandbox_dir: &Path,
    ) -> Result<WasiCtx> {
        let mut wasi_ctx = WasiCtxBuilder::new()
            .stdout(Box::new(stdout_pipe.clone()))
            .stderr(Box::new(stderr_pipe.clone()))
            .build();
        wasi_ctx.push_arg(&self.executable)?;
        for arg in &self.args {
            wasi_ctx.push_arg(arg)?;
        }
        for (k, v) in &self.env {
            wasi_ctx.push_env(k, v)?;
        }
        Self::add_dir_to_wasi_ctx(&mut wasi_ctx, sandbox_dir, "".into())
            .with_context(|| format!("add_dir_to_wasi_ctx() sandbox_dir: {sandbox_dir:?}"))?;
        Ok(wasi_ctx)
    }

    fn add_dir_to_wasi_ctx(wasi: &mut WasiCtx, host_dir: &Path, guest_dir: PathBuf) -> Result<()> {
        let cap_std_dir = wasi_cap_std_sync::Dir::open_ambient_dir(
            host_dir,
            wasi_cap_std_sync::ambient_authority(),
        )?;
        let wasi_dir = Box::new(wasi_cap_std_sync::dir::Dir::from_cap_std(cap_std_dir));
        let dir_caps = DirCaps::all();
        let file_caps = FileCaps::all();
        wasi.push_dir(wasi_dir, dir_caps, file_caps, guest_dir)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::new_tmp_dir;
    use crate::tasks::ensure_equal;
    use std::fs;

    static CP_MODULE_PATH: &str = "test/bin/wasm32-wasi/cp.wasm";
    static SRC_PATH: &str = "src-file";
    static DST_PATH: &str = "dst-file";
    const SOURCE_CONTENTS: &str = "SOURCE_CONTENTS";

    fn create_cp_module() -> Module {
        let engine = WasiExecutor::create_engine().unwrap();
        WasiExecutor::create_module(&engine, CP_MODULE_PATH).unwrap()
    }

    #[test]
    fn cp_help() {
        let sandbox_dir = new_tmp_dir!();
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec!["-h".into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir());
        println!("{x:?}");
        assert!(x.success());
        assert_eq!(x.exit_code, Some(0));
        assert!(std::str::from_utf8(&x.stdout).unwrap().contains("Usage"));
    }

    #[test]
    fn cp() {
        let sandbox_dir = new_tmp_dir!();
        let src = sandbox_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        let dst = sandbox_dir.join(DST_PATH);
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), DST_PATH.into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir());
        println!("{x:?}");
        assert!(x.success());
        ensure_equal(src, dst).unwrap();
    }

    #[test]
    fn cp_not_existing_input_file() {
        let sandbox_dir = new_tmp_dir!();
        // not writing source file
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), DST_PATH.into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir());
        println!("{x:?}");
        assert!(!x.success());
        assert_eq!(x.exit_code, Some(1));
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[test]
    fn cp_read_outside_sandbox() {
        let sandbox_dir = new_tmp_dir!();
        let file_outside_sandbox = fs::canonicalize("README.md").unwrap();
        assert!(file_outside_sandbox.exists());
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![
                file_outside_sandbox.to_str().unwrap().into(),
                DST_PATH.into(),
            ],
            ..Default::default()
        }
        .exec(sandbox_dir.dir());
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[test]
    fn cp_write_outside_sandbox() {
        let sandbox_dir = new_tmp_dir!();
        sandbox_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        let file_outside_sandbox = fs::canonicalize(".").unwrap().join("not-existing-file");
        assert!(!file_outside_sandbox.exists());
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![
                SRC_PATH.into(),
                file_outside_sandbox.to_str().unwrap().into(),
            ],
            ..Default::default()
        }
        .exec(sandbox_dir.dir());
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening output file"));
    }
}
