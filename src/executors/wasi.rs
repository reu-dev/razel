use crate::executors::{ExecutionResult, ExecutionStatus};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::fs::File;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use wasi_common::pipe::WritePipe;
use wasi_common::WasiCtx;
use wasmtime::*;
use wasmtime_wasi::sync::WasiCtxBuilder;
use wasmtime_wasi::I32Exit;

pub fn create_engine() -> Result<Engine> {
    let mut config = Config::new();
    config.cranelift_nan_canonicalization(true);
    let engine = Engine::new(&config)?;
    Ok(engine)
}

pub fn create_module(engine: &Engine, file: impl AsRef<Path>) -> Result<Module> {
    Module::from_file(engine, &file)
}

#[derive(Clone, Default)]
pub struct WasiExecutor {
    /// WASM module, is internally shared between executors to compile just once
    pub module: Option<Module>,
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub input_paths: Vec<String>,
    pub output_paths: Vec<String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
}

impl WasiExecutor {
    pub fn exec(&self) -> ExecutionResult {
        match self.wasi_exec() {
            Ok(execution_result) => execution_result,
            Err(error) => ExecutionResult {
                status: ExecutionStatus::FailedToStart,
                error: Some(error),
                ..Default::default()
            },
        }
    }

    fn wasi_exec(&self) -> Result<ExecutionResult> {
        let engine = self.module.as_ref().unwrap().engine();
        let mut linker = Linker::new(engine);
        wasmtime_wasi::add_to_linker(&mut linker, |s| s)?;

        let stdout_pipe = WritePipe::new_in_memory();
        let stderr_pipe = WritePipe::new_in_memory();

        let wasi_ctx = self.create_wasi_ctx(&stdout_pipe, &stderr_pipe)?;
        let mut store = Store::new(engine, wasi_ctx);
        let instance = linker.instantiate(&mut store, self.module.as_ref().unwrap())?;
        let func = instance.get_typed_func::<(), ()>(&mut store, "_start")?;

        let mut execution_result: ExecutionResult = Default::default();
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

    fn create_wasi_ctx(
        &self,
        stdout_pipe: &WritePipe<Cursor<Vec<u8>>>,
        stderr_pipe: &WritePipe<Cursor<Vec<u8>>>,
    ) -> Result<WasiCtx> {
        let mut wasi_ctx = WasiCtxBuilder::new()
            .stdout(Box::new(stdout_pipe.clone()))
            .stderr(Box::new(stderr_pipe.clone()))
            .build();
        wasi_ctx.push_arg(&self.executable)?;
        for arg in &self.args {
            wasi_ctx.push_arg(arg)?;
        }
        for path in &self.input_paths {
            push_input_file_to_wasi_ctx(&mut wasi_ctx, Path::new(path))
                .with_context(|| format!("push_input_file_to_wasi_ctx(): {path}"))?;
        }
        for path in &self.output_paths {
            push_output_file_to_wasi_ctx(&mut wasi_ctx, Path::new(path))
                .with_context(|| format!("push_input_file_to_wasi_ctx(): {path}"))?;
        }
        Ok(wasi_ctx)
    }
}

fn push_input_file_to_wasi_ctx(wasi: &mut WasiCtx, path: &Path) -> Result<()> {
    add_file_to_wasi_ctx(wasi, path)
}

fn push_output_file_to_wasi_ctx(wasi: &mut WasiCtx, path: &Path) -> Result<()> {
    add_file_to_wasi_ctx(wasi, path)
}

fn add_file_to_wasi_ctx(wasi: &mut WasiCtx, path: &Path) -> Result<()> {
    let dir = path.parent().unwrap();
    let std_file = File::open(dir)?;
    let wasi_dir = wasi_cap_std_sync::Dir::from_std_file(std_file);
    wasi.push_preopened_dir(
        Box::new(wasi_cap_std_sync::dir::Dir::from_cap_std(wasi_dir)),
        dir,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    static CP_MODULE_PATH: &str = "test/bin/wasm32-wasi/cp.wasm";

    fn create_cp_module() -> Module {
        let engine = create_engine().unwrap();
        create_module(&engine, CP_MODULE_PATH).unwrap()
    }

    #[test]
    fn cp_help() {
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.to_string(),
            args: vec!["-h".to_string()],
            ..Default::default()
        }
        .exec();
        assert!(x.success());
        assert_eq!(x.exit_code, Some(0));
        assert!(std::str::from_utf8(&x.stdout).unwrap().contains("Usage"));
    }

    #[test]
    fn cp() {
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.to_string(),
            args: vec![
                "test/data/a.csv".to_string(),
                "tmp/cp.outfile.tmp".to_string(),
            ],
            input_paths: vec!["test/data/a.csv".to_string()],
            output_paths: vec!["tmp/cp.outfile.tmp".to_string()],
            ..Default::default()
        }
        .exec();

        assert!(x.success());
    }

    #[test]
    fn cp_invalid_file() {
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.to_string(),
            args: vec!["not-existing-file".to_string(), "xxx".to_string()],
            input_paths: vec!["test/data/a.csv".to_string()],
            output_paths: vec!["tmp/cp.outfile.tmp".to_string()],
            ..Default::default()
        }
        .exec();
        assert!(!x.success());
        assert_eq!(x.exit_code, Some(1));
    }

    #[test]
    fn cp_no_preopened_input_file() {
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.to_string(),
            args: vec![
                "test/data/a.csv".to_string(),
                "tmp/cp.outfile.tmp".to_string(),
            ],
            input_paths: vec![],
            output_paths: vec!["tmp/cp.outfile.tmp".to_string()],
            ..Default::default()
        }
        .exec();
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[test]
    fn no_preopened_output_file() {
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.to_string(),
            args: vec![
                "test/data/a.csv".to_string(),
                "tmp/cp.outfile.tmp".to_string(),
            ],
            input_paths: vec!["test/data/a.csv".to_string()],
            output_paths: vec![],
            ..Default::default()
        }
        .exec();
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening output file"));
    }
}
