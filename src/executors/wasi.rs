use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::{config, FileId};
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Instant;
use wasmtime::component::ResourceTable;
use wasmtime::{Config, Engine, Linker, Module, Store};
use wasmtime_wasi::preview2::preview1::WasiPreview1Adapter;
use wasmtime_wasi::preview2::{
    pipe::MemoryOutputPipe, DirPerms, FilePerms, I32Exit, WasiCtx, WasiCtxBuilder,
};

struct Ctx {
    table: ResourceTable,
    wasi: WasiCtx,
    adapter: WasiPreview1Adapter,
}

impl wasmtime_wasi::preview2::WasiView for Ctx {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.wasi
    }
}

impl wasmtime_wasi::preview2::preview1::WasiPreview1View for Ctx {
    fn adapter(&self) -> &WasiPreview1Adapter {
        &self.adapter
    }
    fn adapter_mut(&mut self) -> &mut WasiPreview1Adapter {
        &mut self.adapter
    }
}

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
        config.async_support(true);
        config.cranelift_nan_canonicalization(true);
        let engine = Engine::new(&config).context("create WASM engine")?;
        Ok(engine)
    }

    pub fn create_module(engine: &Engine, file: impl AsRef<Path>) -> Result<Module> {
        Module::from_file(engine, &file)
            .with_context(|| format!("create WASM module: {:?}", file.as_ref()))
    }

    pub async fn exec(&self, sandbox_dir: &Path) -> ExecutionResult {
        match self.wasi_exec(sandbox_dir).await {
            Ok(execution_result) => execution_result,
            Err(error) => ExecutionResult {
                status: ExecutionStatus::FailedToStart,
                error: Some(error),
                ..Default::default()
            },
        }
    }

    async fn wasi_exec(&self, sandbox_dir: &Path) -> Result<ExecutionResult> {
        assert!(self.module.is_some());
        let engine = self.module.as_ref().unwrap().engine();
        let mut linker = Linker::new(engine);
        wasmtime_wasi::preview2::preview1::add_to_linker_async(&mut linker)?;

        let (ctx, stdout, stderr) = self
            .create_wasi_ctx(sandbox_dir)
            .with_context(|| format!("create_wasi_ctx() sandbox_dir: {sandbox_dir:?}"))?;
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
        sandbox_dir: &Path,
    ) -> Result<(Ctx, MemoryOutputPipe, MemoryOutputPipe)> {
        let stdout = MemoryOutputPipe::new(4096);
        let stderr = MemoryOutputPipe::new(4096);
        let mut builder = WasiCtxBuilder::new();
        builder.stdout(stdout.clone()).stderr(stderr.clone());
        builder.arg(&self.executable);
        for arg in &self.args {
            builder.arg(arg);
        }
        for (k, v) in &self.env {
            builder.env(k, v);
        }
        let preopen_dir =
            cap_std::fs::Dir::open_ambient_dir(sandbox_dir, cap_std::ambient_authority())
                .with_context(|| format!("Add sandbox dir to WASI ctx: {sandbox_dir:?}"))?;
        builder.preopened_dir(preopen_dir, DirPerms::all(), FilePerms::all(), ".");
        let ctx = Ctx {
            table: ResourceTable::new(),
            wasi: builder.build(),
            adapter: WasiPreview1Adapter::new(),
        };
        Ok((ctx, stdout, stderr))
    }
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
        let sandbox_dir = new_tmp_dir!();
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec!["-h".into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(x.success());
        assert_eq!(x.exit_code, Some(0));
        assert!(std::str::from_utf8(&x.stdout).unwrap().contains("Usage"));
    }

    #[tokio::test]
    async fn cp() {
        let sandbox_dir = new_tmp_dir!();
        let src = sandbox_dir.join_and_write_file(SRC_PATH, SOURCE_CONTENTS);
        let dst = sandbox_dir.join(DST_PATH);
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), DST_PATH.into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(x.success());
        ensure_equal(src, dst).unwrap();
    }

    #[tokio::test]
    async fn cp_not_existing_input_file() {
        let sandbox_dir = new_tmp_dir!();
        // not writing source file
        let x = WasiExecutor {
            module: Some(create_cp_module()),
            executable: CP_MODULE_PATH.into(),
            args: vec![SRC_PATH.into(), DST_PATH.into()],
            ..Default::default()
        }
        .exec(sandbox_dir.dir())
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
        .exec(sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening input file"));
    }

    #[tokio::test]
    async fn cp_write_outside_sandbox() {
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
        .exec(sandbox_dir.dir())
        .await;
        println!("{x:?}");
        assert!(!x.success());
        assert!(std::str::from_utf8(&x.stderr)
            .unwrap()
            .contains("error opening output file"));
    }
}
