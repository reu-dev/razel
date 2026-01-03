use crate::cache::{Cache, MessageDigest};
use crate::executors::{ExecutionResult, ExecutionStatus, Executor};
use crate::types::{Digest, ExecutableType, File};
use crate::{BoxedSandbox, SandboxDir, bazel_remote_exec, force_remove_file, is_file_executable};
use anyhow::{Context, Result, bail};
use std::path::Path;
use std::time::Duration;

pub async fn get_execution_result_from_cache(
    action_digest: &MessageDigest,
    cache: Option<&mut Cache>,
    use_remote_cache: bool,
    output_files: &mut [File],
) -> Option<ExecutionResult> {
    let cache = cache?;
    if let Some((action_result, cache_hit)) = cache
        .get_action_result(action_digest, use_remote_cache)
        .await
    {
        let exit_code = Some(action_result.exit_code);
        let metadata = action_result.execution_metadata.as_ref();
        let execution_result = ExecutionResult {
            status: ExecutionStatus::Success,
            exit_code,
            signal: None,
            error: None,
            cache_hit: Some(cache_hit),
            stdout: action_result.stdout_raw,
            stderr: action_result.stderr_raw,
            exec_duration: metadata
                .and_then(|x| x.virtual_execution_duration.as_ref())
                .map(|x| Duration::new(x.seconds as u64, x.nanos as u32)),
            total_duration: None,
        };
        for result_file in action_result.output_files {
            let Some(file) = output_files
                .iter_mut()
                .find(|f| f.path.to_str().unwrap() == result_file.path)
            else {
                panic!("unexpected file in action_result: {:?}", result_file.path);
            };
            file.digest = Some(result_file.digest.unwrap().into());
            set_file_executable(file, result_file.is_executable);
        }
        return Some(execution_result);
    }
    None
}

pub async fn exec_action_with_sandbox(
    action_digest: &Digest,
    cache: Option<&mut Cache>,
    use_remote_cache: bool,
    executor: &Executor,
    sandbox: BoxedSandbox,
    output_files: &mut Vec<File>,
    cwd: &Path,
) -> Result<ExecutionResult> {
    sandbox
        .create(
            &output_files
                .iter()
                .map(|f| f.path.clone())
                .collect::<Vec<_>>(),
        )
        .await
        .context("Sandbox::create()")?;
    let sandbox_dir = sandbox.dir();
    let execution_result = executor.exec(cwd, &sandbox_dir).await;
    if execution_result.success() {
        set_output_files_digest(output_files, &sandbox_dir).await?;
        if let Some(cache) = cache {
            cache_action_result(
                action_digest,
                &execution_result,
                output_files,
                &sandbox_dir,
                cache,
                use_remote_cache,
            )
            .await
            .with_context(|| "cache_action_result()")?;
        } else {
            sandbox
                .move_output_files_into_out_dir(
                    &output_files
                        .iter()
                        .map(|f| f.path.clone())
                        .collect::<Vec<_>>(),
                )
                .await?;
        }
    }
    sandbox
        .destroy()
        .await
        .with_context(|| "Sandbox::destroy()")?;
    Ok(execution_result)
}

//#[allow(clippy::too_many_arguments)]
pub async fn exec_action_without_sandbox(
    action_digest: &MessageDigest,
    cache: Option<&mut Cache>,
    use_remote_cache: bool,
    executor: &Executor,
    output_files: &mut Vec<File>,
    cwd: &Path,
) -> Result<ExecutionResult> {
    // remove expected output files, because symlinks will not be overwritten
    for file in output_files.iter() {
        force_remove_file(&file.path).await?;
    }
    let sandbox_dir = SandboxDir::new(None);
    let execution_result = executor.exec(cwd, &sandbox_dir).await;
    if execution_result.success() {
        set_output_files_digest(output_files, &sandbox_dir).await?;
        if let Some(cache) = cache {
            cache_action_result(
                action_digest,
                &execution_result,
                output_files,
                &sandbox_dir,
                cache,
                use_remote_cache,
            )
            .await
            .with_context(|| "cache_action_result()")?;
        }
    }
    Ok(execution_result)
}

async fn cache_action_result(
    action_digest: &MessageDigest,
    execution_result: &ExecutionResult,
    output_files: &[File],
    sandbox_dir: &SandboxDir,
    cache: &mut Cache,
    use_remote_cache: bool,
) -> Result<()> {
    assert!(execution_result.success());
    let mut action_result = bazel_remote_exec::ActionResult {
        output_files: output_files
            .iter()
            .map(|f| bazel_remote_exec::OutputFile {
                path: f.path.to_str().unwrap().into(),
                digest: Some(f.digest.as_ref().unwrap().into()),
                is_executable: f.executable.is_some(),
                ..Default::default()
            })
            .collect(),
        exit_code: execution_result.exit_code.unwrap_or_default(),
        execution_metadata: Some(bazel_remote_exec::ExecutedActionMetadata {
            virtual_execution_duration: execution_result.exec_duration.map(|x| {
                bazel_remote_exec::Duration {
                    seconds: x.as_secs() as i64,
                    nanos: x.subsec_nanos() as i32,
                }
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    // TODO add stdout/stderr files for non-small outputs
    action_result.stdout_raw = execution_result.stdout.clone();
    action_result.stderr_raw = execution_result.stderr.clone();
    cache
        .push(action_digest, &action_result, sandbox_dir, use_remote_cache)
        .await?;
    Ok(())
}

async fn set_output_files_digest(files: &mut Vec<File>, sandbox_dir: &SandboxDir) -> Result<()> {
    for file in files {
        set_output_file_digest(file, sandbox_dir).await?
    }
    Ok(())
}

async fn set_output_file_digest(file: &mut File, sandbox_dir: &SandboxDir) -> Result<()> {
    let src = sandbox_dir.join(&file.path);
    if src.is_symlink() {
        bail!("Output file must not be a symlink: {:?}", src);
    }
    let fs_file = tokio::fs::File::open(&src)
        .await
        .with_context(|| format!("Failed to open: {src:?}"))?;
    set_file_executable(
        file,
        is_file_executable(&fs_file)
            .await
            .with_context(|| format!("is_file_executable(): {src:?}"))?,
    );
    file.digest = Some(
        Digest::for_file(fs_file)
            .await
            .with_context(|| format!("Digest::for_file(): {src:?}"))?,
    );
    Ok(())
}

pub fn set_file_executable(file: &mut File, flag: bool) {
    file.executable = match flag {
        true if file.path.ends_with(".wasm") => Some(ExecutableType::WasiModule),
        true => Some(ExecutableType::ExecutableInWorkspace),
        false => None,
    };
}
