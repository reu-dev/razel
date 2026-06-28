use crate::config::RESPONSE_FILE_PREFIX;
use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::types::CommandTarget;
use crate::{CGroup, SandboxDir};
use anyhow::{Result, ensure};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Output, Stdio};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;
use tracing::instrument;

static SYSTEM_EXECUTABLE_CACHE: LazyLock<Mutex<HashMap<String, Option<PathBuf>>>> =
    LazyLock::new(Default::default);

/// Resolve `executable` to the program to spawn. A bare name is looked up on `PATH`; anything else is used as-is.
async fn resolve_program(executable: &str) -> Option<PathBuf> {
    if Path::new(executable).components().count() != 1 {
        return Some(PathBuf::from(executable));
    }
    if let Some(hit) = SYSTEM_EXECUTABLE_CACHE.lock().unwrap().get(executable) {
        return hit.clone();
    }
    let name = executable.to_string();
    let resolved = tokio::task::spawn_blocking(move || which::which(&name).ok())
        .await
        .unwrap();
    SYSTEM_EXECUTABLE_CACHE
        .lock()
        .unwrap()
        .insert(executable.to_string(), resolved.clone());
    resolved
}

pub struct CommandExecutor {
    command: CommandTarget,
    timeout: Option<u16>,
    cgroup: Option<CGroup>,
}

impl CommandExecutor {
    pub fn new(
        command: CommandTarget,
        timeout: Option<u16>,
        cgroup: Option<CGroup>,
    ) -> CommandExecutor {
        CommandExecutor {
            command,
            timeout,
            cgroup,
        }
    }

    #[instrument(skip_all)]
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        let response_file_args = match self.maybe_use_response_file(sandbox_dir).await {
            Ok(Some(x)) => Some(vec![x]),
            Ok(None) => None,
            Err(x) => {
                result.status = ExecutionStatus::FailedToCreateResponseFile;
                result.error = Some(x.to_string());
                return result;
            }
        };
        let args = response_file_args.as_ref().unwrap_or(&self.command.args);
        let cwd = sandbox_dir.dir.clone().unwrap_or_else(|| ".".into());
        tracing::trace!(?sandbox_dir, ?args, ?cwd);
        let execution_start = Instant::now();
        let Some(program) = resolve_program(&self.command.executable).await else {
            result.status = ExecutionStatus::FailedToStart;
            result.error = Some(format!(
                "executable not found: {:?}",
                self.command.executable
            ));
            return result;
        };
        let child = match tokio::process::Command::new(&program)
            .env_clear()
            .envs(&self.command.env)
            .args(args)
            .current_dir(&cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                result.status = ExecutionStatus::FailedToStart;
                result.error = Some(e.to_string());
                return result;
            }
        };
        if let Some(cgroup) = &self.cgroup {
            cgroup.add_task("memory", child.id().unwrap()).ok();
        }
        match self.wait_with_timeout(child).await {
            Ok((output, timed_out)) => {
                if output.status.success() {
                    result.status = ExecutionStatus::Success;
                } else if timed_out {
                    result.status = ExecutionStatus::Timeout;
                } else {
                    (result.status, result.signal, result.error) =
                        Self::evaluate_status(output.status);
                }
                result.exit_code = output.status.code();
                result.stdout = output.stdout;
                result.stderr = output.stderr;
                if !result.success() {
                    result.improve_error_message();
                }
            }
            Err(e) => {
                result.status = ExecutionStatus::Failed;
                result.error = Some(e.to_string());
            }
        }
        result.exec_duration = Some(execution_start.elapsed());
        self.write_redirect_files(&cwd, &mut result).await;
        result
    }

    async fn wait_with_timeout(
        &self,
        mut child: tokio::process::Child,
    ) -> std::io::Result<(Output, bool)> {
        let Some(timeout_s) = self.timeout else {
            return child.wait_with_output().await.map(|output| (output, false));
        };

        // With a timeout, stdout/stderr must be drained while waiting, or a fast
        // child with enough output can block on full pipes until the timeout fires.
        fn read_to_end<A: AsyncRead + Unpin + 'static + Send>(
            io: Option<A>,
        ) -> JoinHandle<std::io::Result<Vec<u8>>> {
            tokio::spawn(async move {
                let mut vec = Vec::new();
                if let Some(mut io) = io {
                    io.read_to_end(&mut vec).await?;
                }
                Ok(vec)
            })
        }

        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();
        let stdout_fut = read_to_end(stdout_pipe);
        let stderr_fut = read_to_end(stderr_pipe);
        let (status, timed_out) =
            match tokio::time::timeout(Duration::from_secs(timeout_s.into()), child.wait()).await {
                Ok(status) => (status?, false),
                Err(_) => {
                    child.start_kill()?;
                    (child.wait().await?, true)
                }
            };
        let stdout = stdout_fut
            .await
            .map_err(|e| std::io::Error::other(format!("failed to read stdout: {e}")))??;
        let stderr = stderr_fut
            .await
            .map_err(|e| std::io::Error::other(format!("failed to read stderr: {e}")))??;
        Ok((
            Output {
                status,
                stdout,
                stderr,
            },
            timed_out,
        ))
    }

    #[cfg(target_family = "windows")]
    fn evaluate_status(exit_status: ExitStatus) -> (ExecutionStatus, Option<i32>, Option<String>) {
        if exit_status.success() {
            (ExecutionStatus::Success, None, None)
        } else {
            (
                ExecutionStatus::Failed,
                None,
                Some(format!("command failed: {exit_status}")),
            )
        }
    }

    #[cfg(target_family = "unix")]
    fn evaluate_status(exit_status: ExitStatus) -> (ExecutionStatus, Option<i32>, Option<String>) {
        use std::os::unix::process::ExitStatusExt;
        if exit_status.success() {
            (ExecutionStatus::Success, None, None)
        } else if exit_status.core_dumped() {
            let signal = exit_status.signal().unwrap();
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(format!("command core dumped with signal {signal}")),
            )
        } else if let Some(signal) = exit_status.stopped_signal() {
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(format!("command stopped by signal {signal}")),
            )
        } else if let Some(signal) = exit_status.signal() {
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(format!("command terminated by signal {signal}")),
            )
        } else if let Some(exit_code) = exit_status.code() {
            (
                ExecutionStatus::Failed,
                None,
                Some(format!("command failed with exit code {exit_code}")),
            )
        } else {
            (
                ExecutionStatus::Failed,
                None,
                Some(format!("command failed: {exit_status}")),
            )
        }
    }

    async fn maybe_use_response_file(&self, sandbox_dir: &SandboxDir) -> Result<Option<String>> {
        if !self.is_response_file_needed() {
            return Ok(None);
        }
        let file_name = "params";
        ensure!(
            sandbox_dir.dir.is_some(),
            "Sandbox is required for response file!"
        );
        let path = sandbox_dir.join(&file_name);
        tokio::fs::write(path, self.command.args.join("\n").as_bytes()).await?;
        Ok(Some(RESPONSE_FILE_PREFIX.to_string() + file_name))
    }

    fn is_response_file_needed(&self) -> bool {
        /* those limits are taken from test_arg_max()
         * TODO replace hardcoded limits with running that check before executing commands */
        let (max_len, terminator_len) = if cfg!(windows) {
            (32_760, 1)
        } else if cfg!(target_os = "macos") {
            (1_048_512, 1 + size_of::<usize>())
        } else {
            (2_097_088, 1 + size_of::<usize>())
        };
        let mut args_len_sum = 0;
        for x in &self.command.args {
            args_len_sum += x.len() + terminator_len;
            if args_len_sum >= max_len {
                return true;
            }
        }
        false
    }

    async fn write_redirect_files(&self, cwd: &Path, result: &mut ExecutionResult) {
        if let Err(e) = Self::maybe_write_redirect_file(
            &self.command.stdout_file.as_ref().map(|x| cwd.join(x)),
            &mut result.stdout,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStdoutFile;
            result.error = Some(e.to_string());
            return;
        }
        if let Err(e) = Self::maybe_write_redirect_file(
            &self.command.stderr_file.as_ref().map(|x| cwd.join(x)),
            &mut result.stderr,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStderrFile;
            result.error = Some(e.to_string());
        }
    }

    async fn maybe_write_redirect_file(path: &Option<PathBuf>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(path) = path {
            let mut file = tokio::fs::File::create(path).await?;
            file.write_all(buf).await?;
            file.flush().await?;
            file.sync_all().await?;
            buf.clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::{CommandExecutor, ExecutionStatus};
    use crate::types::CommandTarget;
    use which::which;

    async fn exec_basic(executable: &str, args: Vec<String>) -> ExecutionResult {
        exec_target(
            CommandTarget {
                executable: executable.into(),
                args,
                ..Default::default()
            },
            None,
            None,
        )
        .await
    }

    async fn exec_target(
        command: CommandTarget,
        timeout: Option<u16>,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        let executor = CommandExecutor::new(command, timeout, cgroup);
        executor.exec(&None.into()).await
    }

    #[tokio::test]
    async fn exec_ok() {
        let cmake = which("cmake").unwrap().to_str().unwrap().to_string();
        let result = exec_basic(&cmake, vec!["-E".into(), "true".into()]).await;
        result.assert_success();
    }

    #[tokio::test]
    async fn exec_fail_to_start() {
        let result = exec_basic(
            "./examples/data/a.csv", // file exists but is not executable
            vec![],
        )
        .await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::FailedToStart);
        assert_eq!(result.exit_code, None);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn exec_failed_to_run() {
        let cmake = which("cmake").unwrap().to_str().unwrap().to_string();
        let result = exec_basic(&cmake, vec!["-E".into(), "false".into()]).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn exec_stdout() {
        let echo = which("echo").unwrap().to_str().unwrap().to_string();
        let result = exec_basic(&echo, vec!["Hello".into()]).await;
        result.assert_success();
        assert!(result.error.is_none());
        assert!(!result.stdout.is_empty());
        assert!(result.stderr.is_empty());
    }

    #[tokio::test]
    async fn exec_stderr() {
        let cmake = which("cmake").unwrap().to_str().unwrap().to_string();
        let result = exec_basic(
            &cmake,
            vec!["-E".into(), "hopefully-not-existing-command".into()],
        )
        .await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
        assert!(result.stdout.is_empty());
        assert!(!result.stderr.is_empty());
    }

    #[tokio::test]
    async fn exec_timeout() {
        let command = CommandTarget {
            executable: which("cmake").unwrap().to_str().unwrap().to_string(),
            args: vec!["-E".into(), "sleep".into(), "3".into()],
            ..Default::default()
        };
        let timeout = Some(1);
        let result = exec_target(command, timeout, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Timeout);
        assert_ne!(result.exit_code, Some(0));
    }

    /* TODO
    #[tokio::test]
    async fn exec_kill() {
        let cmake = which("cmake").unwrap().to_str().unwrap().to_string();
        let result = exec_basic(
            &cmake,
            vec!["-E".into(), "sleep".into(), "10".into()],
            Default::default(),
        )
        .await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(-1));
        assert!(result.error.is_some());
    }
     */

    #[tokio::test]
    async fn test_arg_max() {
        let echo = which("echo").unwrap().to_str().unwrap().to_string();
        for arg in &["a", "ab", "abcdefabcdef"] {
            let mut args = vec![];
            let mut lower: usize = 0;
            let mut upper: Option<usize> = None;
            let mut current = 2048;
            loop {
                args.resize(current, arg.to_string());
                let result = exec_basic(&echo, args.clone()).await;
                if result.success() {
                    lower = current;
                } else {
                    upper = Some(current);
                }
                let new_len = upper.map(|x| (lower + x) / 2).unwrap_or_else(|| lower * 2);
                if new_len == current {
                    break;
                }
                current = new_len;
            }
            let max = if cfg!(windows) {
                // add terminator for all args
                (lower - 1) * (arg.len() + 1)
            } else {
                // add terminator and pointer for all args
                (lower - 1) * (arg.len() + 1 + size_of::<usize>())
            };
            println!("{arg:>13}: {lower:>7} {max:>7}");
        }
    }
}
