use crate::config::RESPONSE_FILE_PREFIX;
use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::types::CommandTarget;
use crate::{CGroup, SandboxDir};
use anyhow::{anyhow, ensure, Result};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Instant;
use tokio::io::AsyncWriteExt;

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

    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        let response_file_args = match self.maybe_use_response_file(sandbox_dir).await {
            Ok(Some(x)) => Some(vec![x]),
            Ok(None) => None,
            Err(x) => {
                result.status = ExecutionStatus::FailedToCreateResponseFile;
                result.error = Some(x);
                return result;
            }
        };
        let cwd = sandbox_dir.dir.clone().unwrap_or_else(|| ".".into());
        let execution_start = Instant::now();
        let child = match tokio::process::Command::new(&self.command.executable)
            .env_clear()
            .envs(&self.command.env)
            .args(response_file_args.as_ref().unwrap_or(&self.command.args))
            .current_dir(&cwd)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                result.status = ExecutionStatus::FailedToStart;
                result.error = Some(e.into());
                return result;
            }
        };
        if let Some(cgroup) = &self.cgroup {
            cgroup.add_task("memory", child.id().unwrap()).ok();
        }
        let (exec_result, timed_out) = self.wait_with_timeout(child).await;
        match exec_result {
            Ok(output) => {
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
                result.error = Some(e.into());
            }
        }
        result.exec_duration = Some(execution_start.elapsed());
        self.write_redirect_files(&cwd, &mut result).await;
        result
    }

    async fn wait_with_timeout(
        &self,
        mut child: tokio::process::Child,
    ) -> (std::io::Result<std::process::Output>, bool) {
        let timed_out = if let Some(timeout_s) = self.timeout {
            let sleep = tokio::time::sleep(std::time::Duration::from_secs(timeout_s.into()));
            tokio::pin!(sleep);
            tokio::select! {
                _ = child.wait() => {
                    false
                }
                _ = &mut sleep => {
                    let _ = child.kill().await;
                    true
                }
            }
        } else {
            false
        };
        (child.wait_with_output().await, timed_out)
    }

    #[cfg(target_family = "windows")]
    fn evaluate_status(
        exit_status: ExitStatus,
    ) -> (ExecutionStatus, Option<i32>, Option<anyhow::Error>) {
        if exit_status.success() {
            (ExecutionStatus::Success, None, None)
        } else {
            (
                ExecutionStatus::Failed,
                None,
                Some(anyhow!("command failed: {}", exit_status)),
            )
        }
    }

    #[cfg(target_family = "unix")]
    fn evaluate_status(
        exit_status: ExitStatus,
    ) -> (ExecutionStatus, Option<i32>, Option<anyhow::Error>) {
        use std::os::unix::process::ExitStatusExt;
        if exit_status.success() {
            (ExecutionStatus::Success, None, None)
        } else if exit_status.core_dumped() {
            let signal = exit_status.signal().unwrap();
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(anyhow!("command core dumped with signal {signal}")),
            )
        } else if let Some(signal) = exit_status.stopped_signal() {
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(anyhow!("command stopped by signal {signal}")),
            )
        } else if let Some(signal) = exit_status.signal() {
            (
                ExecutionStatus::Crashed,
                Some(signal),
                Some(anyhow!("command terminated by signal {signal}")),
            )
        } else if let Some(exit_code) = exit_status.code() {
            (
                ExecutionStatus::Failed,
                None,
                Some(anyhow!("command failed with exit code {exit_code}")),
            )
        } else {
            (
                ExecutionStatus::Failed,
                None,
                Some(anyhow!("command failed: {}", exit_status)),
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
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(self.command.args.join("\n").as_bytes())
            .await?;
        file.sync_all().await?;
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
            result.error = Some(e);
            return;
        }
        if let Err(e) = Self::maybe_write_redirect_file(
            &self.command.stderr_file.as_ref().map(|x| cwd.join(x)),
            &mut result.stderr,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStderrFile;
            result.error = Some(e);
        }
    }

    async fn maybe_write_redirect_file(path: &Option<PathBuf>, buf: &mut Vec<u8>) -> Result<()> {
        if let Some(path) = path {
            let mut file = tokio::fs::File::create(path).await?;
            file.write_all(buf).await?;
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
