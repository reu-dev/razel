use crate::config::RESPONSE_FILE_PREFIX;
use crate::CGroup;
use anyhow::anyhow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::time::Instant;
use tokio::io::AsyncWriteExt;

use crate::executors::{ExecutionResult, ExecutionStatus};

#[derive(Clone, Default)]
pub struct CustomCommandExecutor {
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
    pub stdout_file: Option<PathBuf>,
    pub stderr_file: Option<PathBuf>,
    pub timeout: Option<u16>,
}

impl CustomCommandExecutor {
    pub async fn exec(
        &self,
        sandbox_dir_option: Option<PathBuf>,
        cgroup: Option<CGroup>,
    ) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        let response_file_args = match self.maybe_use_response_file(&sandbox_dir_option).await {
            Ok(Some(x)) => Some(vec![x]),
            Ok(None) => None,
            Err(x) => {
                result.status = ExecutionStatus::FailedToCreateResponseFile;
                result.error = Some(x);
                return result;
            }
        };
        let cwd = sandbox_dir_option.unwrap_or_else(|| ".".into());
        let execution_start = Instant::now();
        let child = match tokio::process::Command::new(&self.executable)
            .env_clear()
            .envs(&self.env)
            .args(response_file_args.as_ref().unwrap_or(&self.args))
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
        if let Some(cgroup) = cgroup {
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
                    (result.status, result.error) = Self::evaluate_status(output.status);
                }
                result.exit_code = output.status.code();
                result.stdout = output.stdout;
                result.stderr = output.stderr;
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

    pub fn args_with_executable(&self) -> Vec<String> {
        [self.executable.clone()]
            .iter()
            .chain(self.args.iter())
            .cloned()
            .collect()
    }

    pub fn command_line_with_redirects(&self) -> Vec<String> {
        [self.executable.clone()]
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

    #[cfg(target_family = "windows")]
    fn evaluate_status(exit_status: ExitStatus) -> (ExecutionStatus, Option<anyhow::Error>) {
        if exit_status.success() {
            (ExecutionStatus::Success, None)
        } else {
            (
                ExecutionStatus::Failed,
                Some(anyhow!("command failed: {}", exit_status)),
            )
        }
    }

    #[cfg(target_family = "unix")]
    fn evaluate_status(exit_status: ExitStatus) -> (ExecutionStatus, Option<anyhow::Error>) {
        use std::os::unix::process::ExitStatusExt;
        if exit_status.success() {
            (ExecutionStatus::Success, None)
        } else if exit_status.core_dumped() {
            (
                ExecutionStatus::Crashed,
                Some(anyhow!(
                    "command crashed with signal {}",
                    exit_status.signal().unwrap()
                )),
            )
        } else if let Some(signal) = exit_status.signal() {
            (
                ExecutionStatus::Killed,
                Some(anyhow!("command terminated by signal {signal}")),
            )
        } else if let Some(signal) = exit_status.stopped_signal() {
            (
                ExecutionStatus::Killed,
                Some(anyhow!("command stopped by {signal}")),
            )
        } else if let Some(exit_code) = exit_status.code() {
            (
                ExecutionStatus::Failed,
                Some(anyhow!("command failed with exit code {exit_code}")),
            )
        } else {
            (
                ExecutionStatus::Failed,
                Some(anyhow!("command failed: {}", exit_status)),
            )
        }
    }

    async fn maybe_use_response_file(
        &self,
        sandbox_dir: &Option<PathBuf>,
    ) -> Result<Option<String>, anyhow::Error> {
        if !self.is_response_file_needed() {
            return Ok(None);
        }
        let file_name = "params";
        let path = sandbox_dir
            .as_ref()
            .ok_or_else(|| anyhow!("Sandbox is required for response file!"))?
            .join(file_name);
        let mut file = tokio::fs::File::create(path).await?;
        file.write_all(self.args.join("\n").as_bytes()).await?;
        file.sync_all().await?;
        Ok(Some(RESPONSE_FILE_PREFIX.to_string() + file_name))
    }

    fn is_response_file_needed(&self) -> bool {
        /* those limits are taken from test_arg_max()
         * TODO replace hardcoded limits with running that check before executing commands */
        let (max_len, terminator_len) = if cfg!(windows) {
            (32_760, 1)
        } else if cfg!(target_os = "macos") {
            (1_048_512, 1 + std::mem::size_of::<usize>())
        } else {
            (2_097_088, 1 + std::mem::size_of::<usize>())
        };
        let mut args_len_sum = 0;
        for x in &self.args {
            args_len_sum += x.len() + terminator_len;
            if args_len_sum >= max_len {
                return true;
            }
        }
        false
    }

    async fn write_redirect_files(&self, cwd: &Path, result: &mut ExecutionResult) {
        if let Err(e) = Self::maybe_write_redirect_file(
            &self.stdout_file.as_ref().map(|x| cwd.join(x)),
            &mut result.stdout,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStdoutFile;
            result.error = Some(e);
            return;
        }
        if let Err(e) = Self::maybe_write_redirect_file(
            &self.stderr_file.as_ref().map(|x| cwd.join(x)),
            &mut result.stderr,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStderrFile;
            result.error = Some(e);
        }
    }

    async fn maybe_write_redirect_file(
        path: &Option<PathBuf>,
        buf: &mut Vec<u8>,
    ) -> Result<(), anyhow::Error> {
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
    use crate::executors::{CustomCommandExecutor, ExecutionStatus};
    use crate::metadata::Tag;
    use crate::Razel;
    use std::path::Path;

    #[tokio::test]
    async fn exec_ok() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "true".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(result.success());
        assert_eq!(result.status, ExecutionStatus::Success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn exec_fail_to_start() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "./examples/data/a.csv".into(), // file exists but is not executable
                vec![],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::FailedToStart);
        assert_eq!(result.exit_code, None);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn exec_failed_to_run() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "false".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn exec_stdout() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-h".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(result.success());
        assert_eq!(result.status, ExecutionStatus::Success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.error.is_none());
        assert!(!result.stdout.is_empty());
        assert!(result.stderr.is_empty());
    }

    #[tokio::test]
    async fn exec_stderr() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "hopefully-not-existing-command".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
        assert!(result.stdout.is_empty());
        assert!(!result.stderr.is_empty());
    }

    #[tokio::test]
    async fn exec_timeout() {
        let mut razel = Razel::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "sleep".into(), "3".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
                vec![],
                vec![Tag::Timeout(1)],
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(Path::new("."), None, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Timeout);
        assert_ne!(result.exit_code, Some(0));
    }

    /* TODO
    #[tokio::test]
    async fn exec_kill() {
        let mut razel = Scheduler::new();
        let command = razel
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "sleep".into(), "10".into()],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec().await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(-1));
        assert!(result.error.is_some());
    }
     */

    #[tokio::test]
    async fn test_arg_max() {
        let mut executor = CustomCommandExecutor {
            executable: "echo".to_string(),
            ..Default::default()
        };
        for arg in &["a", "ab", "abcdefabcdef"] {
            executor.args.clear();
            let mut lower: usize = 0;
            let mut upper: Option<usize> = None;
            let mut current = 2048;
            loop {
                executor.args.resize(current, arg.to_string().clone());
                let result = executor.exec(None, None).await;
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
                (lower - 1) * (arg.len() + 1 + std::mem::size_of::<usize>())
            };
            println!("{arg:>13}: {lower:>7} {max:>7}");
        }
    }
}
