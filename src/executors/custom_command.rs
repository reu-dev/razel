use crate::config::{RESPONSE_FILE_MIN_ARGS_LEN, RESPONSE_FILE_PREFIX};
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
        match child.wait_with_output().await {
            Ok(output) => {
                if output.status.success() {
                    result.status = ExecutionStatus::Success;
                } else {
                    (result.status, result.error) = Self::evaluate_status(output.status);
                }
                result.exit_code = output.status.code();
                result.stdout = output.stdout;
                result.stderr = output.stderr;
                result.duration = Some(execution_start.elapsed());
            }
            Err(e) => {
                result.status = ExecutionStatus::Failed;
                result.error = Some(e.into());
            }
        }
        self.write_redirect_files(&cwd, &mut result).await;
        result
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
        let mut args_len_sum = 0;
        for x in &self.args {
            args_len_sum += x.len();
            if args_len_sum >= RESPONSE_FILE_MIN_ARGS_LEN {
                return true;
            }
        }
        false
    }

    async fn write_redirect_files(&self, cwd: &Path, result: &mut ExecutionResult) {
        if let Err(e) = Self::maybe_write_file(
            &self.stdout_file.as_ref().map(|x| cwd.join(x)),
            &result.stdout,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStdoutFile;
            result.error = Some(e);
            return;
        }
        if let Err(e) = Self::maybe_write_file(
            &self.stderr_file.as_ref().map(|x| cwd.join(x)),
            &result.stderr,
        )
        .await
        {
            result.status = ExecutionStatus::FailedToWriteStderrFile;
            result.error = Some(e);
        }
    }

    async fn maybe_write_file(path: &Option<PathBuf>, buf: &[u8]) -> Result<(), anyhow::Error> {
        if let Some(path) = path {
            let mut file = tokio::fs::File::create(path).await?;
            file.write_all(buf).await?;
            file.sync_all().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::executors::ExecutionStatus;
    use crate::Razel;

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
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None, None).await;
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
                "./test/data/a.csv".into(), // file exists but is not executable
                vec![],
                Default::default(),
                vec![],
                vec![],
                None,
                None,
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None, None).await;
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
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None, None).await;
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
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None, None).await;
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
            )
            .map(|id| razel.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None, None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
        assert!(result.stdout.is_empty());
        assert!(!result.stderr.is_empty());
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
}
