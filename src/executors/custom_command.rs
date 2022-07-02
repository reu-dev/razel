use anyhow::anyhow;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitStatus;

use crate::executors::{ExecutionResult, ExecutionStatus};

#[derive(Clone)]
pub struct CustomCommandExecutor {
    pub executable: String,
    pub args: Vec<String>,
    pub env: HashMap<String, String>,
}

impl CustomCommandExecutor {
    pub async fn exec(&self, sandbox_dir: Option<PathBuf>) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        let mut child = match tokio::process::Command::new(&self.executable)
            .env_clear()
            .envs(&self.env)
            .args(&self.args)
            .current_dir(sandbox_dir.unwrap_or(".".into()))
            .spawn()
        {
            Ok(child) => child,
            Err(e) => {
                result.status = ExecutionStatus::FailedToStart;
                result.error = Some(e.into());
                return result;
            }
        };
        match child.wait().await {
            Ok(exit_status) => {
                if exit_status.success() {
                    result.status = ExecutionStatus::Success;
                } else {
                    result.status = ExecutionStatus::Failed;
                    result.error = Some(Self::handle_error(exit_status));
                }
                result.exit_code = exit_status.code();
            }
            Err(e) => {
                result.status = ExecutionStatus::Failed;
                result.error = Some(e.into());
            }
        }
        result
    }

    pub fn args_with_executable(&self) -> Vec<String> {
        [self.executable.clone()]
            .iter()
            .chain(self.args.iter())
            .cloned()
            .collect()
    }

    #[cfg(target_family = "windows")]
    fn handle_error(exit_status: ExitStatus) -> anyhow::Error {
        anyhow!("command failed: {}", exit_status)
    }

    #[cfg(target_family = "unix")]
    fn handle_error(exit_status: ExitStatus) -> anyhow::Error {
        use std::os::unix::process::ExitStatusExt;
        if exit_status.core_dumped() {
            anyhow!(
                "command crashed with signal {}",
                exit_status.signal().unwrap()
            )
        } else if let Some(signal) = exit_status.signal() {
            anyhow!("command terminated by signal {signal}")
        } else if let Some(signal) = exit_status.stopped_signal() {
            anyhow!("command stopped by {signal}")
        } else if let Some(exit_code) = exit_status.code() {
            anyhow!("command failed with exit code {exit_code}")
        } else {
            anyhow!("command failed: {}", exit_status)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::executors::ExecutionStatus;
    use crate::Scheduler;

    #[tokio::test]
    async fn exec_ok() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "true".into()],
                Default::default(),
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap()
            .clone();
        let result = command.executor.exec(None).await;
        assert!(result.success());
        assert_eq!(result.status, ExecutionStatus::Success);
        assert_eq!(result.exit_code, Some(0));
        assert!(result.error.is_none());
    }

    #[tokio::test]
    async fn exec_fail_to_start() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "./hopefully-not-existing-command-to-test-razel".into(),
                vec![],
                Default::default(),
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::FailedToStart);
        assert_eq!(result.exit_code, None);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn exec_failed_to_run() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "false".into()],
                Default::default(),
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec(None).await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(1));
        assert!(result.error.is_some());
    }

    /* TODO
    #[tokio::test]
    async fn exec_kill() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "sleep".into(), "10".into()],
                Default::default(),
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap();
        let result = command.executor.exec().await;
        assert!(!result.success());
        assert_eq!(result.status, ExecutionStatus::Failed);
        assert_eq!(result.exit_code, Some(-1));
        assert!(result.error.is_some());
    }
     */
}
