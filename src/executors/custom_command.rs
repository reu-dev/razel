use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;

use anyhow::anyhow;
use itertools::{chain, join};

use crate::executors::{ExecutionResult, ExecutionStatus};

#[derive(Clone)]
pub struct CustomCommandExecutor {
    pub executable: String,
    pub args: Vec<String>,
}

impl CustomCommandExecutor {
    pub async fn exec(&self, sandbox_dir: Option<PathBuf>) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        // TODO let program = fs::canonicalize(&self.executable).unwrap();
        let mut child = match tokio::process::Command::new(&self.executable)
            // TODO .env_clear()
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
                    if exit_status.core_dumped() {
                        result.error = Some(anyhow!(
                            "command crashed with signal {}",
                            exit_status.signal().unwrap()
                        ));
                    } else if let Some(signal) = exit_status.signal() {
                        result.error = Some(anyhow!("command terminated by signal {signal}"));
                    } else if let Some(signal) = exit_status.stopped_signal() {
                        result.error = Some(anyhow!("command stopped by {signal}"));
                    } else if let Some(exit_code) = exit_status.code() {
                        result.error = Some(anyhow!("command failed with exit code {exit_code}"));
                    } else {
                        result.error = Some(anyhow!("command failed"));
                    }
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

    pub fn command_line(&self) -> String {
        join(
            chain([self.executable.clone()].iter(), self.args.iter()),
            " ",
        )
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
                "hopefully-not-existing-command-to-test-razel".into(),
                vec![],
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
