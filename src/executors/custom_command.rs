use crate::executors::{ExecutionResult, ExecutionStatus};
use anyhow::anyhow;
use itertools::{chain, join};
use std::os::unix::process::ExitStatusExt;

#[derive(Clone)]
pub struct CustomCommandExecutor {
    pub executable: String,
    pub args: Vec<String>,
}

impl CustomCommandExecutor {
    pub async fn exec(&self) -> ExecutionResult {
        let mut result: ExecutionResult = Default::default();
        // TODO add sandbox dir
        let mut child = match tokio::process::Command::new(&self.executable)
            .env_clear()
            .args(&self.args)
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
                    if let Some(signal) = exit_status.signal() {
                        result.error = Some(anyhow!("command failed (signal {signal})"));
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
    use crate::Scheduler;

    #[tokio::test]
    async fn exec_ok() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-h".into()],
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap()
            .clone();
        command.exec().await.unwrap();
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
        command.exec().await.unwrap_err();
    }

    #[tokio::test]
    async fn exec_failed_to_run() {
        let mut scheduler = Scheduler::new();
        let command = scheduler
            .push_custom_command(
                "test".into(),
                "cmake".into(),
                vec!["-E".into(), "not-existing-command".into()],
                vec![],
                vec![],
            )
            .map(|id| scheduler.get_command(id).unwrap())
            .unwrap();
        command.exec().await.unwrap_err();
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
        command.exec().await.unwrap_err();
    }
     */
}
