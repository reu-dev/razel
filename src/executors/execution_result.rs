use crate::CacheHit;
use anyhow::{anyhow, Error};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;
use std::time::Instant;

#[derive(Default)]
pub struct ExecutionResult {
    pub status: ExecutionStatus,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub error: Option<anyhow::Error>,
    pub cache_hit: Option<CacheHit>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    /// original execution duration of the command/task - ignoring cache
    pub exec_duration: Option<Duration>,
    /// actual duration of processing the command/task - including caching and overheads
    pub total_duration: Option<Duration>,
}

impl ExecutionResult {
    pub fn for_task(result: Result<(), Error>, execution_start: Instant) -> Self {
        let exec_duration = Some(execution_start.elapsed());
        match result {
            Ok(()) => Self {
                status: ExecutionStatus::Success,
                exit_code: Some(0),
                exec_duration,
                ..Default::default()
            },
            Err(e) => Self {
                status: ExecutionStatus::Failed,
                error: Some(e),
                exec_duration,
                ..Default::default()
            },
        }
    }

    pub fn success(&self) -> bool {
        self.status == ExecutionStatus::Success
    }

    pub fn out_of_memory_killed(&self) -> bool {
        self.status == ExecutionStatus::Crashed && self.signal == Some(9)
    }

    pub fn improve_error_message(&mut self) {
        let Ok(stderr) = std::str::from_utf8(&self.stderr).map(|x| x.lines()) else {
            return;
        };
        let Some(last_line) = stderr.clone().next_back() else {
            return;
        };
        if last_line.contains("Assertion") {
            // C/C++ assertion failed
            self.error = Some(anyhow!("{last_line}"));
            return;
        }
        let mut iter = stderr;
        let mut next = None;
        while let Some(curr) = iter.next_back() {
            if curr.contains("panicked at") {
                // Rust panic
                let error = if let Some(next) = next.filter(|_| curr.ends_with(":")) {
                    format!("{curr} {next}")
                } else {
                    curr.to_string()
                };
                self.error = Some(anyhow!(error));
                return;
            }
            next = Some(curr);
        }
    }

    #[cfg(test)]
    pub fn assert_success(&mut self) {
        use anyhow::Context;
        if self.success() {
            assert_eq!(self.exit_code, Some(0));
            assert!(self.error.is_none());
        } else {
            assert!(self.error.is_some());
            Err::<(), Error>(self.error.take().unwrap())
                .context(format!("Status: {:?}", self.status))
                .unwrap();
        }
    }
}

impl fmt::Debug for ExecutionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:?} ({:?}), stdout: '{}', stderr: '{}'",
            self.status,
            self.exit_code,
            std::str::from_utf8(&self.stdout)
                .unwrap()
                .replace('\n', "\\n"),
            std::str::from_utf8(&self.stderr)
                .unwrap()
                .replace('\n', "\\n"),
        )
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum ExecutionStatus {
    NotStarted,
    /// Command could not be started because it depends on a failed condition
    Skipped,
    FailedToStart,
    FailedToCreateResponseFile,
    FailedToWriteStdoutFile,
    FailedToWriteStderrFile,
    Failed,
    /// core dumped or terminated by signal
    Crashed,
    Timeout,
    Success,
    /// not command related error, e.g. cache, sandbox
    SystemError,
}

impl Default for ExecutionStatus {
    fn default() -> Self {
        Self::NotStarted
    }
}
