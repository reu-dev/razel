use crate::types::CacheHit;
use anyhow::{anyhow, Result};
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
    pub fn for_task(result: Result<()>, execution_start: Instant) -> Self {
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
        if let Some(error) = self
            .improve_error_message_stderr()
            .or_else(|| self.improve_error_message_stdout())
        {
            self.error = Some(anyhow!(error));
        }
    }

    fn improve_error_message_stderr(&self) -> Option<String> {
        let lines = std::str::from_utf8(&self.stderr).map(|x| x.lines()).ok()?;
        let last_line = lines.clone().next_back()?;
        let last_line_lc = last_line.to_lowercase();
        if last_line_lc.contains("assertion") || last_line_lc.contains("error") {
            return Some(last_line.to_string());
        }
        let mut iter = lines;
        let mut next = None;
        while let Some(curr) = iter.next_back() {
            if curr.contains("panicked at") {
                // Rust panic
                let error = if let Some(next) = next.filter(|_| curr.ends_with(":")) {
                    format!("{curr} {next}")
                } else {
                    curr.to_string()
                };
                return Some(error);
            }
            next = Some(curr);
        }
        None
    }

    fn improve_error_message_stdout(&self) -> Option<String> {
        let lines = std::str::from_utf8(&self.stdout).map(|x| x.lines()).ok()?;
        let last_line = lines.clone().next_back()?;
        let last_line_lc = last_line.to_lowercase();
        if last_line_lc.contains("error") {
            return Some(last_line.to_string());
        }
        None
    }

    #[cfg(test)]
    pub fn assert_success(&self) {
        if self.success() {
            assert_eq!(self.exit_code, Some(0));
            assert!(self.error.is_none());
        } else {
            assert!(self.error.is_some());
            panic!(
                "assert_success(): status: {:?}, error: {:?}",
                self.status,
                self.error.as_ref().unwrap()
            );
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

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub enum ExecutionStatus {
    #[default]
    NotStarted,
    /// Target could not be started because it depends on a failed condition
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
