use crate::executors::ExecutionResult;
use crate::{Command, SchedulerStats};
use bstr::ByteSlice;
use crossterm::cursor::{RestorePosition, SavePosition};
use crossterm::style::{Attribute, Color, SetForegroundColor};
use crossterm::terminal;
use itertools::Itertools;
use std::io::{stdout, Write};

/// Terminal user interface
pub struct TUI {
    verbose: bool,
    status_printed: bool,
}

impl TUI {
    pub fn new() -> Self {
        Self {
            verbose: false,
            status_printed: false,
        }
    }

    pub fn command_succeeded(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.clear_status();
        Self::field(
            format!("{:?} ", execution_result.status).as_str(),
            Color::Green,
            command.name.as_str(),
        );
        if self.verbose {
            Self::field(
                "stderr:\n",
                Color::Blue,
                &execution_result.stderr.to_str_lossy(),
            );
            Self::field(
                "stdout:\n",
                Color::Blue,
                &execution_result.stdout.to_str_lossy(),
            );
        }
    }

    pub fn command_retry(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.clear_status();
        Self::field(
            format!("{:?} (will retry) ", execution_result.status).as_str(),
            Color::Yellow,
            command.name.as_str(),
        );
    }

    pub fn command_failed(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.clear_status();
        println!();
        Self::line();
        Self::field(
            format!("{:<11}", format!("{:?}", execution_result.status)).as_str(),
            Color::Red,
            command.name.as_str(),
        );
        if let Some(x) = &execution_result.error {
            Self::field("error:     ", Color::Red, format!("{}", x).as_str());
        } else if let Some(x) = execution_result.exit_code {
            Self::field("exit code: ", Color::Red, x.to_string().as_str());
        }
        Self::field(
            "command:   ",
            Color::Blue,
            command.executor.command_line().as_str(),
        );
        if let Some(env) = command.executor.env() {
            Self::field(
                "env:       ",
                Color::Blue,
                format!(
                    "{}",
                    env.iter()
                        .sorted_unstable_by(|a, b| Ord::cmp(&a.0, &b.0))
                        .map(|x| format!("{}={}", x.0, x.1))
                        .join(" ")
                )
                .as_str(),
            );
        }
        Self::field(
            "stderr:\n",
            Color::Blue,
            &execution_result.stderr.to_str_lossy(),
        );
        Self::field(
            "stdout:\n",
            Color::Blue,
            &execution_result.stdout.to_str_lossy(),
        );
        Self::line();
        println!();
    }

    pub fn status(
        &mut self,
        succeeded: usize,
        cached: usize,
        failed: usize,
        running: usize,
        remaining: usize,
    ) {
        if self.status_printed {
            print!("{}", RestorePosition);
        } else {
            print!("{}", SavePosition);
        }
        print!(
            "{}Status: {}{}{} succeeded ({} cached), {}{}{} failed, {} running, {} remaining",
            SetForegroundColor(Color::Blue),
            SetForegroundColor(if succeeded > 0 {
                Color::Green
            } else {
                Color::Reset
            }),
            succeeded,
            SetForegroundColor(Color::Reset),
            cached,
            SetForegroundColor(if failed > 0 { Color::Red } else { Color::Reset }),
            failed,
            SetForegroundColor(Color::Reset),
            running,
            remaining,
        );
        stdout().flush().unwrap();
        self.status_printed = true;
    }

    pub fn finished(&mut self, stats: &SchedulerStats) {
        self.clear_status();
        println!(
            "{}Done. {}{}{} succeeded ({} cached), {}{}{} failed, {}{}{} not run.",
            SetForegroundColor(Color::Blue),
            SetForegroundColor(if stats.exec.succeeded > 0 {
                Color::Green
            } else {
                Color::Reset
            }),
            stats.exec.succeeded,
            SetForegroundColor(Color::Reset),
            stats.cache_hits,
            SetForegroundColor(if stats.exec.failed > 0 {
                Color::Red
            } else {
                Color::Reset
            }),
            stats.exec.failed,
            SetForegroundColor(Color::Reset),
            SetForegroundColor(if stats.exec.not_run > 0 {
                Color::Red
            } else {
                Color::Reset
            }),
            stats.exec.not_run,
            SetForegroundColor(Color::Reset),
        );
    }

    fn clear_status(&mut self) {
        if self.status_printed {
            print!("{}{:>80}{}", RestorePosition, "", RestorePosition);
            self.status_printed = false;
        }
    }

    fn field(name: &str, color: Color, value: &str) {
        if value.is_empty() {
            return;
        }
        println!(
            "{}{}{}{}",
            SetForegroundColor(color),
            name,
            Attribute::Reset,
            value.trim()
        );
    }

    fn line() {
        let columns = terminal::size().map_or(80, |x| x.0 as usize);
        println!(
            "{}{}{}",
            SetForegroundColor(Color::Red),
            "-".repeat(columns),
            Attribute::Reset,
        );
    }
}
