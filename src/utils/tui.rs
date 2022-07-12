use crate::executors::ExecutionResult;
use crate::{Command, SchedulerStats};
use crossterm::cursor::{RestorePosition, SavePosition};
use crossterm::style::{Attribute, Color, SetForegroundColor};
use crossterm::terminal;
use std::io::{stdout, Write};

/// Terminal user interface
pub struct TUI {
    status_printed: bool,
}

impl TUI {
    pub fn new() -> Self {
        Self {
            status_printed: false,
        }
    }

    pub fn command_succeeded(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.clear_status();
        println!(
            "{}{:?}{} {}",
            SetForegroundColor(Color::Green),
            execution_result.status,
            Attribute::Reset,
            command.name
        );
    }

    pub fn command_failed(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.clear_status();
        println!();
        Self::line();
        println!(
            "{}{:?}{}     {}",
            SetForegroundColor(Color::Red),
            execution_result.status,
            Attribute::Reset,
            command.name
        );
        if let Some(x) = execution_result.exit_code {
            println!(
                "{}exit code:{} {}",
                SetForegroundColor(Color::Red),
                Attribute::Reset,
                x
            );
        }
        if let Some(x) = &execution_result.error {
            println!(
                "{}error:{}     {}",
                SetForegroundColor(Color::Red),
                Attribute::Reset,
                x
            );
        }
        println!(
            "{}command:{}   {}",
            SetForegroundColor(Color::Blue),
            Attribute::Reset,
            command.executor.command_line()
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
