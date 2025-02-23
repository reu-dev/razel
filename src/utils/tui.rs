use crate::executors::ExecutionResult;
use crate::metadata::Tag;
use crate::{config, Command, SchedulerStats};
use bstr::ByteSlice;
use crossterm::cursor::{RestorePosition, SavePosition};
use crossterm::style::{Attribute, Color, SetForegroundColor};
use crossterm::terminal;
use crossterm::tty::IsTty;
use itertools::Itertools;
use std::io::{stdout, Write};

pub static A_BOLD: Attribute = Attribute::Bold;
pub static A_RESET: Attribute = Attribute::Reset;
pub static C_BLUE: SetForegroundColor = SetForegroundColor(Color::Blue);
pub static C_GREEN: SetForegroundColor = SetForegroundColor(Color::Green);
pub static C_YELLOW: SetForegroundColor = SetForegroundColor(Color::Yellow);
pub static C_RED: SetForegroundColor = SetForegroundColor(Color::Red);
pub static C_RESET: SetForegroundColor = SetForegroundColor(Color::Reset);

/// Terminal user interface
pub struct TUI {
    pub razel_executable: String,
    pub verbose: bool,
    status_printed: bool,
    is_tty: bool,
}

impl TUI {
    pub fn new() -> Self {
        let razel_executable = std::env::args()
            .next()
            .unwrap_or(config::EXECUTABLE.to_string());
        Self {
            razel_executable,
            verbose: false,
            status_printed: false,
            is_tty: stdout().is_tty(),
        }
    }

    pub fn get_update_interval(&self) -> std::time::Duration {
        let secs = if self.is_tty {
            config::UI_UPDATE_INTERVAL_TTY
        } else {
            config::UI_UPDATE_INTERVAL_NON_TTY
        };
        std::time::Duration::from_secs_f32(secs)
    }

    pub fn command_succeeded(&mut self, command: &Command, execution_result: &ExecutionResult) {
        if (!self.verbose && !command.tags.contains(&Tag::Verbose))
            || command.tags.contains(&Tag::Quiet)
        {
            return;
        }
        let stdout = execution_result.stdout.to_str().unwrap_or_default();
        let stderr = execution_result.stderr.to_str().unwrap_or_default();
        if !self.verbose && stdout.is_empty() && stderr.is_empty() {
            return;
        }
        self.clear_status();
        Self::field(
            format!("{:?} ", execution_result.status).as_str(),
            Color::Green,
            if let Some(duration) = execution_result.exec_duration {
                format!(
                    "{} {A_BOLD}{C_BLUE}{:?}{C_RESET}{A_RESET}",
                    command.name, duration,
                )
            } else {
                command.name.clone()
            },
        );
        let print_stream_name = !stdout.is_empty() && !stderr.is_empty();
        Self::field(
            if print_stream_name { "stdout:\n" } else { "" },
            Color::Blue,
            stdout,
        );
        Self::field(
            if print_stream_name { "stderr:\n" } else { "" },
            Color::Blue,
            stderr,
        );
    }

    pub fn command_failed(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.command_failed_impl(command, execution_result, false);
    }

    pub fn command_retry(&mut self, command: &Command, execution_result: &ExecutionResult) {
        self.command_failed_impl(command, execution_result, true);
    }

    fn command_failed_impl(
        &mut self,
        command: &Command,
        execution_result: &ExecutionResult,
        will_retry: bool,
    ) {
        if command.tags.contains(&Tag::Condition)
            && !self.verbose
            && !command.tags.contains(&Tag::Verbose)
        {
            return;
        }
        let color = if will_retry {
            Color::Yellow
        } else {
            Color::Red
        };
        self.clear_status();
        println!();
        Self::line();
        Self::field(
            format!("{:<11}", format!("{:?} ", execution_result.status)).as_str(),
            color,
            command.name.as_str(),
        );
        if let Some(x) = &execution_result.error {
            if will_retry {
                Self::field_with_hint(
                    "error:     ",
                    color,
                    format!("{x:?}").as_str(),
                    "(will retry)",
                );
            } else {
                Self::field("error:     ", color, format!("{x:?}").as_str());
            }
        } else if let Some(x) = execution_result.exit_code {
            Self::field("exit code: ", color, x.to_string().as_str());
        }
        Self::field(
            "command:   ",
            Color::Blue,
            self.format_command_line(
                &command
                    .executor
                    .command_line_with_redirects(&self.razel_executable),
            )
            .as_str(),
        );
        if let Some(env) = command.executor.env() {
            Self::field(
                "env:       ",
                Color::Blue,
                env.iter()
                    .sorted_unstable_by(|a, b| Ord::cmp(&a.0, &b.0))
                    .map(|x| format!("{}={}", x.0, x.1))
                    .join(" ")
                    .as_str(),
            );
        }
        Self::field(
            "stderr:\n",
            Color::Blue,
            execution_result.stderr.to_str().unwrap_or_default(),
        );
        Self::field(
            "stdout:\n",
            Color::Blue,
            execution_result.stdout.to_str().unwrap_or_default(),
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
        if self.is_tty {
            if self.status_printed {
                print!("{RestorePosition}");
            } else {
                print!("{SavePosition}");
            }
        }
        print!(
            "{A_BOLD}{C_BLUE}Status{C_RESET}{A_RESET}: {A_BOLD}{}{}{C_RESET}{A_RESET} succeeded ({} cached), {}{}{}{C_RESET}{A_RESET} failed, {} running, {} remaining",
            if succeeded > 0 {
                C_GREEN
            } else {
                C_RESET
            },
            succeeded,
            cached,
            if failed > 0 { A_BOLD } else { A_RESET },
            if failed > 0 { C_RED } else { C_RESET },
            failed,
            running,
            remaining,
        );
        if !self.is_tty {
            println!();
        }
        stdout().flush().unwrap();
        self.status_printed = true;
    }

    pub fn finished(&mut self, stats: &SchedulerStats) {
        self.clear_status();
        println!(
            "{A_BOLD}{}{} {}{C_RESET}{A_RESET}: {A_BOLD}{}{}{C_RESET}{A_RESET} succeeded ({} cached), {A_BOLD}{}{}{C_RESET}{A_RESET} failed, {A_BOLD}{}{A_RESET} skipped, {A_BOLD}{}{}{C_RESET}{A_RESET} not run.",
            if stats.exec.finished_successfully() {
                C_GREEN
            } else {
                C_RED
            },
            if stats.exec.not_run == 0 {
                "Finished"
            } else {
                "Stopped"
            },
            if stats.exec.finished_successfully() {
                "successfully"
            } else if stats.exec.failed == 1 {
                "after error"
            } else {
                "after errors"
            },
            if stats.exec.succeeded > 0 {
                C_GREEN
            } else {
                C_RESET
            },
            stats.exec.succeeded,
            stats.cache_hits,
            if stats.exec.failed > 0 {
                C_RED
            } else {
                C_RESET
            },
            stats.exec.failed,
            stats.exec.skipped,
            if stats.exec.not_run > 0 {
                C_RED
            } else {
                C_RESET
            },
            stats.exec.not_run,
        );
    }

    pub fn format_command_line(&self, args_with_executable: &[String]) -> String {
        let mut iter = args_with_executable.iter().map(|x| {
            if x.is_empty() {
                "\"\"".to_string()
            } else if x.contains(' ') {
                format!("\"{x}\"")
            } else {
                x.to_string()
            }
        });
        let max_len = config::UI_COMMAND_ARGS_LIMIT
            .map(|x| x + 1) // + 1 for the executable
            .unwrap_or(usize::MAX);
        if args_with_executable.len() > max_len {
            iter.take(max_len)
                .chain(std::iter::once(format!(
                    "{A_BOLD}{C_BLUE}<... {} more args>{C_RESET}{A_RESET}",
                    args_with_executable.len() - max_len
                )))
                .join(" ")
        } else {
            iter.join(" ")
        }
    }

    fn clear_status(&mut self) {
        if self.is_tty && self.status_printed {
            print!("{}{:>90}{}", RestorePosition, " ", RestorePosition);
            self.status_printed = false;
        }
    }

    fn field<S: AsRef<str>>(name: &str, color: Color, value: S) {
        if value.as_ref().is_empty() {
            return;
        }
        let c = SetForegroundColor(color);
        println!(
            "{A_BOLD}{c}{name}{C_RESET}{A_RESET}{}",
            value.as_ref().trim()
        );
    }

    fn field_with_hint<S: AsRef<str>>(name: &str, color: Color, value: S, hint: &str) {
        if value.as_ref().is_empty() {
            return;
        }
        let c = SetForegroundColor(color);
        println!(
            "{A_BOLD}{c}{name}{C_RESET}{A_RESET}{}{A_BOLD}{c} {hint}{C_RESET}{A_RESET}",
            value.as_ref().trim()
        );
    }

    fn line() {
        let columns = terminal::size().map_or(90, |x| x.0 as usize);
        println!("{C_RED}{}{C_RESET}", "-".repeat(columns));
    }
}

impl Default for TUI {
    fn default() -> Self {
        Self::new()
    }
}
