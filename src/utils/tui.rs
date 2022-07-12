use crate::executors::ExecutionResult;
use crate::Command;
use crossterm::style::{Attribute, Color, SetForegroundColor};
use crossterm::terminal;

/// Terminal user interface
pub struct TUI {}

impl TUI {
    pub fn command_succeeded(
        command: &Command,
        execution_result: &ExecutionResult,
    ) -> Result<(), anyhow::Error> {
        println!(
            "{}{:?}{} {}",
            SetForegroundColor(Color::Green),
            execution_result.status,
            Attribute::Reset,
            command.name
        );
        Ok(())
    }

    pub fn command_failed(
        command: &Command,
        execution_result: &ExecutionResult,
    ) -> Result<(), anyhow::Error> {
        println!();
        Self::line()?;
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
        Self::line()?;
        println!();
        Ok(())
    }

    fn line() -> Result<(), anyhow::Error> {
        let columns = terminal::size()?.0 as usize;
        println!(
            "{}{}{}",
            SetForegroundColor(Color::Red),
            "-".repeat(columns),
            Attribute::Reset,
        );
        Ok(())
    }
}
