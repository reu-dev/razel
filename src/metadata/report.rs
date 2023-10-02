use crate::executors::ExecutionStatus;
use crate::metadata::LogFileItem;
use crate::tui::{A_BOLD, A_RESET, C_GREEN, C_RED, C_RESET, C_YELLOW};
use crate::SchedulerExecStats;
use anyhow::Result;
use crossterm::style::SetForegroundColor;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

static KEY_ALL: &str = "[all]";
static KEY_OTHER: &str = "[other]";

#[derive(Deserialize, Serialize)]
pub struct Report {
    pub stats: HashMap<String, Stats>,
}

impl Report {
    pub fn new(group_by_tag: &str, items: &Vec<LogFileItem>) -> Self {
        let mut all: Stats = Default::default();
        let mut grouped: HashMap<String, Stats> = Default::default();
        let mut other: Stats = Default::default();
        let key_with_colon = format!("{group_by_tag}:");
        for item in items {
            all.add_execution_status(&item.status);
            let mut is_other = true;
            for value in item
                .tags
                .iter()
                .filter_map(|x| x.strip_prefix(&key_with_colon))
            {
                grouped
                    .entry(value.into())
                    .or_default()
                    .add_execution_status(&item.status);
                is_other = false;
            }
            if is_other {
                other.add_execution_status(&item.status);
            }
        }
        if !grouped.is_empty() && other != Default::default() {
            grouped.insert(KEY_OTHER.into(), other);
        }
        grouped.insert(KEY_ALL.into(), all);
        Self { stats: grouped }
    }

    pub fn write(&self, path: &PathBuf) -> Result<()> {
        let vec = serde_json::to_vec_pretty(&self.stats)?;
        fs::write(path, vec)?;
        Ok(())
    }

    pub fn print(&self) {
        if self.stats.len() <= 2 {
            return; // not useful: just [all] and another group
        }
        println!();
        println!("report:");
        let width = self.stats.keys().map(|x| x.len()).max().unwrap_or_default();
        for value in self.stats.keys().sorted() {
            if value == KEY_ALL || value == KEY_OTHER {
                continue;
            }
            self.print_stats(value, width);
        }
        if self.stats.contains_key(KEY_OTHER) {
            self.print_stats(KEY_OTHER, width);
        }
        println!();
    }

    fn print_stats(&self, value: &str, width: usize) {
        let stats = &self.stats[value];
        print!("  {value:width$}: ");
        Self::print_status("succeeded", stats.succeeded, C_GREEN);
        Self::maybe_print_status("failed", stats.failed, C_RED);
        Self::maybe_print_status("skipped", stats.skipped, C_RESET);
        Self::maybe_print_status("not run", stats.not_run, C_YELLOW);
        println!();
    }

    fn print_status(status: &str, count: usize, color: SetForegroundColor) {
        print!(
            "{A_BOLD}{}{count}{C_RESET}{A_RESET} {status}",
            if count != 0 { color } else { C_RESET }
        );
    }

    fn maybe_print_status(status: &str, count: usize, color: SetForegroundColor) {
        if count == 0 {
            return;
        }
        print!(", {A_BOLD}{color}{count}{C_RESET}{A_RESET} {status}");
    }
}

type Stats = SchedulerExecStats;

impl Stats {
    fn add_execution_status(&mut self, status: &ExecutionStatus) {
        match status {
            ExecutionStatus::NotStarted => self.not_run += 1,
            ExecutionStatus::Skipped => self.skipped += 1,
            ExecutionStatus::Success => self.succeeded += 1,
            _ => self.failed += 1,
        }
    }
}
