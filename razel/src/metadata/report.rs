use super::LogWriter;
use crate::SchedulerExecStats;
use crate::executors::ExecutionResult;
use crate::executors::ExecutionStatus;
use crate::tui::{A_BOLD, A_RESET, C_GREEN, C_RED, C_RESET, C_YELLOW};
use crate::types::{Tag, Target};
use anyhow::Result;
use crossterm::style::SetForegroundColor;
use itertools::Itertools;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

static KEY_ALL: &str = "[all]";
static KEY_OTHER: &str = "[other]";

pub struct ReportWriter {
    path: PathBuf,
    key_with_colon: String,
    all: Stats,
    grouped: HashMap<String, Stats>,
    other: Stats,
}

impl ReportWriter {
    pub fn new(path: PathBuf, group_by_tag: &str) -> Self {
        Self {
            path,
            key_with_colon: format!("{group_by_tag}:"),
            all: Default::default(),
            grouped: Default::default(),
            other: Default::default(),
        }
    }

    fn push(&mut self, tags: &[Tag], status: ExecutionStatus) {
        self.all.add_execution_status(&status);
        let mut is_other = true;
        for value in tags
            .iter()
            .filter_map(|x| match x {
                Tag::Custom(x) => Some(x),
                _ => None,
            })
            .filter_map(|x| x.strip_prefix(&self.key_with_colon))
        {
            self.grouped
                .entry(value.into())
                .or_default()
                .add_execution_status(&status);
            is_other = false;
        }
        if is_other {
            self.other.add_execution_status(&status);
        }
    }

    fn report(&self) -> Report {
        let mut grouped = self.grouped.clone();
        if !grouped.is_empty() && self.other != Default::default() {
            grouped.insert(KEY_OTHER.into(), self.other.clone());
        }
        grouped.insert(KEY_ALL.into(), self.all.clone());
        Report(grouped)
    }
}

impl LogWriter for ReportWriter {
    fn push_target_finished(
        &mut self,
        target: &Target,
        execution_result: &ExecutionResult,
        _output_size: Option<u64>,
        _measurements: &Map<String, Value>,
    ) {
        self.push(&target.tags, execution_result.status);
    }

    fn push_target_not_run(&mut self, target: &Target, status: ExecutionStatus) {
        self.push(&target.tags, status);
    }

    fn finish(&self) -> Result<()> {
        let report = self.report();
        report.print();
        report.write(&self.path)
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

struct Report(HashMap<String, Stats>);

impl Report {
    pub fn write(&self, path: &PathBuf) -> Result<()> {
        let vec = serde_json::to_vec_pretty(&self.0)?;
        fs::write(path, vec)?;
        Ok(())
    }

    pub fn print(&self) {
        if self.0.len() <= 2 {
            return; // not useful: just [all] and another group
        }
        println!();
        println!("report:");
        let width = self.0.keys().map(|x| x.len()).max().unwrap_or_default();
        for value in self.0.keys().sorted() {
            if value == KEY_ALL || value == KEY_OTHER {
                continue;
            }
            self.print_stats(value, width);
        }
        if self.0.contains_key(KEY_OTHER) {
            self.print_stats(KEY_OTHER, width);
        }
        println!();
    }

    fn print_stats(&self, value: &str, width: usize) {
        let stats = &self.0[value];
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
