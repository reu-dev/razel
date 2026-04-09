use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::metadata::LogWriter;
use crate::types::{Tag, Target};
use anyhow::Result;
use serde_json::{Map, Value};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

struct JunitItem {
    name: String,
    classname: String,
    status: ExecutionStatus,
    error: Option<String>,
    exec_duration: Option<f32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Accumulates execution results and writes a JUnit XML report.
pub struct JunitWriter {
    path: PathBuf,
    group_by_tag: String,
    max_output_bytes: Option<usize>,
    include_output_on_success: bool,
    items: Vec<JunitItem>,
}

impl JunitWriter {
    pub fn new(path: PathBuf, group_by_tag: String) -> Self {
        Self {
            path,
            group_by_tag,
            max_output_bytes: Some(4096),
            include_output_on_success: false,
            items: vec![],
        }
    }

    fn push(&mut self, target: &Target, execution_result: &ExecutionResult) {
        debug_assert!(!self.items.iter().any(|x| x.name == target.name));
        let prefix = format!("{}:", self.group_by_tag);
        let classname = target
            .tags
            .iter()
            .find_map(|t| match t {
                Tag::Custom(v) => v.strip_prefix(&prefix).map(escape_attr),
                _ => None,
            })
            .unwrap_or_default();
        self.items.push(JunitItem {
            name: target.name.clone(),
            classname,
            status: execution_result.status,
            error: execution_result.error.clone(),
            exec_duration: execution_result.exec_duration.map(|d| d.as_secs_f32()),
            stdout: self.limit_bytes(&execution_result.stdout),
            stderr: self.limit_bytes(&execution_result.stderr),
        });
    }

    fn write(&self) -> Result<()> {
        let mut w = BufWriter::new(File::create(&self.path)?);
        let total_time: f32 = self.items.iter().filter_map(|i| i.exec_duration).sum();
        writeln!(w, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
        writeln!(w, r#"<testsuites time="{:.3}">"#, total_time)?;
        writeln!(w, r#"  <testsuite name="razel" time="{:.3}">"#, total_time)?;
        for item in &self.items {
            self.write_testcase(&mut w, item)?;
        }
        writeln!(w, "  </testsuite>")?;
        writeln!(w, "</testsuites>")?;
        w.flush()?;
        Ok(())
    }

    fn write_testcase(&self, w: &mut impl Write, item: &JunitItem) -> Result<()> {
        let classname = &item.classname;
        let name = escape_attr(&item.name);
        let time = item.exec_duration.unwrap_or(0.0);
        writeln!(
            w,
            r#"    <testcase classname="{classname}" name="{name}" time="{time:.3}">"#,
        )?;
        match item.status {
            ExecutionStatus::Success => {}
            ExecutionStatus::Skipped => {
                writeln!(w, "      <skipped>depends on a failed target</skipped>")?;
            }
            ExecutionStatus::NotStarted => {
                writeln!(w, "      <skipped>not started</skipped>")?;
            }
            ExecutionStatus::SystemError => {
                let msg = item
                    .error
                    .as_deref()
                    .unwrap_or("system error (cache/sandbox)");
                writeln!(w, "      <error>{}</error>", escape_content(msg))?;
            }
            _ => {
                let fallback = match item.status {
                    ExecutionStatus::FailedToStart => "failed to start",
                    ExecutionStatus::FailedToCreateResponseFile => "failed to create response file",
                    ExecutionStatus::FailedToWriteStdoutFile => "failed to write stdout file",
                    ExecutionStatus::FailedToWriteStderrFile => "failed to write stderr file",
                    ExecutionStatus::Crashed => "crashed (core dump or signal)",
                    ExecutionStatus::Timeout => "timed out",
                    _ => "failed",
                };
                let msg = item.error.as_deref().unwrap_or(fallback);
                writeln!(w, "      <failure>{}</failure>", escape_content(msg))?;
            }
        }

        let emit_output =
            self.include_output_on_success || !matches!(item.status, ExecutionStatus::Success);
        if emit_output {
            if !item.stdout.is_empty() {
                let text = escape_content(&String::from_utf8_lossy(&item.stdout));
                writeln!(w, "      <system-out>{text}</system-out>")?;
            }
            if !item.stderr.is_empty() {
                let text = escape_content(&String::from_utf8_lossy(&item.stderr));
                writeln!(w, "      <system-err>{text}</system-err>")?;
            }
        }
        writeln!(w, "    </testcase>")?;
        Ok(())
    }

    fn limit_bytes(&self, bytes: &[u8]) -> Vec<u8> {
        match self.max_output_bytes {
            Some(limit) if bytes.len() > limit => {
                let safe_end = (0..=limit)
                    .rev()
                    .find(|&i| std::str::from_utf8(&bytes[..i]).is_ok())
                    .unwrap_or(0);
                let mut out = bytes[..safe_end].to_vec();
                out.extend_from_slice(
                    format!(
                        "\n[... output truncated at {limit} bytes ({} total) ...]",
                        bytes.len()
                    )
                    .as_bytes(),
                );
                out
            }
            _ => bytes.to_vec(),
        }
    }
}

impl LogWriter for JunitWriter {
    fn push_target_finished(
        &mut self,
        target: &Target,
        execution_result: &ExecutionResult,
        _output_size: Option<u64>,
        _measurements: &Map<String, Value>,
    ) {
        self.push(target, execution_result);
    }

    fn push_target_not_run(&mut self, target: &Target, status: ExecutionStatus) {
        self.push(
            target,
            &ExecutionResult {
                status,
                ..Default::default()
            },
        );
    }

    fn finish(&self) -> Result<()> {
        self.write()
    }
}

fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '"' => out.push_str("&quot;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '\'' => out.push_str("&apos;"),
            c => out.push(c),
        }
    }
    out
}

fn escape_content(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            c => out.push(c),
        }
    }
    out
}
