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
    group_by_tag_prefix: String,
    classname: Option<String>,
    failed_output_bytes: usize,
    passed_output_bytes: usize,
    items: Vec<JunitItem>,
}

impl JunitWriter {
    pub fn new(
        path: PathBuf,
        group_by_tag: String,
        classname: Option<String>,
        failed_output_bytes: usize,
        passed_output_bytes: usize,
    ) -> Self {
        Self {
            path,
            group_by_tag_prefix: format!("{group_by_tag}:"),
            classname: classname.map(|x| escape_attr(&x)),
            failed_output_bytes,
            passed_output_bytes,
            items: vec![],
        }
    }

    fn push(&mut self, target: &Target, execution_result: &ExecutionResult) {
        debug_assert!(!self.items.iter().any(|x| x.name == target.name));
        let classname = self.classname.clone().unwrap_or_else(|| {
            target
                .tags
                .iter()
                .find_map(|t| match t {
                    Tag::Custom(v) => v.strip_prefix(&self.group_by_tag_prefix).map(escape_attr),
                    _ => None,
                })
                .unwrap_or_default()
        });
        let limit = if execution_result.status == ExecutionStatus::Success {
            self.passed_output_bytes
        } else {
            self.failed_output_bytes
        };
        self.items.push(JunitItem {
            name: target.name.clone(),
            classname,
            status: execution_result.status,
            error: execution_result.error.clone(),
            exec_duration: execution_result.exec_duration.map(|d| d.as_secs_f32()),
            stdout: Self::limit_bytes(&execution_result.stdout, limit),
            stderr: Self::limit_bytes(&execution_result.stderr, limit),
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
        let status = item.status;
        let classname = &item.classname;
        let name = escape_attr(&item.name);
        let time = item.exec_duration.unwrap_or(0.0);
        writeln!(
            w,
            r#"    <testcase classname="{classname}" name="{name}" time="{time:.3}">"#,
        )?;
        match status {
            ExecutionStatus::Success => {}
            ExecutionStatus::Skipped => {
                writeln!(w, "      <skipped>depends on a failed condition</skipped>")?;
            }
            ExecutionStatus::NotStarted => {
                writeln!(w, "      <skipped>not started</skipped>")?;
            }
            ExecutionStatus::SystemError => {
                if let Some(error) = item.error.as_deref().map(escape_content) {
                    writeln!(w, "      <error>{status:?}: {error}</error>")?;
                } else {
                    writeln!(w, "      <error>{status:?}</error>")?;
                }
            }
            _ => {
                if let Some(error) = item.error.as_deref().map(escape_content) {
                    writeln!(w, "      <failure>{status:?}: {error}</failure>")?;
                } else {
                    writeln!(w, "      <failure>{status:?}</failure>")?;
                }
            }
        }
        if !item.stdout.is_empty() {
            let text = escape_content(&String::from_utf8_lossy(&item.stdout));
            writeln!(w, "      <system-out>{text}</system-out>")?;
        }
        if !item.stderr.is_empty() {
            let text = escape_content(&String::from_utf8_lossy(&item.stderr));
            writeln!(w, "      <system-err>{text}</system-err>")?;
        }
        writeln!(w, "    </testcase>")?;
        Ok(())
    }

    fn limit_bytes(bytes: &[u8], limit: usize) -> Vec<u8> {
        match limit {
            0 => vec![],
            limit if bytes.len() > limit => {
                let half = limit / 2;
                let head_end = (0..=half)
                    .rev()
                    .find(|&i| std::str::from_utf8(&bytes[..i]).is_ok())
                    .unwrap_or(0);
                let tail_start = ((bytes.len() - half)..bytes.len())
                    .find(|&i| std::str::from_utf8(&bytes[i..]).is_ok())
                    .unwrap_or(bytes.len());
                let omitted = tail_start - head_end;
                let mut out = bytes[..head_end].to_vec();
                out.extend_from_slice(
                    format!(
                        "\n[... {omitted} bytes omitted ({} total) ...]\n",
                        bytes.len()
                    )
                    .as_bytes(),
                );
                out.extend_from_slice(&bytes[tail_start..]);
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
