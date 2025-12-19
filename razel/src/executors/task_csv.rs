use crate::types::{CsvConcatTask, CsvFilterTask};
use crate::SandboxDir;
use anyhow::{anyhow, ensure, Result};
use itertools::Itertools;
use std::io;
use tokio::task::spawn_blocking;

impl CsvConcatTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let inputs = self.input.iter().map(|x| sandbox_dir.join(x)).collect_vec();
        let output = sandbox_dir.join(&self.output);
        spawn_blocking(move || {
            let mut writer =
                csv::Writer::from_path(&output).map_err(|e| anyhow!("{e}: {output:?}"))?;
            let mut combined_headers: Option<csv::StringRecord> = None;
            for input in inputs {
                let mut reader =
                    csv::Reader::from_path(&input).map_err(|e| anyhow!("{e}: {input:?}"))?;
                let curr_headers = reader.headers()?;
                if let Some(combined_headers) = &combined_headers {
                    ensure!(curr_headers == combined_headers, "headers do not match!");
                } else {
                    combined_headers = Some(curr_headers.clone());
                    writer.write_record(curr_headers)?;
                }
                for result in reader.records() {
                    let record = result?;
                    writer.write_record(&record)?;
                }
            }
            writer.flush()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

impl CsvFilterTask {
    pub async fn exec(&self, sandbox_dir: &SandboxDir) -> Result<()> {
        let input = sandbox_dir.join(&self.input);
        let output = sandbox_dir.join(&self.output);
        let cols = self.cols.clone();
        spawn_blocking(move || -> Result<()> {
            let mut reader = csv::Reader::from_path(input)?;
            let headers = reader.headers()?;
            let indices: Vec<usize> = if !cols.is_empty() {
                headers
                    .iter()
                    .enumerate()
                    .filter(|&(_, x)| cols.contains(&x.to_string()))
                    .map(|(i, _)| i)
                    .collect()
            } else {
                headers.iter().enumerate().map(|(i, _)| i).collect()
            };
            let mut writer = csv::Writer::from_path(output)?;
            write_record_filtered(&mut writer, headers, &indices)?;
            for result in reader.records() {
                // TODO filter fields
                write_record_filtered(&mut writer, &result?, &indices)?;
            }
            writer.flush()?;
            Ok(())
        })
        .await??;
        Ok(())
    }
}

fn write_record_filtered<W: io::Write>(
    writer: &mut csv::Writer<W>,
    record: &csv::StringRecord,
    indices: &Vec<usize>,
) -> Result<()> {
    for i in indices {
        writer.write_field(record.get(*i).unwrap_or_default())?;
    }
    writer.write_record(None::<&[u8]>)?;
    Ok(())
}
