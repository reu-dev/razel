use anyhow::ensure;
use csv::{StringRecord, Writer};
use log::info;
use std::io;

pub fn csv_concat(inputs: Vec<String>, output: String) -> Result<(), anyhow::Error> {
    info!("csv_concat {:?} -> {}", inputs, output);
    let mut writer = csv::Writer::from_path(output)?;
    let mut combined_headers: Option<StringRecord> = None;
    for input in inputs {
        let mut reader = csv::Reader::from_path(input)?;
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
}

pub fn csv_filter(
    input: String,
    output: String,
    cols: Vec<String>,
    fields: Vec<(String, String)>,
) -> Result<(), anyhow::Error> {
    if !fields.is_empty() {
        todo!();
    }
    let mut reader = csv::Reader::from_path(input)?;
    let headers = reader.headers()?;
    let indices: Vec<usize> = if !cols.is_empty() {
        headers
            .iter()
            .enumerate()
            .filter_map(|(i, x)| cols.contains(&x.to_string()).then(|| i))
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
}

fn write_record_filtered<W: io::Write>(
    writer: &mut Writer<W>,
    record: &StringRecord,
    indices: &Vec<usize>,
) -> Result<(), anyhow::Error> {
    for i in indices {
        writer.write_field(record.get(*i).unwrap_or_default())?;
    }
    writer.write_record(None::<&[u8]>)?;
    Ok(())
}
