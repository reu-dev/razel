use anyhow::ensure;
use csv::StringRecord;
use log::info;

pub async fn csv_concat(inputs: Vec<String>, output: String) -> Result<(), anyhow::Error> {
    info!("{:?} -> {}", inputs, output);
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
