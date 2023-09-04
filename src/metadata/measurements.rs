use crate::executors::ExecutionResult;
use bstr::ByteSlice;
use itertools::Itertools;
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::PathBuf;

/// Parses and collects measurements for all execution results and writes a csv file.
///
/// Cols for command name and execution status are added automatically.
pub struct Measurements {
    re: Vec<Regex>,
    /// measurement names with col index
    cols: HashMap<String, usize>,
    rows: Vec<Vec<String>>,
}

impl Measurements {
    pub fn new() -> Self {
        Self {
            re: vec![
                // see https://cmake.org/cmake/help/latest/command/ctest_test.html#additional-test-measurements
                Regex::new(
                    r#"<CTestMeasurement\s+type="[^"]+"\s+name="(?P<key>[^"]+)">(?P<value>[^<]+)</CTestMeasurement>"#,
                ).unwrap(),
                Regex::new(
                    r#"<CTestMeasurement\s+name="(?P<key>[^"]+)"\s+type="[^"]+">(?P<value>[^<]+)</CTestMeasurement>"#,
                ).unwrap(),
                // <DartMeasurement> is an old version of <CTestMeasurement>
                Regex::new(
                    r#"<DartMeasurement\s+type="[^"]+"\s+name="(?P<key>[^"]+)">(?P<value>[^<]+)</DartMeasurement>"#,
                ).unwrap(),
                Regex::new(
                    r#"<DartMeasurement\s+name="(?P<key>[^"]+)"\s+type="[^"]+">(?P<value>[^<]+)</DartMeasurement>"#,
                ).unwrap(),
            ],
            cols: HashMap::from([("command".into(), 0), ("status".into(), 1)]),
            rows: vec![],
        }
    }

    pub fn collect(
        &mut self,
        command_name: &str,
        execution_result: &ExecutionResult,
    ) -> Map<String, Value> {
        let (mut row, map) = self.capture(execution_result.stdout.to_str_lossy().as_ref());
        if !row.is_empty() {
            row[0] = command_name.to_owned();
            row[1] = format!("{:?}", execution_result.status);
            self.rows.push(row);
        }
        map
    }

    fn capture(&mut self, text: &str) -> (Vec<String>, Map<String, Value>) {
        let mut vec: Vec<String> = vec![];
        let mut map: Map<String, Value> = Default::default();
        for re in &self.re {
            for captures in re.captures_iter(text) {
                let keys_len = self.cols.len();
                let col = *self
                    .cols
                    .entry(captures["key"].to_string())
                    .or_insert(keys_len);
                if vec.len() < col + 1 {
                    vec.resize(col + 1, Default::default());
                }
                vec[col] = captures["value"].to_string();
                map.insert(
                    captures["key"].to_string(),
                    Value::String(captures["value"].to_string()),
                );
            }
        }
        (vec, map)
    }

    pub fn write_csv(&self, path: &PathBuf) -> Result<(), anyhow::Error> {
        if self.rows.is_empty() {
            return Ok(());
        }
        let mut writer = csv::Writer::from_path(path)?;
        writer.write_record(
            self.cols
                .iter()
                .sorted_unstable_by_key(|(_, x)| *x)
                .map(|(x, _)| x),
        )?;
        for x in &self.rows {
            let mut fixed_size_row = x.clone();
            fixed_size_row.resize(self.cols.len(), Default::default());
            writer.write_record(fixed_size_row)?;
        }
        writer.flush()?;
        Ok(())
    }
}

impl Default for Measurements {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static FIXED_COLS: usize = 2;

    #[test]
    fn ctest() {
        let mut measurements = Measurements::new();
        assert_eq!(
            measurements.capture(
                r#"<CTestMeasurement type="numeric/float" name="score">12.3</CTestMeasurement>"#,
            ).0,
            vec!["".to_string(), "".to_string(), "12.3".to_string()]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
    }

    #[test]
    fn dart() {
        let mut measurements = Measurements::new();
        assert_eq!(
            measurements
                .capture(
                    r#"<DartMeasurement type="numeric/float" name="score">12.3</DartMeasurement>"#,
                )
                .0,
            vec!["".to_string(), "".to_string(), "12.3".to_string()]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
    }

    #[test]
    fn ctest_and_dart() {
        let mut measurements = Measurements::new();
        assert_eq!(
            measurements
                .capture(
                    r#"
                <CTestMeasurement type="numeric/float" name="score">12.3</CTestMeasurement>
                <CTestMeasurement  name="cost"  type="numeric/integer">3</CTestMeasurement>
                <DartMeasurement type="text/string" name="color_fg">blue</DartMeasurement>
                <DartMeasurement name="color bg" type="text/string">grey</DartMeasurement>
                "#,
                )
                .0,
            vec![
                "".to_string(),
                "".to_string(),
                "12.3".to_string(),
                "3".to_string(),
                "blue".to_string(),
                "grey".to_string()
            ]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
        assert_eq!(measurements.cols.get("cost"), Some(&(FIXED_COLS + 1)));
        assert_eq!(measurements.cols.get("color_fg"), Some(&(FIXED_COLS + 2)));
        assert_eq!(measurements.cols.get("color bg"), Some(&(FIXED_COLS + 3)));
    }
}
