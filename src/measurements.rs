use crate::executors::ExecutionResult;
use bstr::ByteSlice;
use itertools::Itertools;
use regex::Regex;
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
                    r#"<CTestMeasurement name="(?P<key>[^"]+)" type="[^"]+">(?P<value>[^<]+)</CTestMeasurement>"#,
                ).unwrap(),
                // <DartMeasurement> is an old version of <CTestMeasurement>
                Regex::new(
                    r#"<DartMeasurement name="(?P<key>[^"]+)" type="[^"]+">(?P<value>[^<]+)</DartMeasurement>"#,
                ).unwrap(),
            ],
            cols: HashMap::from([("command".into(), 0), ("status".into(), 1)]),
            rows: vec![],
        }
    }

    pub fn collect(&mut self, command_name: &str, execution_result: &ExecutionResult) {
        let mut measurements = self.capture(execution_result.stdout.to_str_lossy().as_ref());
        if measurements.is_empty() {
            return;
        }
        measurements[0] = command_name.to_owned();
        measurements[1] = format!("{:?}", execution_result.status);
        self.rows.push(measurements);
    }

    fn capture(&mut self, text: &str) -> Vec<String> {
        let mut measurements: Vec<String> = vec![];
        for re in &self.re {
            for captures in re.captures_iter(text) {
                let keys_len = self.cols.len();
                let col = *self
                    .cols
                    .entry(captures["key"].to_string())
                    .or_insert(keys_len);
                if measurements.len() < col + 1 {
                    measurements.resize(col + 1, Default::default());
                }
                measurements[col] = captures["value"].to_string();
            }
        }
        measurements
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
            writer.write_record(x)?;
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
                r#"<CTestMeasurement name="score" type="numeric/float">12.3</CTestMeasurement>"#,
            ),
            vec!["".to_string(), "".to_string(), "12.3".to_string()]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
    }

    #[test]
    fn dart() {
        let mut measurements = Measurements::new();
        assert_eq!(
            measurements.capture(
                r#"<DartMeasurement name="score" type="numeric/float">12.3</DartMeasurement>"#,
            ),
            vec!["".to_string(), "".to_string(), "12.3".to_string()]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
    }

    #[test]
    fn ctest_and_dart() {
        let mut measurements = Measurements::new();
        assert_eq!(
            measurements.capture(
                r#"<CTestMeasurement name="score" type="numeric/float">12.3</CTestMeasurement>
                <DartMeasurement name="color" type="text/string">blue</DartMeasurement>
                "#,
            ),
            vec![
                "".to_string(),
                "".to_string(),
                "12.3".to_string(),
                "blue".to_string()
            ]
        );
        assert_eq!(measurements.cols.get("score"), Some(&FIXED_COLS));
        assert_eq!(measurements.cols.get("color"), Some(&(FIXED_COLS + 1)));
    }
}
