use crate::executors::{ExecutionResult, ExecutionStatus};
use crate::types::Target;
use anyhow::Result;
use serde_json::{Map, Value};

pub trait LogWriter {
    fn push_target_finished(
        &mut self,
        target: &Target,
        execution_result: &ExecutionResult,
        output_size: Option<u64>,
        measurements: &Map<String, Value>,
    );

    fn push_target_not_run(&mut self, target: &Target, status: ExecutionStatus);

    fn finish(&self) -> Result<()>;
}
