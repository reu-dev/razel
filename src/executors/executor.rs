use crate::executors::{CustomCommandExecutor, TaskExecutor};

pub enum Executor {
    CustomCommand(CustomCommandExecutor),
    Task(TaskExecutor),
}

/* TODO
pub struct ExecutionResult {
    exit_status: Option<ExitStatus>,
}
*/
