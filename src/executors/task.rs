pub type TaskFn = Box<dyn Fn() -> Result<(), anyhow::Error>>;

pub struct TaskExecutor {
    pub f: TaskFn,
    pub command_line: String,
}

impl TaskExecutor {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        (self.f)()
    }
}
