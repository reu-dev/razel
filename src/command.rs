use crate::executors::{CustomCommandExecutor, Executor, TaskExecutor, TaskFn};
use crate::{ArenaId, FileId, ScheduleState, Scheduler};

pub struct Command {
    pub id: CommandId,
    pub name: String,
    pub inputs: Vec<FileId>,
    pub outputs: Vec<FileId>,
    pub executor: Executor,
    /// dependencies which are not yet finished successfully
    pub unfinished_deps: Vec<CommandId>,
    /// commands which depend on this command
    pub reverse_deps: Vec<CommandId>,
    /// TODO remove, Scheduler should keep track of states
    pub schedule_state: ScheduleState,
    //exit_status: Option<ExitStatus>,
    //error: Option<io::Error>,
}

pub type CommandId = ArenaId<Command>;

impl Command {
    pub async fn exec(&self) -> Result<(), anyhow::Error> {
        match &self.executor {
            Executor::CustomCommand(c) => c.exec().await,
            Executor::Task(t) => t.exec().await,
        }
    }

    pub fn command_line(&self) -> String {
        match &self.executor {
            Executor::CustomCommand(c) => c.command_line(),
            Executor::Task(t) => t.command_line.clone(),
        }
    }
}

pub struct CommandBuilder {
    name: String,
    args: Vec<String>,
    inputs: Vec<FileId>,
    outputs: Vec<FileId>,
    executor: Option<Executor>,
}

impl CommandBuilder {
    pub fn new(name: String, args: Vec<String>) -> CommandBuilder {
        CommandBuilder {
            name,
            args,
            inputs: vec![],
            outputs: vec![],
            executor: None,
        }
    }

    fn map_path(&mut self, original: &String, mapped: &String) {
        self.args.iter_mut().for_each(|x| {
            if x == original {
                *x = mapped.clone()
            }
        });
    }

    pub fn input(
        &mut self,
        path: &String,
        scheduler: &mut Scheduler,
    ) -> Result<String, anyhow::Error> {
        scheduler.input_file(path).map(|file| {
            let new_path = file.path.clone();
            self.map_path(path, &new_path);
            self.inputs.push(file.id);
            new_path
        })
    }

    pub fn inputs(
        &mut self,
        paths: &Vec<String>,
        scheduler: &mut Scheduler,
    ) -> Result<Vec<String>, anyhow::Error> {
        self.inputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = scheduler.input_file(path)?;
                let new_path = file.path.clone();
                self.map_path(path, &new_path);
                self.inputs.push(file.id);
                Ok(new_path)
            })
            .collect()
    }

    pub fn output(
        &mut self,
        path: &String,
        scheduler: &mut Scheduler,
    ) -> Result<String, anyhow::Error> {
        scheduler.output_file(path).map(|file| {
            let new_path = file.path.clone();
            self.map_path(path, &new_path);
            self.outputs.push(file.id);
            new_path
        })
    }

    pub fn outputs(
        &mut self,
        paths: &Vec<String>,
        scheduler: &mut Scheduler,
    ) -> Result<Vec<String>, anyhow::Error> {
        self.inputs.reserve(paths.len());
        paths
            .iter()
            .map(|path| {
                let file = scheduler.output_file(path)?;
                let new_path = file.path.clone();
                self.map_path(path, &new_path);
                self.inputs.push(file.id);
                Ok(new_path)
            })
            .collect()
    }

    pub fn custom_command_executor(&mut self, executable: String) {
        self.executor = Some(Executor::CustomCommand(CustomCommandExecutor {
            executable,
            args: self.args.clone(),
        }));
    }

    pub fn task_executor(&mut self, f: TaskFn) {
        let command_line = self.args.join(" ");
        self.executor = Some(Executor::Task(TaskExecutor { f, command_line }));
    }

    pub fn build(self, id: CommandId) -> Command {
        Command {
            id,
            name: self.name,
            inputs: self.inputs,
            outputs: self.outputs,
            executor: self.executor.unwrap(),
            unfinished_deps: vec![],
            reverse_deps: vec![],
            schedule_state: ScheduleState::New,
        }
    }
}
