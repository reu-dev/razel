use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;

use anyhow::bail;
use log::info;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use crate::executors::ExecutionResult;
use crate::{config, Arena, Command, CommandBuilder, CommandId, File, FileId};

#[derive(Debug, PartialEq)]
pub enum ScheduleState {
    New,
    /// Command can not yet be executed because dependencies are still missing
    Waiting,
    /// Command is ready for being executed
    Ready,
    /// Command execution finished successfully
    Succeeded,
    /// Command execution failed
    Failed,
}

type ExecutionResultChannel = (CommandId, ExecutionResult);

pub struct Scheduler {
    files: Arena<File>,
    path_to_file_id: HashMap<String, FileId>,
    commands: Arena<Command>,

    waiting: HashSet<CommandId>,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready: VecDeque<CommandId>,
    running: usize,
    succeeded: Vec<CommandId>,
    failed: Vec<CommandId>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            files: Default::default(),
            path_to_file_id: Default::default(),
            commands: Default::default(),
            waiting: Default::default(),
            ready: Default::default(),
            running: 0,
            succeeded: vec![],
            failed: vec![],
        }
    }

    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn push_custom_command(
        &mut self,
        name: String,
        executable: String,
        args: Vec<String>,
        inputs: Vec<String>,
        outputs: Vec<String>,
    ) -> Result<CommandId, anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args);
        builder.inputs(&inputs, self)?;
        builder.outputs(&outputs, self)?;
        builder.custom_command_executor(executable);
        self.push(builder)
    }

    pub fn push(&mut self, builder: CommandBuilder) -> Result<CommandId, anyhow::Error> {
        let id = self.commands.alloc_with_id(|id| builder.build(id));
        // TODO check if name is unique
        // patch outputs.creating_command
        for output_id in &self.commands[id].outputs {
            let output = &mut self.files[*output_id];
            assert!(output.creating_command.is_none());
            output.creating_command = Some(id);
        }
        Ok(id)
    }

    #[cfg(test)]
    pub fn get_command(&self, id: CommandId) -> Option<&Command> {
        self.commands.get(id)
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        self.create_dependency_graph();
        fs::create_dir_all(config::BIN_DIR)?;
        let (tx, mut rx) = mpsc::channel(32);
        self.start_ready_commands(&tx);
        while self.ready.len() + self.running != 0 {
            if let Some((id, result)) = rx.recv().await {
                self.on_command_finished(id, result);
                self.start_ready_commands(&tx);
            }
        }
        info!(
            "Done. {} succeeded, {} failed, {} not run.",
            self.succeeded.len(),
            self.failed.len(),
            self.waiting.len() + self.ready.len()
        );
        Ok(())
    }

    pub fn input_file(&mut self, path: &String) -> Result<&File, anyhow::Error> {
        let id = self.path_to_file_id.get(path).cloned().unwrap_or_else(|| {
            // create new data file
            let id = self.files.alloc_with_id(|id| File {
                id,
                creating_command: None,
                path: path.clone(),
            });
            self.path_to_file_id.insert(path.clone(), id);
            id
        });
        Ok(&self.files[id])
    }

    pub fn output_file(&mut self, path: &String) -> Result<&File, anyhow::Error> {
        if let Some(file) = self.path_to_file_id.get(path).map(|x| &self.files[*x]) {
            if let Some(creating_command) = file.creating_command {
                bail!(
                    "File {} cannot be output of multiple commands, already output of {}",
                    path,
                    self.commands[creating_command].name
                );
            } else {
                bail!(
                    "File {} cannot be output because it's already used as data",
                    path,
                );
            }
        }
        let id = self.files.alloc_with_id(|id| File {
            id,
            creating_command: None, // will be patched in Scheduler::push()
            path: format!("{}/{}", crate::config::BIN_DIR, path),
        });
        self.path_to_file_id.insert(path.clone(), id);
        Ok(&self.files[id])
    }

    fn create_dependency_graph(&mut self) {
        self.waiting.reserve(self.commands.len());
        self.succeeded.reserve(self.commands.len());
        let mut rdeps = vec![];
        for command in self.commands.iter_mut() {
            assert_eq!(command.schedule_state, ScheduleState::New);
            for input_id in &command.inputs {
                if let Some(dep) = self.files[*input_id].creating_command {
                    command.unfinished_deps.push(dep);
                    rdeps.push((dep, command.id));
                }
            }
            if command.unfinished_deps.is_empty() {
                command.schedule_state = ScheduleState::Ready;
                self.ready.push_back(command.id);
            } else {
                command.schedule_state = ScheduleState::Waiting;
                self.waiting.insert(command.id);
            }
        }
        for (id, rdep) in rdeps {
            self.commands[id].reverse_deps.push(rdep);
        }
        self.check_for_circular_dependencies();
        assert!(!self.ready.is_empty());
    }

    fn check_for_circular_dependencies(&self) {
        // TODO
    }

    fn start_ready_commands(&mut self, tx: &Sender<ExecutionResultChannel>) {
        while let Some(id) = self.ready.pop_front() {
            self.start_next_command(id, tx.clone());
        }
    }

    fn start_next_command(&mut self, id: CommandId, tx: Sender<ExecutionResultChannel>) {
        self.running += 1;
        let command = &self.commands[id];
        assert_eq!(command.schedule_state, ScheduleState::Ready);
        assert_eq!(command.unfinished_deps.len(), 0);
        info!(
            "Execute {}: {}",
            command.name,
            command.executor.command_line()
        );
        let executor = command.executor.clone();
        tokio::task::spawn(async move {
            let result = executor.exec().await;
            // TODO .with_context(|| format!("{}\n{}", command.name, command.command_line()))?;
            tx.send((id, result)).await.unwrap();
        });
    }

    fn on_command_finished(&mut self, id: CommandId, result: ExecutionResult) {
        self.running -= 1;
        if result.success() {
            self.on_command_succeeded(id, result);
        } else {
            self.on_command_failed(id, result);
        }
    }

    /// Track state and check if reverse dependencies are ready
    fn on_command_succeeded(&mut self, id: CommandId, result: ExecutionResult) {
        self.succeeded.push(id);
        let command = &mut self.commands[id];
        command.schedule_state = ScheduleState::Succeeded;
        info!("Success {}: {:?}", command.name, result);
        for rdep_id in command.reverse_deps.clone() {
            let rdep = &mut self.commands[rdep_id];
            assert_eq!(rdep.schedule_state, ScheduleState::Waiting);
            assert!(!rdep.unfinished_deps.is_empty());
            rdep.unfinished_deps
                .swap_remove(rdep.unfinished_deps.iter().position(|x| *x == id).unwrap());
            if rdep.unfinished_deps.is_empty() {
                rdep.schedule_state = ScheduleState::Ready;
                self.waiting.remove(&rdep_id);
                self.ready.push_back(rdep_id);
            }
        }
    }

    fn on_command_failed(&mut self, id: CommandId, result: ExecutionResult) {
        self.failed.push(id);
        let command = &self.commands[id];
        info!("Error {}: {:?}", command.name, result);
    }
}
