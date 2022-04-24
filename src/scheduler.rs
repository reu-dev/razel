use std::collections::HashMap;
use std::fs;

use anyhow::{bail, Context};
use log::info;

use crate::{config, Arena, Command, CommandBuilder, File, FileId};

pub struct Scheduler {
    files: Arena<File>,
    path_to_file_id: HashMap<String, FileId>,
    commands: Arena<Command>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            files: Default::default(),
            path_to_file_id: Default::default(),
            commands: Default::default(),
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
    ) -> Result<(), anyhow::Error> {
        let mut builder = CommandBuilder::new(name, args);
        builder.inputs(&inputs, self)?;
        builder.outputs(&outputs, self)?;
        builder.custom_command_executor(executable);
        let command = builder.build();
        self.push(command)
    }

    pub fn push(&mut self, command: Command) -> Result<(), anyhow::Error> {
        let id = self.commands.alloc(command);
        // TODO check if name is unique
        // patch outputs.creating_command
        for output_id in &self.commands[id].outputs {
            let output = &mut self.files[*output_id];
            assert!(output.creating_command.is_none());
            output.creating_command = Some(id);
        }
        Ok(())
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        fs::create_dir_all(config::BIN_DIR)?;
        for command in self.commands.iter() {
            info!("Execute {}: {}", command.name, command.command_line());
            command
                .exec()
                .await
                .with_context(|| format!("{}\n{}", command.name, command.command_line()))?
        }
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
}
