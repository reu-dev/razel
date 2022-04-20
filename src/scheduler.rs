use anyhow::Context;
use log::info;

use crate::Command;

pub struct Scheduler {
    queue: Vec<Box<Command>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            queue: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn push(&mut self, command: Box<Command>) {
        self.queue.push(command);
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        for command in self.queue.iter_mut() {
            info!("Execute {}: {}", command.name, command.command_line);
            command
                .exec()
                .await
                .with_context(|| format!("{}\n{}", command.name, command.command_line))?
        }
        Ok(())
    }
}
