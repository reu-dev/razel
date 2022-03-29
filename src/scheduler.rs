use crate::Command;

pub struct Scheduler {
    queue: Vec<Box<dyn Command>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler { queue: Default::default() }
    }

    pub fn push(&mut self, command: Box<dyn Command>) {
        self.queue.push(command);
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        for command in self.queue.iter_mut() {
            command.exec().await?
        }
        Ok(())
    }
}
