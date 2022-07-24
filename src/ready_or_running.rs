use crate::executors::Executor;
use crate::{Command, CommandId};
use itertools::Itertools;
use log::info;
use std::collections::HashMap;

type Group = String;

struct ReadyItem {
    id: CommandId,
    group: Group,
    slots: usize,
}

/// Keeps track of ready/running commands and selects next to run depending on resources
pub struct ReadyOrRunning {
    available_slots: usize,
    used_slots: usize,
    // TODO sort by weight, e.g. recursive number of rdeps
    ready_items: Vec<ReadyItem>,
    running_items: HashMap<CommandId, Group>,
    /// groups commands by estimated resource requirement
    group_to_slots: HashMap<String, usize>,
}

impl ReadyOrRunning {
    pub fn new(available_slots: usize) -> Self {
        Self {
            available_slots,
            used_slots: 0,
            ready_items: Default::default(),
            running_items: Default::default(),
            group_to_slots: Default::default(),
        }
    }

    pub fn ready(&self) -> usize {
        self.ready_items.len()
    }

    pub fn ready_ids(&self) -> Vec<CommandId> {
        self.ready_items.iter().map(|x| x.id).collect()
    }

    pub fn running(&self) -> usize {
        self.running_items.len()
    }

    pub fn len(&self) -> usize {
        self.ready_items.len() + self.running_items.len()
    }

    pub fn push_ready(&mut self, command: &Command) {
        let group = Self::group_for_command(command);
        let slots = self.slots_for_group(&group);
        self.ready_items.push(ReadyItem {
            id: command.id,
            group,
            slots,
        });
    }

    pub fn pop_ready_and_run(&mut self) -> Option<CommandId> {
        if self.used_slots >= self.available_slots || self.ready_items.is_empty() {
            return None;
        }
        let free_slots = self.available_slots - self.used_slots;
        if let Some((index, _)) = self
            .ready_items
            .iter()
            .find_position(|x| x.slots <= free_slots)
        {
            let item = self.ready_items.swap_remove(index);
            self.running_items.insert(item.id, item.group);
            self.used_slots += item.slots;
            Some(item.id)
        } else {
            None
        }
    }

    pub fn set_finished_and_get_retry_flag(&mut self, id: CommandId, killed: bool) -> bool {
        let group = self.running_items.remove(&id).unwrap();
        let slots = self.slots_for_group(&group);
        assert!(self.used_slots >= slots);
        self.used_slots -= slots;
        if killed && self.scale_up_memory_requirement(&group) {
            self.ready_items.push(ReadyItem { id, group, slots });
            true
        } else {
            false
        }
    }

    fn scale_up_memory_requirement(&mut self, group: &Group) -> bool {
        let slots_old = self.slots_for_group(group);
        let slots_new = (slots_old * 2).min(self.available_slots);
        if slots_new == slots_old {
            return false;
        }
        info!("scale_up_memory_requirement({group}): {slots_old} -> {slots_new}");
        self.group_to_slots.insert(group.clone(), slots_new);
        let running_in_group = self
            .running_items
            .iter()
            .filter(|(_, x)| *x == group)
            .count();
        self.used_slots += running_in_group * (slots_new - slots_old);
        self.ready_items
            .iter_mut()
            .filter(|x| x.group == *group)
            .for_each(|x| x.slots = slots_new);
        true
    }

    fn slots_for_group(&self, group: &Group) -> usize {
        *self.group_to_slots.get(group).unwrap_or(&1)
    }

    fn group_for_command(command: &Command) -> Group {
        // assume resource requirements depends just on executable
        // could also use the command line with file arguments stripped
        match &command.executor {
            Executor::CustomCommand(c) => c.executable.clone(),
            Executor::Task(_) => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Arena, CommandBuilder, Scheduler};

    fn create(available_slots: usize, executables: Vec<&str>) -> ReadyOrRunning {
        let mut ready_or_running = ReadyOrRunning::new(available_slots);
        let mut scheduler = Scheduler::new();
        let mut commands: Arena<Command> = Default::default();
        for executable in &executables {
            let mut builder =
                CommandBuilder::new(format!("cmd_{}", ready_or_running.len()), vec![]);
            builder
                .custom_command_executor(executable.to_string(), Default::default(), &mut scheduler)
                .unwrap();
            let id = commands.alloc_with_id(|id| builder.build(id));
            ready_or_running.push_ready(&commands[id]);
        }
        assert_eq!(ready_or_running.ready(), executables.len());
        ready_or_running
    }

    #[test]
    fn simple() {
        let mut ror = create(3, vec!["exec_0", "exec_0", "exec_0", "exec_0"]);
        let c0 = ror.pop_ready_and_run().unwrap();
        let c1 = ror.pop_ready_and_run().unwrap();
        let c2 = ror.pop_ready_and_run().unwrap();
        assert_eq!(ror.pop_ready_and_run(), None);
        assert_eq!(ror.set_finished_and_get_retry_flag(c1, false), false);
        let c3 = ror.pop_ready_and_run().unwrap();
        assert_eq!(ror.set_finished_and_get_retry_flag(c0, false), false);
        assert_eq!(ror.set_finished_and_get_retry_flag(c2, false), false);
        assert_eq!(ror.set_finished_and_get_retry_flag(c3, false), false);
        assert_eq!(ror.len(), 0);
    }
}
