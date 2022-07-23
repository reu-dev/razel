use anyhow::{bail, Context};
use std::fs;
use std::fs::{read_to_string, File};
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::str::FromStr;

/** Reproduces what the K8s kubelet does to calculate memory.available relative to root cgroup.

see https://kubernetes.io/docs/concepts/scheduling-eviction/node-pressure-eviction/ */
pub fn get_available_memory() -> Result<u64, anyhow::Error> {
    let memory_capacity = procfs::Meminfo::new()?.mem_total;
    let cgroup = CGroup::new("".into());
    let memory_usage = cgroup.read::<u64>("memory", "memory.usage_in_bytes")?;
    let memory_total_inactive_file =
        cgroup.read_field::<u64>("memory", "memory.stat", "total_inactive_file")?;
    let memory_working_set = if memory_usage < memory_total_inactive_file {
        0
    } else {
        memory_usage - memory_total_inactive_file
    };
    let memory_available = memory_capacity - memory_working_set;
    Ok(memory_available)
}

#[derive(Clone)]
pub struct CGroup {
    group: String,
}

impl CGroup {
    pub fn new(group: String) -> Self {
        Self { group }
    }

    pub fn create(&self, controller: &str) -> Result<(), anyhow::Error> {
        let path = self.path(controller, "");
        let dir = path.parent().unwrap();
        fs::create_dir_all(&dir)?;
        Ok(())
    }

    pub fn add_task(&self, controller: &str, pid: u32) -> Result<(), anyhow::Error> {
        self.write(controller, "tasks", pid)
    }

    pub fn read<T>(&self, controller: &str, file: &str) -> Result<T, anyhow::Error>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let path = self.path(controller, file);
        let string = read_to_string(&path).with_context(|| format!("Failed to read {:?}", path))?;
        let value = string.trim().parse::<T>()?;
        Ok(value)
    }

    pub fn read_field<T>(
        &self,
        controller: &str,
        file: &str,
        field: &str,
    ) -> Result<T, anyhow::Error>
    where
        T: FromStr,
        <T as FromStr>::Err: std::error::Error + Send + Sync + 'static,
    {
        let path = self.path(controller, file);
        let file = File::open(&path).with_context(|| format!("Failed to read {:?}", path))?;
        let lines = BufReader::new(file).lines();
        for line in lines {
            if let Some(string) = line?.strip_prefix(field) {
                let value = string.trim().parse::<T>()?;
                return Ok(value);
            }
        }
        bail!("field not found: {}", field);
    }

    pub fn write<T>(&self, controller: &str, file: &str, value: T) -> Result<(), anyhow::Error>
    where
        T: std::fmt::Display,
    {
        let path = self.path(controller, file);
        std::fs::write(&path, value.to_string())?;
        Ok(())
    }

    fn path(&self, controller: &str, file: &str) -> PathBuf {
        PathBuf::from("/sys/fs/cgroup")
            .join(controller)
            .join(&self.group)
            .join(file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn available_memory() {
        println!("available_memory: {}", get_available_memory().unwrap());
    }

    #[test]
    fn cgroup_razel() {
        let cgroup = CGroup::new("razel".into());
        cgroup.create("memory").unwrap();
        cgroup
            .write("memory", "memory.limit_in_bytes", 60000000)
            .unwrap();
        cgroup.write("memory", "memory.swappiness", 1).unwrap();
        println!(
            "memory.limit_in_bytes: {:?}",
            cgroup.read::<u64>("memory", "memory.limit_in_bytes")
        );
        println!(
            "memory.swappiness: {:?}",
            cgroup.read::<i32>("memory", "memory.swappiness")
        );

        println!(
            "tasks before: {:?}",
            cgroup.read::<String>("memory", "tasks")
        );
        cgroup.add_task("memory", std::process::id()).unwrap();
        println!(
            "tasks after: {:?}",
            cgroup.read::<String>("memory", "tasks")
        );
    }
}
