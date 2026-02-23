use serde::{Deserialize, Serialize};

#[allow(non_camel_case_types)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkerTag {
    linux,
    macos,
    windows,
    arm,
    aarch64,
    i686,
    x86_64,
    task,
    wasm,
    Service(String),
    Custom(String),
}

impl WorkerTag {
    pub fn local_default_tags() -> Vec<WorkerTag> {
        let os = if cfg!(target_os = "linux") {
            WorkerTag::linux
        } else if cfg!(target_os = "macos") {
            WorkerTag::macos
        } else if cfg!(target_os = "windows") {
            WorkerTag::windows
        } else {
            todo!("Unsupported target_os: {}", std::env::consts::OS);
        };
        let architecture = if cfg!(target_arch = "arm") {
            WorkerTag::arm
        } else if cfg!(target_arch = "aarch64") {
            WorkerTag::aarch64
        } else if cfg!(target_arch = "x86") {
            WorkerTag::i686
        } else if cfg!(target_arch = "x86_64") {
            WorkerTag::x86_64
        } else {
            todo!("Unsupported target_arch: {}", std::env::consts::ARCH);
        };
        vec![os, architecture, WorkerTag::task, WorkerTag::wasm]
    }
}
