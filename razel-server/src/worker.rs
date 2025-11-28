#![allow(unused)] // TODO remove

use crate::{Node, RemoteNode};
use anyhow::Result;
use razel::remote_exec::{Job, JobId};
use std::{collections::HashMap, net::SocketAddr};

pub enum Worker {
    Local(LocalWorker),
    Remote(RemoteWorker),
}

/// Worker running within the server process
pub struct LocalWorker {
    node: Node,
}

/// Stub for a remote worker
pub struct RemoteWorker {
    node: Node,
}
