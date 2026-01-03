use crate::config;
use crate::types::{DependencyGraph, ExecutableType, FileId, Target, TargetKind};
use anyhow::Result;
use itertools::{Itertools, chain};
use std::collections::HashSet;
use std::path::Path;

impl DependencyGraph {
    pub fn write_graphs_html(&self, excluded_commands_len: usize, output: &Path) -> Result<()> {
        let template = include_str!("graphs.in.html");
        let ignored_files = self.collect_ignored_files();
        let contents = if self.targets.len() - excluded_commands_len < 100
            && self.files.len() - ignored_files.len() < 100
        {
            template
                .replacen(
                    "{{graph_with_subgraphs}}",
                    &self.graph_with_subgraphs(&ignored_files)?,
                    1,
                )
                .replacen("{{graph_simple}}", &self.graph_simple(&ignored_files)?, 1)
        } else {
            "Skipped generating graphs because of too many commands/files.".into()
        };
        std::fs::write(output, contents)?;
        Ok(())
    }

    fn collect_ignored_files(&self) -> HashSet<FileId> {
        HashSet::from_iter(
            self.files
                .iter()
                .filter(|x| {
                    x.is_excluded
                        || x.executable.as_ref().is_some_and(|x| {
                            matches!(
                                x,
                                ExecutableType::SystemExecutable | ExecutableType::RazelExecutable
                            )
                        })
                })
                .map(|x| x.id),
        )
    }

    fn graph_with_subgraphs(&self, ignored_files: &HashSet<FileId>) -> Result<String> {
        let path = |x: FileId| self.files[x].path.strip_prefix(config::OUT_DIR).unwrap();
        let mut lines = vec![];
        for (id, p) in self
            .files
            .iter()
            // TODO .filter(|x| x.file_type != FileType::OutputFile)
            .filter(|x| !ignored_files.contains(&x.id))
            .map(|x| (x.id, &x.path))
        {
            lines.push(format!("f{id}([{p:?}])"));
            lines.push(format!("style f{id} fill:#fff4dd"));
        }
        for command in self.targets.iter().filter(|c| !c.is_excluded) {
            let command_id = command.id;
            lines.push(format!("subgraph cg{command_id} [{}]", command.name));
            lines.push(format!("ce{command_id}[[{}]]", executable(command)));
            for (x, path) in command
                .outputs
                .iter()
                .filter(|&x| !ignored_files.contains(x))
                .map(|x| (x, path(*x)))
            {
                lines.push(format!("f{x}([{path:?}])"));
                lines.push(format!("style f{x} fill:#fff4dd"));
            }
            lines.push("end".into());
            for x in chain(command.executables.iter(), command.inputs.iter())
                .filter(|&x| !ignored_files.contains(x))
            {
                lines.push(format!("f{x} --> cg{command_id}"));
            }
        }
        Ok(mermaid(&lines))
    }

    fn graph_simple(&self, ignored_files: &HashSet<FileId>) -> Result<String> {
        let path = |x: FileId| self.files[x].path.strip_prefix(config::OUT_DIR).unwrap();
        let mut lines = vec![];
        for (id, p) in self
            .files
            .iter()
            // TODO .filter(|x| x.file_type == FileType::DataFile)
            .filter(|x| !ignored_files.contains(&x.id))
            .map(|x| (x.id, &x.path))
        {
            lines.push(format!("f{id}[{p:?}]"));
            lines.push(format!("style f{id} fill:#fff4dd"));
        }
        for command in self.targets.iter().filter(|c| !c.is_excluded) {
            let command_id = command.id;
            if command.inputs.len() == 1 && command.outputs.len() == 1 {
                let input_id = command.inputs.first().unwrap();
                let output_id = *command.outputs.first().unwrap();
                lines.push(format!(
                    "f{input_id}-- \"{}\" -->f{output_id}[{:?}]",
                    executable(command),
                    path(output_id)
                ));
                lines.push(format!("style f{output_id} fill:#fff4dd"));
            } else {
                lines.push(format!("c{command_id}([\"{}\"])", executable(command)));
                for x in command.inputs.iter() {
                    lines.push(format!("f{x}---c{command_id}"));
                }
                for x in command.outputs.iter().cloned() {
                    lines.push(format!("c{command_id} --> f{x}[{:?}]", path(x)));
                    lines.push(format!("style f{x} fill:#fff4dd"));
                }
            }
        }
        Ok(mermaid(&lines))
    }
}

fn mermaid(lines: &[String]) -> String {
    // defaultRenderer is useless for non-tiny number of nodes
    // useMaxWidth breaks zooming/panning
    let config = r#"%%{init: {"flowchart": {"defaultRenderer": "elk", "useMaxWidth": false}} }%%"#;
    format!(
        r#"
<pre class="mermaid">
flowchart LR
{config}
{}
</pre>
    "#,
        lines.join("\n")
    )
}

fn executable(target: &Target) -> String {
    match &target.kind {
        TargetKind::Command(x) | TargetKind::Wasi(x) => x.executable.clone(),
        TargetKind::Task(x) | TargetKind::HttpRemoteExecTask(x) => x.args.iter().take(3).join(" "),
    }
}
