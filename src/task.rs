use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::Command;

#[derive(Debug)]
pub struct Task {
    command: Vec<String>,
    dir: PathBuf,
    inputs: BTreeMap<String, TaskState>,
}

impl Task {
    pub fn new(command: Vec<String>, inputs: Vec<String>, dir: PathBuf) -> Task {
        Task {
            command,
            dir,
            inputs: inputs
                .into_iter()
                .map(|i| (i, TaskState::WAITING))
                .collect(),
        }
    }

    pub fn run(&mut self) {
        for input in self.inputs.keys() {
            Command::new(&self.command[0])
                .args(&self.command[1..])
                .arg(input)
                .spawn()
                .expect("tried running command");
        }
    }
}

#[derive(Debug)]
enum TaskState {
    WAITING,
    STARTED,
    FAILED,
    SUCCEEDED,
}
