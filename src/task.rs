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
        for (input, state) in self.inputs.iter_mut() {
            *state = TaskState::STARTED;
            let result = Command::new(&self.command[0])
                .args(&self.command[1..])
                .arg(input)
                .spawn();
            *state = if result.is_ok() {
                TaskState::SUCCEEDED
            } else {
                TaskState::FAILED
            };
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
