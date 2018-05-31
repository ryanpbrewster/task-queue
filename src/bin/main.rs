extern crate task_queue;
extern crate tempfile;

use task_queue::task::Task;

use std::io::BufRead;

fn main() -> Result<(), Box<std::error::Error>> {
    let command: Vec<String> = std::env::args().skip(1).collect();

    let inputs: Vec<String> = {
        let stdin = std::io::stdin();
        let lines = stdin.lock().lines().collect::<Result<_, _>>();
        lines?
    };

    let dir = tempfile::tempdir()?.into_path();

    let mut task = Task::new(command, inputs, dir);

    Ok(task.run())
}
