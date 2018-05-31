use actix::*;
use futures::future::Future;

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
        let system = System::new("task-queue");
        let addr: Addr<Unsync, _> = TaskActor.start();
        for (input, state) in self.inputs.iter_mut() {
            println!("marking {} as started", input);
            *state = TaskState::STARTED;
            let cmd_req = CommandRequest(self.command.clone(), input.clone());
            let request = addr.send(cmd_req);
            Arbiter::handle().spawn(
                request
                    .map(|result| {
                        println!("result = {}", result);
                        if result {
                            println!("marking as done");
                        } else {
                            println!("marking as failed");
                        };
                    })
                    .map_err(|_| ()),
            );
        }

        system.run();
    }
}

#[derive(Debug)]
enum TaskState {
    WAITING,
    STARTED,
    FAILED,
    SUCCEEDED,
}

struct TaskActor;
impl Actor for TaskActor {
    type Context = Context<Self>;
}

struct CommandRequest(Vec<String>, String);
impl Message for CommandRequest {
    type Result = bool;
}
impl Handler<CommandRequest> for TaskActor {
    type Result = bool;

    fn handle(&mut self, msg: CommandRequest, ctx: &mut Context<Self>) -> Self::Result {
        println!("actually running {}", msg.1);
        let result = Command::new(&(msg.0)[0])
            .args(&(msg.0)[1..])
            .arg(msg.1)
            .spawn();
        result.is_ok()
    }
}
