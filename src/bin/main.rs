#[macro_use]
extern crate structopt;
extern crate task_queue;

use task_queue::TaskQueue;

use std::io::BufRead;
use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "tq", about = "Interact with your local task queue.")]
struct Opts {
    #[structopt(long = "verbose", help = "Add extra logging")]
    verbose: bool,

    #[structopt(subcommand)]
    op: Operation,
}

#[derive(StructOpt, Debug)]
enum Operation {
    #[structopt(name = "clean")]
    Clean,

    #[structopt(name = "create")]
    #[structopt(raw(settings = "&[AppSettings::TrailingVarArg]"))]
    Create { command: Vec<String> },

    #[structopt(name = "delete")]
    Delete { task_ids: Vec<i64> },

    #[structopt(name = "list")]
    List,

    #[structopt(name = "run")]
    Run {
        task_id: i64,
        #[structopt(
            long = "concurrency", help = "How many tasks to run in parallel", default_value = "1"
        )]
        concurrency: usize,
    },

    #[structopt(name = "show")]
    Show { task_id: i64 },
}

fn main() {
    let opts = Opts::from_args();

    let mut tq = TaskQueue::new().unwrap();

    match opts.op {
        Operation::Clean => {
            tq.clean().unwrap();
        }
        Operation::Create { command } => {
            let stdin = std::io::stdin();
            let inputs: Vec<String> = stdin.lock().lines().collect::<Result<Vec<_>, _>>().unwrap();
            tq.push_task(command, inputs).unwrap();
        }
        Operation::Delete { task_ids } => {
            for task_id in task_ids {
                tq.remove_task(task_id).unwrap();
            }
        }
        Operation::List => {
            tq.list_tasks().unwrap();
        }
        Operation::Run {
            task_id,
            concurrency,
        } => {
            tq.run_task(task_id, concurrency).unwrap();
        }
        Operation::Show { task_id } => {
            tq.show_task(task_id).unwrap();
        }
    };
}
