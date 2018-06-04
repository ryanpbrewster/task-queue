extern crate chrono;
extern crate rusqlite;
#[macro_use]
extern crate structopt;

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::io::BufRead;
use std::process::Command;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "rpb", about = "Interact with your local task queue.")]
struct Opts {
    #[structopt(long = "verbose", help = "Add extra logging")]
    verbose: bool,

    #[structopt(subcommand)]
    op: Operation,
}

#[derive(StructOpt, Debug)]
enum Operation {
    #[structopt(name = "create")]
    Create { command: String },

    #[structopt(name = "delete")]
    Delete { task_ids: Vec<i64> },

    #[structopt(name = "list")]
    List,

    #[structopt(name = "run")]
    Run { task_id: i64 },

    #[structopt(name = "show")]
    Show { task_id: i64 },
}

fn main() {
    let opts = Opts::from_args();

    let conn = Connection::open("file:rpb.sqlite").unwrap();

    match opts.op {
        Operation::Create { command } => {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS tasks (
                    id              INTEGER PRIMARY KEY,
                    create_time     TEXT NOT NULL,
                    command         TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inputs (
                    id              INTEGER PRIMARY KEY,
                    task_id         INTEGER,
                    value           TEXT NOT NULL
                );",
            ).expect("create tables");

            let now = Utc::now();
            conn.execute(
                "INSERT INTO tasks (create_time, command) VALUES (?1, ?2)",
                &[&now, &command],
            ).expect("insert into `tasks`");

            let task_id = conn.last_insert_rowid();

            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                let input = line.expect("read line from stdin");
                conn.execute(
                    "INSERT INTO INPUTS (task_id, value) VALUES (?1, ?2)",
                    &[&task_id, &input],
                ).expect("insert into `inputs`");
            }
        }
        Operation::Delete { task_ids } => {
            for task_id in task_ids {
                conn.execute("DELETE FROM tasks WHERE id = ?1", &[&task_id])
                    .expect("delete row from `tasks`");
                conn.execute("DELETE FROM inputs WHERE task_id = ?1", &[&task_id])
                    .expect("delete rows from `inputs`");
            }
        }
        Operation::List => {
            let mut statement = conn.prepare(
                "
                 SELECT id, create_time, command,
                 (SELECT COUNT(1) FROM inputs WHERE task_id = tasks.id)
                 FROM tasks",
            ).expect("prepare query for `tasks`");
            let mut query = statement.query(&[]).expect("perform query on `tasks`");
            while let Some(Ok(row)) = query.next() {
                let id: i64 = row.get(0);
                let create_time: DateTime<Utc> = row.get(1);
                let command: String = row.get(2);
                let input_count: u32 = row.get(3);
                println!(
                    "[{}] {} --- {} inputs --- {}",
                    id, create_time, input_count, command
                );
            }
        }
        Operation::Run { task_id } => {
            let mut command: String = conn.query_row(
                "SELECT command FROM tasks WHERE id = ?1",
                &[&task_id],
                |row| row.get(0),
            ).expect("query row");

            let mut statement = conn.prepare("SELECT id, value FROM inputs WHERE task_id = ?1")
                .expect("prepare query for `inputs`");
            let mut query = statement
                .query_map(&[&task_id], |row| {
                    let input_id: i64 = row.get(0);
                    let value: String = row.get(1);
                    (input_id, value)
                })
                .expect("perform query on `inputs`");

            let inputs: BTreeMap<i64, String> =
                query.collect::<Result<_, _>>().expect("aggregate inputs");
            for (_, input) in inputs.into_iter() {
                Command::new(&command)
                    .arg(&input)
                    .spawn()
                    .expect("run command");
            }
        }
        Operation::Show { task_id } => {
            let mut command: String = conn.query_row(
                "SELECT command FROM tasks WHERE id = ?1",
                &[&task_id],
                |row| row.get(0),
            ).expect("query row");

            println!("{}", command);

            let mut statement = conn.prepare("SELECT id, value FROM inputs WHERE task_id = ?1")
                .expect("prepare query for `inputs`");
            let mut query = statement
                .query_map(&[&task_id], |row| {
                    let input_id: i64 = row.get(0);
                    let value: String = row.get(1);
                    (input_id, value)
                })
                .expect("perform query on `inputs`");

            let inputs: BTreeMap<i64, String> =
                query.collect::<Result<_, _>>().expect("aggregate inputs");
            for (_, input) in inputs.into_iter() {
                println!("  - {}", input);
            }
        }
    };
}
