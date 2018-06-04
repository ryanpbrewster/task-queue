extern crate chrono;
extern crate rusqlite;
#[macro_use]
extern crate structopt;

use chrono::{DateTime, Utc};
use rusqlite::Connection;
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
    Create,

    #[structopt(name = "delete")]
    Delete { task_ids: Vec<i64> },

    #[structopt(name = "list")]
    List,
}

fn main() {
    let opts = Opts::from_args();

    let conn = Connection::open("file:rpb.sqlite").unwrap();

    match opts.op {
        Operation::Create => {
            conn.execute_batch(
                "
                CREATE TABLE IF NOT EXISTS tasks (
                    id              INTEGER PRIMARY KEY,
                    name            TEXT NOT NULL,
                    create_time     TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inputs (
                    id              INTEGER PRIMARY KEY,
                    task_id         INTEGER,
                    value           TEXT NOT NULL
                );",
            ).expect("create tables");

            let now = Utc::now();
            conn.execute(
                "INSERT INTO tasks (create_time, name) VALUES (?1, ?2)",
                &[&now, &"rpb1"],
            ).expect("insert into `tasks` table");

            let task_id = conn.last_insert_rowid();

            conn.execute(
                "INSERT INTO INPUTS (task_id, value) VALUES (?1, ?2)",
                &[&task_id, &format!("asdf-{}", task_id)],
            ).expect("insert into `inputs` table");
        }
        Operation::Delete { task_ids } => {
            for id in task_ids {
                conn.execute("DELETE FROM tasks WHERE id = ?1", &[&id])
                    .expect("delete row from `tasks` table");
            }
        }
        Operation::List => {
            let mut statement = conn.prepare("SELECT id, create_time, name FROM tasks")
                .expect("prepare list query");
            let mut query = statement.query(&[]).expect("perform list query");
            while let Some(Ok(row)) = query.next() {
                let id: i64 = row.get(0);
                let create_time: DateTime<Utc> = row.get(1);
                let name: String = row.get(2);
                println!("[{}] {} --- {}", id, create_time, name);
            }
        }
    };
}
