use chrono::{DateTime, Utc};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::{params, Connection, Error, Row};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};

pub struct TaskQueue {
    conn: Connection,
}

pub struct Task {
    id: i64,
    create_time: DateTime<Utc>,
    command: String,
}

impl Task {
    const INIT_STATEMENT: &'static str = "
        CREATE TABLE IF NOT EXISTS tasks (
            id              INTEGER PRIMARY KEY,
            create_time     TEXT NOT NULL,
            command         TEXT NOT NULL
        )";
    pub fn from_row(row: &Row) -> Result<Task, Error> {
        Ok(Task {
            id: row.get(0)?,
            create_time: row.get(1)?,
            command: row.get(2)?,
        })
    }
}

#[allow(dead_code)]
pub struct TaskInput {
    id: i64,
    task_id: i64,
    state: InputState,
    value: String,
    updated_at: DateTime<Utc>,
}

impl TaskInput {
    const INIT_STATEMENT: &'static str = "
        CREATE TABLE IF NOT EXISTS inputs (
            id              INTEGER PRIMARY KEY,
            task_id         INTEGER,
            value           TEXT NOT NULL,
            state           TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        )";
    pub fn from_row(row: &Row) -> Result<TaskInput, Error> {
        Ok(TaskInput {
            id: row.get(0)?,
            task_id: row.get(1)?,
            value: row.get(2)?,
            state: row.get(3)?,
            updated_at: row.get(4)?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum InputState {
    New,
    Started,
    Finished,
    Failed,
}

impl ToSql for InputState {
    fn to_sql(&self) -> Result<ToSqlOutput, Error> {
        Ok(ToSqlOutput::Owned(Value::Text(
            serde_json::to_string(self).unwrap(),
        )))
    }
}

impl FromSql for InputState {
    fn column_result(value: ValueRef) -> Result<Self, FromSqlError> {
        match value {
            ValueRef::Text(txt) => Ok(serde_json::from_slice(txt).unwrap()),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct CommandTemplate(Vec<String>);
impl CommandTemplate {
    fn run(&self, arg: &str) -> bool {
        Command::new(&(self.0)[0])
            .args(&(self.0)[1..])
            .arg(arg)
            .status()
            .unwrap()
            .success()
    }
}

impl Display for CommandTemplate {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        for s in &self.0 {
            write!(f, "{}", s)?;
        }
        Ok(())
    }
}

impl ToSql for CommandTemplate {
    fn to_sql(&self) -> Result<ToSqlOutput, Error> {
        Ok(ToSqlOutput::Owned(Value::Text(
            serde_json::to_string(self).unwrap(),
        )))
    }
}

impl FromSql for CommandTemplate {
    fn column_result(value: ValueRef) -> Result<Self, FromSqlError> {
        match value {
            ValueRef::Text(txt) => Ok(serde_json::from_slice(txt).unwrap()),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl TaskQueue {
    pub fn new() -> Result<TaskQueue, Error> {
        let location = TaskQueue::init_sqlite_file();
        let conn = Connection::open(&location)?;
        conn.execute(Task::INIT_STATEMENT, params![])?;
        conn.execute(TaskInput::INIT_STATEMENT, params![])?;
        Ok(TaskQueue { conn })
    }

    // Create a .sqlite file and return its location.
    // By convention this is at $HOME/.config/task-queue/tq.sqlite
    fn init_sqlite_file() -> PathBuf {
        let home = dirs::home_dir().expect("$HOME not defined");
        let dir = {
            let mut path = home.clone();
            path.push(".config");
            path.push("task-queue");
            path
        };
        std::fs::create_dir_all(&dir).unwrap();
        let location = {
            let mut path = dir;
            path.push("tq.sqlite");
            path
        };
        location
    }

    pub fn push_task(&mut self, command: Vec<String>, inputs: Vec<String>) -> Result<(), Error> {
        let now = Utc::now();

        let txn = self.conn.transaction()?;

        txn.execute(
            "INSERT INTO tasks (create_time, command) VALUES (?1, ?2)",
            params![&now, &CommandTemplate(command)],
        )?;

        let task_id = txn.last_insert_rowid();

        for input in inputs {
            txn.execute(
                "INSERT INTO INPUTS (task_id, value, state, updated_at) VALUES (?1, ?2, ?3, ?4)",
                params![&task_id, &input, &InputState::New, &Utc::now()],
            )?;
        }

        txn.commit()
    }

    pub fn remove_task(&mut self, task_id: i64) -> Result<(), Error> {
        self.conn
            .execute("DELETE FROM tasks WHERE id = ?1", &[&task_id])?;
        self.conn
            .execute("DELETE FROM inputs WHERE task_id = ?1", &[&task_id])?;
        Ok(())
    }

    pub fn list_tasks(&mut self) -> Result<(), Error> {
        for task in self.get_tasks()? {
            println!("[{}] {} --- {}", task.id, task.create_time, task.command);
        }
        Ok(())
    }

    pub fn run_task(&mut self, task_id: i64, concurrency: usize) -> Result<(), Error> {
        rayon::ThreadPoolBuilder::new()
            .num_threads(concurrency)
            .build_global()
            .unwrap();

        let command: CommandTemplate = self.conn.query_row(
            "SELECT command FROM tasks WHERE id = ?1",
            params![&task_id],
            |row| row.get(0),
        )?;

        let handle = Arc::new(Mutex::new(self));
        for i in 0.. {
            let inputs: Vec<TaskInput> = {
                let mut handle_mut = handle.lock().unwrap();
                handle_mut.get_inputs(task_id, 100 * i, 100)?
            };
            if inputs.is_empty() {
                break;
            }

            inputs.into_par_iter().for_each(|input| {
                // Skip inputs that are already in progress or finished.
                if input.state == InputState::Finished || input.state == InputState::Started {
                    return;
                }
                // Avoid race conditions: mark as started, only proceed if successful
                {
                    let mut handle_mut = handle.lock().unwrap();
                    if !handle_mut
                        .set_input_state(input.id, input.state, InputState::Started)
                        .unwrap_or(false)
                    {
                        return;
                    }
                }
                let new_state = if command.run(&input.value) {
                    InputState::Finished
                } else {
                    InputState::Failed
                };
                {
                    let mut handle_mut = handle.lock().unwrap();
                    handle_mut
                        .set_input_state(input.id, InputState::Started, new_state)
                        .unwrap();
                }
            });
        }
        Ok(())
    }

    pub fn show_task(&mut self, task_id: i64) -> Result<(), Error> {
        let command: CommandTemplate = self.conn.query_row(
            "SELECT command FROM tasks WHERE id = ?1",
            &[&task_id],
            |row| row.get(0),
        )?;

        println!("{:?}", command);

        for i in 0.. {
            let inputs = self.get_inputs(task_id, 100 * i, 100)?;
            if inputs.is_empty() {
                break;
            }
            for input in inputs {
                println!("  [{:?}] {}", input.state, input.value);
            }
        }

        Ok(())
    }

    pub fn clean(&mut self) -> Result<(), Error> {
        // Remove finished inputs.
        self.conn.execute(
            "DELETE FROM inputs WHERE state = ?1",
            &[&InputState::Finished],
        )?;
        // Remove tasks with no inputs (possibly because they all finished and were just cleaned).
        self.conn.execute(
            "DELETE FROM tasks WHERE NOT EXISTS (SELECT 1 FROM inputs WHERE task_id = tasks.id)",
            params![],
        )?;
        // Mark any in-progress inputs as failed.
        self.conn.execute(
            "UPDATE inputs SET state = ?1, updated_at = ?2 WHERE state = ?3",
            params![&InputState::Failed, &Utc::now(), &InputState::Started],
        )?;
        // Reclaim any space that has been freed up.
        self.conn.execute("VACUUM", params![])?;
        Ok(())
    }

    fn get_tasks(&mut self) -> Result<Vec<Task>, Error> {
        let mut statement = self.conn.prepare("SELECT * FROM tasks")?;
        let query = statement.query_map(params![], |row| Task::from_row(row))?;
        query.collect()
    }

    fn get_inputs(
        &mut self,
        task_id: i64,
        offset: u32,
        limit: u32,
    ) -> Result<Vec<TaskInput>, Error> {
        let mut statement = self
            .conn
            .prepare("SELECT * FROM inputs WHERE task_id = ? ORDER BY id LIMIT ?, ?")?;
        let query = statement.query_map(params![&task_id, &offset, &limit], |row| {
            TaskInput::from_row(row)
        })?;

        query.collect()
    }

    /** true iff the TaskInput row was actually mutated */
    fn set_input_state(
        &mut self,
        input_id: i64,
        expected_state: InputState,
        new_state: InputState,
    ) -> Result<bool, Error> {
        let mutated_count = self.conn.execute(
            "UPDATE inputs SET state = ?1 WHERE id = ?2 AND state = ?3",
            params![&new_state, &input_id, &expected_state],
        )?;
        if mutated_count == 1 {
            self.conn.execute(
                "UPDATE inputs SET updated_at = ?1 WHERE id = ?2",
                params![&Utc::now(), &input_id],
            )?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
