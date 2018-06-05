extern crate chrono;
extern crate rusqlite;

extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use chrono::{DateTime, Utc};
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::{Connection, Error, Row};
use std::collections::BTreeMap;
use std::process::Command;

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
}

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
            id: row.get_checked(0)?,
            task_id: row.get_checked(1)?,
            value: row.get_checked(2)?,
            state: row.get_checked(3)?,
            updated_at: row.get_checked(4)?,
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
            ValueRef::Text(ref txt) => Ok(serde_json::from_str(txt).unwrap()),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl TaskQueue {
    pub fn new() -> Result<TaskQueue, Error> {
        let tq = Connection::open("file:rpb.sqlite").map(|conn| TaskQueue { conn })?;
        tq.conn.execute(Task::INIT_STATEMENT, &[])?;
        tq.conn.execute(TaskInput::INIT_STATEMENT, &[])?;
        Ok(tq)
    }

    pub fn push_task(&mut self, command: String, inputs: Vec<String>) -> Result<(), Error> {
        let now = Utc::now();
        self.conn.execute(
            "INSERT INTO tasks (create_time, command) VALUES (?1, ?2)",
            &[&now, &command],
        )?;

        let task_id = self.conn.last_insert_rowid();

        for input in inputs {
            self.conn.execute(
                "INSERT INTO INPUTS (task_id, value, state, updated_at) VALUES (?1, ?2, ?3, ?4)",
                &[&task_id, &input, &InputState::New, &Utc::now()],
            )?;
        }
        Ok(())
    }

    pub fn remove_task(&mut self, task_id: i64) -> Result<(), Error> {
        self.conn
            .execute("DELETE FROM tasks WHERE id = ?1", &[&task_id])?;
        self.conn
            .execute("DELETE FROM inputs WHERE task_id = ?1", &[&task_id])?;
        Ok(())
    }

    pub fn list_tasks(&mut self) -> Result<(), Error> {
        let mut statement = self.conn.prepare(
            "
                 SELECT id, create_time, command,
                 (SELECT COUNT(1) FROM inputs WHERE task_id = tasks.id)
                 FROM tasks",
        )?;
        let mut query = statement.query(&[])?;
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
        Ok(())
    }

    pub fn run_task(&mut self, task_id: i64) -> Result<(), Error> {
        let command: String = self.conn.query_row(
            "SELECT command FROM tasks WHERE id = ?1",
            &[&task_id],
            |row| row.get(0),
        )?;

        let inputs: Vec<TaskInput> = self.get_inputs(task_id)?;
        for input in inputs {
            if input.state == InputState::Finished {
                continue;
            }
            self.set_input_state(input.id, InputState::Started)?;
            println!("starting: {} {}", command, input.value);
            let result = Command::new(&command).arg(&input.value).status().unwrap();
            let state = if result.success() {
                InputState::Finished
            } else {
                InputState::Failed
            };
            self.set_input_state(input.id, state)?;
        }
        Ok(())
    }

    pub fn show_task(&mut self, task_id: i64) -> Result<(), Error> {
        let command: String = self.conn.query_row(
            "SELECT command FROM tasks WHERE id = ?1",
            &[&task_id],
            |row| row.get(0),
        )?;

        println!("{}", command);

        let inputs = self.get_inputs(task_id)?;
        for input in inputs {
            println!("  [{:?}] {}", input.state, input.value);
        }

        Ok(())
    }

    fn get_inputs(&mut self, task_id: i64) -> Result<Vec<TaskInput>, Error> {
        let mut statement = self.conn
            .prepare("SELECT * FROM inputs WHERE task_id = ?1")?;
        let query = statement.query_map(&[&task_id], |row| TaskInput::from_row(row).unwrap())?;

        query.collect()
    }
    fn set_input_state(&mut self, input_id: i64, state: InputState) -> Result<(), Error> {
        self.conn
            .execute(
                "UPDATE inputs SET state = ?1 WHERE id = ?2 LIMIT 1",
                &[&state, &input_id],
            )
            .map(|_| ())
    }
}
