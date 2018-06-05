extern crate chrono;
extern crate rusqlite;

use chrono::{DateTime, Utc};
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, ValueRef};
use rusqlite::{Connection, Error};
use std::collections::BTreeMap;
use std::process::Command;

pub struct TaskQueue {
    conn: Connection,
}

#[derive(Debug)]
enum InputState {
    New,
    Started(DateTime<Utc>),
    Finished(DateTime<Utc>),
    Failed(DateTime<Utc>),
}

impl InputState {
    pub fn char(&self) -> char {
        match *self {
            InputState::New => ' ',
            InputState::Started(_) => '.',
            InputState::Failed(_) => 'X',
            InputState::Finished(_) => 'o',
        }
    }
}

impl ToSql for InputState {
    fn to_sql(&self) -> Result<ToSqlOutput, Error> {
        Ok(ToSqlOutput::Borrowed(ValueRef::Text(match *self {
            InputState::New => "NEW",
            InputState::Started(_) => "STARTED",
            InputState::Finished(_) => "FINISHED",
            InputState::Failed(_) => "FAILED",
        })))
    }
}

impl FromSql for InputState {
    fn column_result(value: ValueRef) -> Result<Self, FromSqlError> {
        match value {
            ValueRef::Text(ref txt) => {
                if txt.starts_with("NEW") {
                    Ok(InputState::New)
                } else if txt.starts_with("STARTED") {
                    Ok(InputState::Started(Utc::now()))
                } else if txt.starts_with("FAILED") {
                    Ok(InputState::Failed(Utc::now()))
                } else if txt.starts_with("FINISHED") {
                    Ok(InputState::Finished(Utc::now()))
                } else {
                    Err(FromSqlError::InvalidType)
                }
            }
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl TaskQueue {
    pub fn new() -> Result<TaskQueue, Error> {
        Connection::open("file:rpb.sqlite").map(|conn| TaskQueue { conn })
    }

    pub fn push_task(&mut self, command: String, inputs: Vec<String>) -> Result<(), Error> {
        self.conn.execute_batch(
            "
                CREATE TABLE IF NOT EXISTS tasks (
                    id              INTEGER PRIMARY KEY,
                    create_time     TEXT NOT NULL,
                    command         TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS inputs (
                    id              INTEGER PRIMARY KEY,
                    task_id         INTEGER,
                    value           TEXT NOT NULL,
                    state           TEXT NOT NULL
                );",
        )?;

        let now = Utc::now();
        self.conn.execute(
            "INSERT INTO tasks (create_time, command) VALUES (?1, ?2)",
            &[&now, &command],
        )?;

        let task_id = self.conn.last_insert_rowid();

        for input in inputs {
            self.conn.execute(
                "INSERT INTO INPUTS (task_id, value, state) VALUES (?1, ?2, ?3)",
                &[&task_id, &input, &InputState::New],
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

        let inputs: BTreeMap<i64, String> = {
            let mut statement = self.conn
                .prepare("SELECT id, value FROM inputs WHERE task_id = ?1")?;
            let query = statement.query_map(&[&task_id], |row| {
                let input_id: i64 = row.get(0);
                let value: String = row.get(1);
                (input_id, value)
            })?;

            query.collect::<Result<_, _>>()?
        };
        for (input_id, input) in inputs.into_iter() {
            self.set_input_state(input_id, InputState::Started(Utc::now()))?;
            println!("starting: {} {}", command, input);
            let state = if Command::new(&command)
                .arg(&input)
                .status()
                .unwrap()
                .success()
            {
                InputState::Finished(Utc::now())
            } else {
                InputState::Failed(Utc::now())
            };
            self.set_input_state(input_id, state)?;
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

        let mut statement = self.conn
            .prepare("SELECT id, value, state FROM inputs WHERE task_id = ?1")?;
        let query = statement.query_map(&[&task_id], |row| {
            let input_id: i64 = row.get(0);
            let value: String = row.get(1);
            let state: InputState = row.get(2);
            (input_id, value, state)
        })?;

        let inputs: Vec<(i64, String, InputState)> = query.collect::<Result<_, _>>()?;
        for (_id, value, state) in inputs.into_iter() {
            println!("  [{}] {}", state.char(), value);
        }
        Ok(())
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
