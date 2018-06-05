extern crate chrono;
extern crate rusqlite;

use chrono::{DateTime, Utc};
use rusqlite::{Connection, Error};
use std::collections::BTreeMap;
use std::process::Command;

pub struct TaskQueue {
    conn: Connection,
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
                    start_time      TEXT,
                    finish_time     TEXT,
                    error_time      TEXT
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
                "INSERT INTO INPUTS (task_id, value) VALUES (?1, ?2)",
                &[&task_id, &input],
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

        let mut statement = self.conn
            .prepare("SELECT id, value FROM inputs WHERE task_id = ?1")?;
        let query = statement.query_map(&[&task_id], |row| {
            let input_id: i64 = row.get(0);
            let value: String = row.get(1);
            (input_id, value)
        })?;

        let inputs: BTreeMap<i64, String> = query.collect::<Result<_, _>>()?;
        for (_, input) in inputs.into_iter() {
            Command::new(&command)
                .arg(&input)
                .spawn()
                .expect("actually run command");
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
            .prepare("SELECT id, value FROM inputs WHERE task_id = ?1")?;
        let mut query = statement.query_map(&[&task_id], |row| {
            let input_id: i64 = row.get(0);
            let value: String = row.get(1);
            (input_id, value)
        })?;

        let inputs: BTreeMap<i64, String> = query.collect::<Result<_, _>>()?;
        for (_, input) in inputs.into_iter() {
            println!("  - {}", input);
        }
        Ok(())
    }
}
