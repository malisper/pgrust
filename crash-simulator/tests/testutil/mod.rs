//! Shared test scaffolding for simharness_runner_* integration tests.

use simharness::runner::driver::{ExecOutcome, Session};
use simharness::runner::planface::PlanHeader;

pub fn header(seed: u64) -> PlanHeader {
    PlanHeader {
        seed,
        profile: "test".into(),
        profile_sha256: "0".repeat(64),
        generator: "test".into(),
    }
}

pub enum MockBehavior {
    Ok,
    Rows(Vec<Vec<Option<String>>>),
    Error { sqlstate: String, message: String },
    Crash,
}

pub struct MockSession {
    pub name: String,
    pub behavior: MockBehavior,
    pub calls: Vec<String>,
    pub reconnects: u32,
}

impl MockSession {
    pub fn ok(name: &str) -> Self {
        MockSession { name: name.into(), behavior: MockBehavior::Ok, calls: vec![], reconnects: 0 }
    }
    pub fn with_rows(name: &str, rows: Vec<Vec<Option<String>>>) -> Self {
        MockSession {
            name: name.into(),
            behavior: MockBehavior::Rows(rows),
            calls: vec![],
            reconnects: 0,
        }
    }
    pub fn erroring(name: &str, sqlstate: &str, message: &str) -> Self {
        MockSession {
            name: name.into(),
            behavior: MockBehavior::Error { sqlstate: sqlstate.into(), message: message.into() },
            calls: vec![],
            reconnects: 0,
        }
    }
    pub fn crashing(name: &str) -> Self {
        MockSession {
            name: name.into(),
            behavior: MockBehavior::Crash,
            calls: vec![],
            reconnects: 0,
        }
    }
}

impl Session for MockSession {
    fn engine(&self) -> &str {
        &self.name
    }
    fn execute(&mut self, sql: &str) -> ExecOutcome {
        self.calls.push(sql.to_string());
        match &self.behavior {
            MockBehavior::Ok => {
                if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
                    ExecOutcome::Rows { rows: vec![] }
                } else {
                    ExecOutcome::Command { tag: "OK".into() }
                }
            }
            MockBehavior::Rows(rows) => ExecOutcome::Rows { rows: rows.clone() },
            MockBehavior::Error { sqlstate, message } => {
                ExecOutcome::SqlError { sqlstate: sqlstate.clone(), message: message.clone() }
            }
            MockBehavior::Crash => ExecOutcome::ConnectionLost { message: "server closed the connection".into() },
        }
    }
    fn reconnect(&mut self) -> Result<(), String> {
        self.reconnects += 1;
        Ok(())
    }
}
