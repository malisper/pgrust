use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use rand::random;
use serde::{Deserialize, Serialize};

use crate::backend::access::transam::xact::TransactionId;
use crate::backend::access::transam::xlog::{Lsn, WAL_SEG_SIZE_BYTES};
use crate::backend::utils::misc::checkpoint::CheckpointConfig;
use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;

const CONTROL_FILE_FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlFileState {
    ShutDown,
    InProduction,
    InCrashRecovery,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFile {
    pub format_version: u32,
    pub system_identifier: u64,
    pub state: ControlFileState,
    pub latest_checkpoint_lsn: Lsn,
    pub redo_lsn: Lsn,
    pub next_xid: TransactionId,
    pub checkpoint_timestamp_usecs: i64,
    pub full_page_writes: bool,
    pub wal_segment_size: u32,
}

impl ControlFile {
    pub fn bootstrap(next_xid: TransactionId, config: &CheckpointConfig) -> Self {
        Self {
            format_version: CONTROL_FILE_FORMAT_VERSION,
            system_identifier: random(),
            state: ControlFileState::ShutDown,
            latest_checkpoint_lsn: 0,
            redo_lsn: 0,
            next_xid,
            checkpoint_timestamp_usecs: current_postgres_timestamp_usecs(),
            full_page_writes: config.full_page_writes,
            wal_segment_size: WAL_SEG_SIZE_BYTES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlFileError {
    Io(String),
    Corrupt(String),
    Unsupported(String),
}

impl std::fmt::Display for ControlFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) => write!(f, "{message}"),
            Self::Corrupt(message) => write!(f, "{message}"),
            Self::Unsupported(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ControlFileError {}

pub struct ControlFileStore {
    path: PathBuf,
    inner: RwLock<ControlFile>,
}

impl ControlFileStore {
    pub fn path(base_dir: &Path) -> PathBuf {
        base_dir.join("global").join("pg_control.json")
    }

    pub fn load(base_dir: &Path) -> Result<Self, ControlFileError> {
        let path = Self::path(base_dir);
        let text = fs::read_to_string(&path).map_err(|err| ControlFileError::Io(err.to_string()))?;
        let control: ControlFile = serde_json::from_str(&text)
            .map_err(|err| ControlFileError::Corrupt(format!("invalid control file: {err}")))?;
        if control.format_version != CONTROL_FILE_FORMAT_VERSION {
            return Err(ControlFileError::Unsupported(format!(
                "unsupported control file format version {}",
                control.format_version
            )));
        }
        if control.wal_segment_size != WAL_SEG_SIZE_BYTES {
            return Err(ControlFileError::Unsupported(format!(
                "unsupported WAL segment size {}",
                control.wal_segment_size
            )));
        }
        Ok(Self {
            path,
            inner: RwLock::new(control),
        })
    }

    pub fn bootstrap(
        base_dir: &Path,
        next_xid: TransactionId,
        config: &CheckpointConfig,
    ) -> Result<Self, ControlFileError> {
        let path = Self::path(base_dir);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|err| ControlFileError::Io(err.to_string()))?;
        }
        let store = Self {
            path,
            inner: RwLock::new(ControlFile::bootstrap(next_xid, config)),
        };
        store.persist()?;
        Ok(store)
    }

    pub fn snapshot(&self) -> ControlFile {
        self.inner.read().clone()
    }

    pub fn update(
        &self,
        f: impl FnOnce(&mut ControlFile),
    ) -> Result<ControlFile, ControlFileError> {
        let mut control = self.inner.write();
        f(&mut control);
        control.checkpoint_timestamp_usecs = current_postgres_timestamp_usecs();
        persist_control_file(&self.path, &control)?;
        Ok(control.clone())
    }

    pub fn persist(&self) -> Result<(), ControlFileError> {
        let control = self.inner.read();
        persist_control_file(&self.path, &control)
    }
}

fn persist_control_file(path: &Path, control: &ControlFile) -> Result<(), ControlFileError> {
    let tmp_path = path.with_extension("json.tmp");
    let text = serde_json::to_string_pretty(control)
        .map_err(|err| ControlFileError::Io(err.to_string()))?;
    let mut file = fs::File::create(&tmp_path).map_err(|err| ControlFileError::Io(err.to_string()))?;
    file.write_all(text.as_bytes())
        .map_err(|err| ControlFileError::Io(err.to_string()))?;
    file.sync_data()
        .map_err(|err| ControlFileError::Io(err.to_string()))?;
    fs::rename(&tmp_path, path).map_err(|err| ControlFileError::Io(err.to_string()))?;
    Ok(())
}
