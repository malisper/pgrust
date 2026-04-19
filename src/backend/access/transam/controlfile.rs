use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use parking_lot::RwLock;
use rand::random;

use crate::backend::access::transam::xact::TransactionId;
use crate::backend::access::transam::xlog::{Lsn, WAL_SEG_SIZE_BYTES};
use crate::backend::utils::misc::checkpoint::CheckpointConfig;
use crate::backend::utils::time::datetime::current_postgres_timestamp_usecs;

const CONTROL_FILE_MAGIC: u32 = 0x5052_4354;
const CONTROL_FILE_FORMAT_VERSION: u32 = 1;
const CONTROL_FILE_ENCODED_LEN: usize = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlFileState {
    ShutDown,
    InProduction,
    InCrashRecovery,
}

impl ControlFileState {
    fn encode(self) -> u32 {
        match self {
            Self::ShutDown => 0,
            Self::InProduction => 1,
            Self::InCrashRecovery => 2,
        }
    }

    fn decode(raw: u32) -> Result<Self, ControlFileError> {
        match raw {
            0 => Ok(Self::ShutDown),
            1 => Ok(Self::InProduction),
            2 => Ok(Self::InCrashRecovery),
            other => Err(ControlFileError::Corrupt(format!(
                "invalid control file state {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    path: Option<PathBuf>,
    inner: RwLock<ControlFile>,
}

impl ControlFileStore {
    pub fn path(base_dir: &Path) -> PathBuf {
        base_dir.join("global").join("pg_control")
    }

    pub fn load(base_dir: &Path) -> Result<Self, ControlFileError> {
        let path = Self::path(base_dir);
        let bytes = fs::read(&path).map_err(|err| ControlFileError::Io(err.to_string()))?;
        let control = decode_control_file(&bytes)?;
        Ok(Self {
            path: Some(path),
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
            path: Some(path),
            inner: RwLock::new(ControlFile::bootstrap(next_xid, config)),
        };
        store.persist()?;
        Ok(store)
    }

    /// Create an in-memory control file store. Used by wasm, tests, and any
    /// embedded/ephemeral cluster that has no filesystem backing. `update()`
    /// and `persist()` become no-ops on the disk side (they still mutate the
    /// in-memory snapshot for `update`).
    pub fn new_in_memory(next_xid: TransactionId, config: &CheckpointConfig) -> Self {
        Self {
            path: None,
            inner: RwLock::new(ControlFile::bootstrap(next_xid, config)),
        }
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
        if let Some(path) = self.path.as_ref() {
            persist_control_file(path, &control)?;
        }
        Ok(control.clone())
    }

    pub fn persist(&self) -> Result<(), ControlFileError> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        let control = self.inner.read();
        persist_control_file(path, &control)
    }
}

fn persist_control_file(path: &Path, control: &ControlFile) -> Result<(), ControlFileError> {
    let tmp_path = path.with_extension("tmp");
    let bytes = encode_control_file(control);
    let mut file =
        fs::File::create(&tmp_path).map_err(|err| ControlFileError::Io(err.to_string()))?;
    file.write_all(&bytes)
        .map_err(|err| ControlFileError::Io(err.to_string()))?;
    file.sync_data()
        .map_err(|err| ControlFileError::Io(err.to_string()))?;
    fs::rename(&tmp_path, path).map_err(|err| ControlFileError::Io(err.to_string()))?;
    Ok(())
}

fn encode_control_file(control: &ControlFile) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(CONTROL_FILE_ENCODED_LEN);
    bytes.extend_from_slice(&CONTROL_FILE_MAGIC.to_le_bytes());
    bytes.extend_from_slice(&control.format_version.to_le_bytes());
    bytes.extend_from_slice(&control.system_identifier.to_le_bytes());
    bytes.extend_from_slice(&control.state.encode().to_le_bytes());
    bytes.extend_from_slice(&control.latest_checkpoint_lsn.to_le_bytes());
    bytes.extend_from_slice(&control.redo_lsn.to_le_bytes());
    bytes.extend_from_slice(&control.next_xid.to_le_bytes());
    bytes.extend_from_slice(&control.checkpoint_timestamp_usecs.to_le_bytes());
    bytes.push(u8::from(control.full_page_writes));
    bytes.extend_from_slice(&[0u8; 3]);
    bytes.extend_from_slice(&control.wal_segment_size.to_le_bytes());
    let checksum = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes
}

fn decode_control_file(bytes: &[u8]) -> Result<ControlFile, ControlFileError> {
    if bytes.len() != CONTROL_FILE_ENCODED_LEN {
        return Err(ControlFileError::Corrupt(format!(
            "invalid control file length {}",
            bytes.len()
        )));
    }

    let expected_checksum =
        u32::from_le_bytes(bytes[CONTROL_FILE_ENCODED_LEN - 4..].try_into().unwrap());
    let actual_checksum = crc32c::crc32c(&bytes[..CONTROL_FILE_ENCODED_LEN - 4]);
    if actual_checksum != expected_checksum {
        return Err(ControlFileError::Corrupt(
            "control file checksum mismatch".into(),
        ));
    }

    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != CONTROL_FILE_MAGIC {
        return Err(ControlFileError::Unsupported(format!(
            "unsupported control file magic 0x{magic:08X}"
        )));
    }

    let format_version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if format_version != CONTROL_FILE_FORMAT_VERSION {
        return Err(ControlFileError::Unsupported(format!(
            "unsupported control file format version {format_version}"
        )));
    }

    let wal_segment_size = u32::from_le_bytes(bytes[52..56].try_into().unwrap());
    if wal_segment_size != WAL_SEG_SIZE_BYTES {
        return Err(ControlFileError::Unsupported(format!(
            "unsupported WAL segment size {wal_segment_size}"
        )));
    }

    Ok(ControlFile {
        format_version,
        system_identifier: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        state: ControlFileState::decode(u32::from_le_bytes(bytes[16..20].try_into().unwrap()))?,
        latest_checkpoint_lsn: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
        redo_lsn: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        next_xid: u32::from_le_bytes(bytes[36..40].try_into().unwrap()),
        checkpoint_timestamp_usecs: i64::from_le_bytes(bytes[40..48].try_into().unwrap()),
        full_page_writes: bytes[48] != 0,
        wal_segment_size,
    })
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;

    fn temp_dir(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        env::temp_dir().join(format!("pgrust_control_file_{label}_{nanos}"))
    }

    #[test]
    fn binary_control_file_round_trips() {
        let base = temp_dir("roundtrip");
        let config = CheckpointConfig::default();
        let store = ControlFileStore::bootstrap(&base, 42, &config).unwrap();
        let mut control = store.snapshot();
        control.state = ControlFileState::InProduction;
        control.latest_checkpoint_lsn = 1234;
        control.redo_lsn = 5678;
        persist_control_file(&ControlFileStore::path(&base), &control).unwrap();

        let reopened = ControlFileStore::load(&base).unwrap().snapshot();
        assert_eq!(reopened, control);
    }
}
