//! C: src/bin/pg_combinebackup/load_manifest.c — load each input backup's
//! backup_manifest into memory via the Stage-2 parse_manifest crate.
//!
//! C reads the file in 128kB chunks through the incremental JSON parser
//! purely to bound peak memory; pgrust's parser is whole-buffer, which is
//! behaviorally identical (same accepted inputs, same error identity — see
//! the crate docs of parse_manifest). Parse errors and callback errors are
//! fatal, matching combinebackup's report_manifest_error (pg_log_error +
//! exit 1).

use std::collections::HashMap;
use std::path::Path;

use manifest::PgChecksumType;
use parse_manifest::{json_parse_manifest, JsonManifestParseContext, ManifestWalRange};
use types_core::{TimeLineID, XLogRecPtr};
use types_error::{PgError, PgResult};

use crate::app::flog::{errno_message, log_warning, pg_fatal};

/// C: manifest_file (sans hash-table plumbing).
pub struct ManifestFileEntry {
    /// Kept for C-parity with manifest_file; pg_combinebackup itself only
    /// consumes the checksum fields.
    #[allow(dead_code)]
    pub size: u64,
    pub checksum_type: PgChecksumType,
    pub checksum_payload: Option<Vec<u8>>,
}

/// C: manifest_data.
pub struct ManifestData {
    pub system_identifier: u64,
    pub files: HashMap<Vec<u8>, ManifestFileEntry>,
    pub wal_ranges: Vec<ManifestWalRange>,
}

struct LoadContext {
    data: ManifestData,
}

impl JsonManifestParseContext for LoadContext {
    /// C: combinebackup_version_cb.
    fn version_cb(&mut self, manifest_version: i32) -> PgResult<()> {
        /* Incremental backups supported on manifest version 2 or later */
        if manifest_version == 1 {
            return Err(PgError::error(
                "backup manifest version 1 does not support incremental backup",
            )
            .into());
        }
        Ok(())
    }

    /// C: combinebackup_system_identifier_cb.
    fn system_identifier_cb(&mut self, manifest_system_identifier: u64) -> PgResult<()> {
        /* Validation will be at the later stage */
        self.data.system_identifier = manifest_system_identifier;
        Ok(())
    }

    /// C: combinebackup_per_file_cb.
    fn per_file_cb(
        &mut self,
        pathname: &[u8],
        size: u64,
        checksum_type: PgChecksumType,
        checksum_payload: Option<&[u8]>,
    ) -> PgResult<()> {
        let entry = ManifestFileEntry {
            size,
            checksum_type,
            checksum_payload: checksum_payload.map(<[u8]>::to_vec),
        };
        if self.data.files.insert(pathname.to_vec(), entry).is_some() {
            return Err(PgError::error(format!(
                "duplicate path name in backup manifest: \"{}\"",
                String::from_utf8_lossy(pathname)
            ))
            .into());
        }
        Ok(())
    }

    /// C: combinebackup_per_wal_range_cb.
    fn per_wal_range_cb(
        &mut self,
        tli: TimeLineID,
        start_lsn: XLogRecPtr,
        end_lsn: XLogRecPtr,
    ) -> PgResult<()> {
        self.data.wal_ranges.push(ManifestWalRange { tli, start_lsn, end_lsn });
        Ok(())
    }
}

/// C: load_backup_manifests.
pub fn load_backup_manifests(backup_directories: &[String]) -> Vec<Option<ManifestData>> {
    backup_directories
        .iter()
        .map(|d| load_backup_manifest(Path::new(d)))
        .collect()
}

/// C: load_backup_manifest. Missing file => warning + None; anything else
/// fatal.
pub fn load_backup_manifest(backup_directory: &Path) -> Option<ManifestData> {
    let pathname = backup_directory.join("backup_manifest");

    let buffer = match std::fs::read(&pathname) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            log_warning(&format!("file \"{}\" does not exist", pathname.display()));
            return None;
        }
        Err(e) => pg_fatal!(
            "could not open file \"{}\": {}",
            pathname.display(),
            errno_message(&e)
        ),
    };

    let mut context = LoadContext {
        data: ManifestData {
            system_identifier: 0,
            files: HashMap::new(),
            wal_ranges: Vec::new(),
        },
    };

    let root = mcx::MemoryContext::new("load_backup_manifest");
    let result = json_parse_manifest(root.mcx(), &mut context, &buffer);
    if let Err(e) = result {
        /* C: report_manifest_error — pg_log_error + exit(1). */
        crate::app::flog::log_error(e.message());
        crate::app::flog::exit_program(1);
    }

    Some(context.data)
}
