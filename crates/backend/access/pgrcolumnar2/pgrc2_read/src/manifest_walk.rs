//! Effective-manifest resolution (spec §13): read `CURRENT` (the O(1)
//! candidate hint), then walk `prev_gen` past non-committed generations —
//! the clog fence (#254). Effectiveness is a commit fact, so the clog
//! consult arrives through the [`CommitCheck`] seam (the server-side
//! transaction layer implements it at M3-G/H; tests model publishers
//! directly).
//!
//! Crash-consistency consequences of the §13.3 publish ordering, encoded
//! here as behavior:
//!
//! - **Absent `CURRENT` ⇒ empty table** (`Ok(None)`): the commit record is
//!   written only AFTER `CURRENT` publishes and the directory fsyncs, so no
//!   committed publish can exist without it. No directory scanning fallback
//!   exists — a scan could only find generations that are structurally
//!   invisible.
//! - **A corrupt `CURRENT` or a missing/corrupt chained manifest is a typed
//!   refusal**, never silent emptiness: COMMITTED generations were fsynced
//!   before any commit that references them, so on a RECOVERED directory
//!   damage is real corruption. Un-recovered crash residue can legally show
//!   a dangling `CURRENT` (a pre-commit dirent-loss window can persist the
//!   `CURRENT` rename while dropping the `manifest-<g>` link); the #462
//!   repoint law makes `recover_and_clean` — which runs before readers —
//!   repair exactly that, so the refusal here stays a corruption tripwire.
//!   Scan fallback is a writer-side affordance only (spec §13.3): a reader
//!   cannot distinguish missing uncommitted residue from a missing
//!   committed manifest, so softening this walk would swallow corruption.

use pgrc2_format::dirlayout::{manifest_file_name, CURRENT_FILE_NAME};
use pgrc2_format::manifest::{CommitPointer, Manifest};
use pgrc2_format::wire::crc32c;
use pgrc2_format::FormatError;

use crate::io::TableDirIo;
use crate::{ReadError, ReadResult};

/// The clog seam: is the publishing FullTransactionId committed?
/// (Epoch-qualified 64-bit — xid recycling cannot alias it, spec §13.1.)
pub trait CommitCheck {
    fn committed(&self, fxid: u64) -> bool;
}

/// Every publisher committed — tools/tests over known-good directories.
pub struct AllCommitted;

impl CommitCheck for AllCommitted {
    fn committed(&self, _fxid: u64) -> bool {
        true
    }
}

/// Validation facts for the table the directory must belong to.
#[derive(Debug, Clone, Copy, Default)]
pub struct TableExpect {
    pub relfilenumber: Option<u64>,
    pub spc_db: Option<(u32, u32)>,
    pub schema_fingerprint: Option<u64>,
}

/// The resolved effective generation.
#[derive(Debug, Clone)]
pub struct EffectiveManifest {
    pub manifest: Manifest,
    /// Generations walked past (non-committed publishers) before this one —
    /// 0 when `CURRENT`'s candidate was already effective.
    pub walked_past: u32,
}

/// Resolve the effective manifest generation for a table directory.
/// `Ok(None)` = no committed publish (empty table).
pub fn resolve_effective(
    dir: &dyn TableDirIo,
    check: &dyn CommitCheck,
    expect: &TableExpect,
) -> ReadResult<Option<EffectiveManifest>> {
    let Some(cur_bytes) = dir.read_file(CURRENT_FILE_NAME)? else {
        return Ok(None);
    };
    let ptr = CommitPointer::decode(&cur_bytes)?;
    let mut gen = ptr.gen;
    let mut walked_past = 0u32;
    loop {
        let bytes = dir
            .read_file(&manifest_file_name(gen))?
            .ok_or(ReadError::ManifestMissing { gen })?;
        if bytes.len() < 4 {
            return Err(ReadError::Format(FormatError::Truncated { at: "Manifest" }));
        }
        // The commit pointer pins the candidate's length and trailing CRC
        // (spec §13.2) — check them for the candidate generation only.
        if gen == ptr.gen {
            if bytes.len() as u64 != ptr.manifest_len {
                return Err(ReadError::Format(FormatError::Corrupt {
                    at: "CommitPointer manifest_len",
                }));
            }
            let trailing = crc32c(&bytes[..bytes.len() - 4]);
            if trailing != ptr.manifest_crc {
                return Err(ReadError::Format(FormatError::CrcMismatch {
                    at: "CommitPointer manifest_crc",
                }));
            }
        }
        let m = Manifest::decode(&bytes)?;
        if m.header.gen != gen {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "manifest gen vs file name",
            }));
        }
        validate_table_facts(&m, expect)?;
        if check.committed(m.header.publisher_fxid) {
            return Ok(Some(EffectiveManifest {
                manifest: m,
                walked_past,
            }));
        }
        if m.header.prev_gen == 0 {
            // Every generation back to the first is uncommitted: the table
            // has no effective content.
            return Ok(None);
        }
        gen = m.header.prev_gen;
        walked_past += 1;
    }
}

fn validate_table_facts(m: &Manifest, expect: &TableExpect) -> ReadResult<()> {
    if let Some(want) = expect.relfilenumber {
        if m.header.relfilenumber != want {
            return Err(ReadError::OpenMismatch {
                field: "manifest relfilenumber",
            });
        }
    }
    if let Some((spc, db)) = expect.spc_db {
        if m.header.spc != spc || m.header.db != db {
            return Err(ReadError::OpenMismatch {
                field: "manifest spc/db",
            });
        }
    }
    if let Some(want) = expect.schema_fingerprint {
        if m.header.schema_fingerprint != want {
            return Err(ReadError::OpenMismatch {
                field: "manifest schema_fingerprint",
            });
        }
    }
    Ok(())
}
