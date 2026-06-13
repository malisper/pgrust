//! Family `admin` — the SRF / system-administration glue files:
//! `genfile.c` + `hbafuncs.c` + `lockfuncs.c` + `partitionfuncs.c` +
//! `pg_upgrade_support.c`.
//!
//! These are grouped because they share the same shape: set-returning or
//! privileged SQL functions that are thin glue over substrate owned by other
//! units, which they reach through seams in those real owners (genuinely
//! unported owners are seam-and-panic, named below):
//!
//! * genfile.c   — `pg_read_file*` / `pg_stat_file` / `pg_ls_dir*` directory
//!   and file SRFs; seam to backend-storage-file-fd + the data-dir path
//!   helpers.
//! * hbafuncs.c  — `pg_hba_file_rules` / `pg_ident_file_mappings` SRFs over the
//!   parsed HBA/ident token lists; seam to the (unported) hba.c parser owner.
//! * lockfuncs.c — `pg_lock_status` + the `pg_advisory_*` lock SRFs/functions;
//!   seam to backend-storage-lmgr (lock.c / lmgr.c advisory-lock surface).
//! * partitionfuncs.c — `pg_partition_tree` / `_root` / `_ancestors`; seam to
//!   partition descriptor + pg_inherits owners.
//! * pg_upgrade_support.c — the `binary_upgrade_*` setters; seam to the
//!   (genuinely unported) catalog binary-upgrade state owners (relfilenumber,
//!   pg_enum/authid oid pinning, slot/replorigin advance).
//!
//! All allocate result tuplestores / text, so they take `Mcx` and surface
//! `ereport`s as `PgResult`. Independent of the keystone. Only the
//! representative public surface is enumerated; the full SRF matrix is filled
//! in the port phase.

use mcx::Mcx;
use types_datum::Datum;
use types_error::PgResult;

// --- genfile.c ---

/// `pg_read_file_off_len(filename, offset, length)` (and the all/missing
/// variants).
pub fn pg_read_file_off_len<'mcx>(
    _mcx: Mcx<'mcx>,
    _filename: &str,
    _offset: i64,
    _length: i64,
) -> PgResult<Datum> {
    todo!("pg_read_file_off_len")
}

/// `pg_stat_file(filename, missing_ok)`.
pub fn pg_stat_file<'mcx>(
    _mcx: Mcx<'mcx>,
    _filename: &str,
    _missing_ok: bool,
) -> PgResult<Datum> {
    todo!("pg_stat_file")
}

/// `pg_ls_dir(dirname, missing_ok, include_dot_dirs)` — SRF.
pub fn pg_ls_dir<'mcx>(_mcx: Mcx<'mcx>, _dirname: &str) -> PgResult<Datum> {
    todo!("pg_ls_dir")
}

// --- hbafuncs.c ---

/// `pg_hba_file_rules(fcinfo)` — SRF over the parsed pg_hba.conf lines.
pub fn pg_hba_file_rules<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("pg_hba_file_rules")
}

/// `pg_ident_file_mappings(fcinfo)` — SRF over the parsed pg_ident.conf maps.
pub fn pg_ident_file_mappings<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("pg_ident_file_mappings")
}

// --- lockfuncs.c ---

/// `pg_lock_status(fcinfo)` — SRF over the lock manager state.
pub fn pg_lock_status<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum> {
    todo!("pg_lock_status")
}

/// `pg_advisory_lock_int8(key)` (representative of the pg_advisory_* family).
pub fn pg_advisory_lock_int8(_key: i64) -> PgResult<()> {
    todo!("pg_advisory_lock_int8")
}

/// `pg_advisory_unlock_int8(key)`.
pub fn pg_advisory_unlock_int8(_key: i64) -> PgResult<bool> {
    todo!("pg_advisory_unlock_int8")
}

/// `pg_advisory_unlock_all()`.
pub fn pg_advisory_unlock_all() -> PgResult<()> {
    todo!("pg_advisory_unlock_all")
}

// --- partitionfuncs.c ---

/// `pg_partition_tree(rootrelid)` — SRF.
pub fn pg_partition_tree<'mcx>(_mcx: Mcx<'mcx>, _rootrelid: u32) -> PgResult<Datum> {
    todo!("pg_partition_tree")
}

/// `pg_partition_root(relid)`.
pub fn pg_partition_root(_relid: u32) -> PgResult<Datum> {
    todo!("pg_partition_root")
}

/// `pg_partition_ancestors(relid)` — SRF.
pub fn pg_partition_ancestors<'mcx>(_mcx: Mcx<'mcx>, _relid: u32) -> PgResult<Datum> {
    todo!("pg_partition_ancestors")
}

// --- pg_upgrade_support.c ---

/// `binary_upgrade_set_next_heap_relfilenode(relfilenumber)` (representative of
/// the binary_upgrade_* setter family).
pub fn binary_upgrade_set_next_heap_relfilenode(_relfilenumber: u32) -> PgResult<()> {
    todo!("binary_upgrade_set_next_heap_relfilenode")
}

/// `binary_upgrade_logical_slot_has_caught_up(slot_name)`.
pub fn binary_upgrade_logical_slot_has_caught_up(_slot_name: &str) -> PgResult<bool> {
    todo!("binary_upgrade_logical_slot_has_caught_up")
}
