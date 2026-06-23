//! `storage/aio/aio_target.c` — IO target registry + per-target dispatch.
//!
//! The only registered target is `PGAIO_TID_SMGR` (`aio_smgr_target_info`,
//! smgr.c). Its `name` is the static string `"smgr"`; its `describe_identity`
//! and `reopen` vtable entries bottom out in the unported smgr AIO layer and are
//! seamed (`pgaio_io_reopen`). The invalid target's name is `"invalid"`.

extern crate alloc;

use alloc::string::String;

use ::types_error::PgResult;

use crate::aio::ioh;
use crate::{PgAioHandleState, PGAIO_TID_INVALID, PGAIO_TID_SMGR};

/// `static const PgAioTargetInfo *pgaio_target_info[]` — the target name (the
/// only field resolvable without the smgr owner).
fn target_name(targetid: u8) -> &'static str {
    match targetid {
        PGAIO_TID_INVALID => "invalid",
        PGAIO_TID_SMGR => "smgr",
        other => panic!("pgaio_target_info: out-of-range target id {other}"),
    }
}

/// `bool pgaio_io_has_target(PgAioHandle *ioh)` (aio_target.c).
pub fn pgaio_io_has_target(ioh_index: usize) -> bool {
    ioh(ioh_index).data().target != PGAIO_TID_INVALID
}

/// `const char *pgaio_io_get_target_name(PgAioHandle *ioh)` (aio_target.c).
pub fn pgaio_io_get_target_name(ioh_index: usize) -> &'static str {
    // explicitly allow INVALID here, used by debug messages
    target_name(ioh(ioh_index).data().target)
}

/// `void pgaio_io_set_target(PgAioHandle *ioh, PgAioTargetID targetid)`
/// (aio_target.c).
pub fn pgaio_io_set_target(ioh_index: usize, targetid: u8) {
    let h = ioh(ioh_index);
    debug_assert!(h.state() == PgAioHandleState::HandedOut);
    let mut d = h.data();
    debug_assert!(d.target == PGAIO_TID_INVALID);
    d.target = targetid;
}

/// `PgAioTargetData *pgaio_io_get_target_data(PgAioHandle *ioh)` (aio_target.c).
///
/// C returns `&ioh->target_data` (a mutable pointer); the target data is filled
/// by the target-assigning code through the per-handle data lock, so this exposes
/// a copy for the read/introspection consumers.
pub fn pgaio_io_get_target_data(ioh_index: usize) -> crate::PgAioTargetData {
    ioh(ioh_index).data().target_data
}

/// `char *pgaio_io_get_target_description(PgAioHandle *ioh)` (aio_target.c).
///
/// Returns a stringified description of the IO's target, localized and allocated
/// in the current memory context. The smgr target's `describe_identity`
/// (`aio_smgr_describe_identity`) bottoms out in the unported smgr AIO layer and
/// is seamed (`pgaio_io_describe_identity`).
pub fn pgaio_io_get_target_description(ioh_index: usize) -> PgResult<String> {
    let target = ioh(ioh_index).data().target;
    // disallow INVALID, there wouldn't be a description
    debug_assert!(target > PGAIO_TID_INVALID);
    completion_seams::pgaio_io_describe_identity::call(ioh_index as u32)
}

/// `bool pgaio_io_can_reopen(PgAioHandle *ioh)` (aio_target.c). The smgr target
/// always provides a `reopen` vtable entry.
pub fn pgaio_io_can_reopen(ioh_index: usize) -> bool {
    let target = ioh(ioh_index).data().target;
    debug_assert!(target > PGAIO_TID_INVALID);
    // pgaio_target_info[target]->reopen != NULL — smgr provides one.
    target == PGAIO_TID_SMGR
}

/// `void pgaio_io_reopen(PgAioHandle *ioh)` (aio_target.c) — reopen the target's
/// fd in an IO worker. Bottoms out in the unported smgr AIO layer.
pub fn pgaio_io_reopen(ioh_index: usize) -> PgResult<()> {
    let target = ioh(ioh_index).data().target;
    debug_assert!(target > PGAIO_TID_INVALID);
    completion_seams::pgaio_io_reopen::call(ioh_index as u32)
}
