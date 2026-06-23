//! Error-context phase save/restore (`vacuumlazy.c`).
//!
//! Functions in this module:
//!   * [`vacuum_error_callback`] (vacuumlazy.c:3758) — produce the `errcontext`
//!     message string for the current phase/block/offset.
//!   * [`update_vacuum_error_info`] (vacuumlazy.c:3822).
//!   * [`restore_vacuum_error_info`] (vacuumlazy.c:3841).
//!
//! In the owned model the `errcontext` callback is reduced to a pure function
//! that computes the message from the current [`LVRelState`] reporting fields;
//! the runtime's error-context stack frame (pushed/popped via the seam) invokes
//! the equivalent message construction. The phase/blk/offnum bookkeeping the C
//! threads is identical.

use ::types_core::{BlockNumber, OffsetNumber};

use crate::consts::{InvalidBlockNumber, InvalidOffsetNumber, MaxOffsetNumber};
use crate::core::{LVRelState, LVSavedErrInfo, VacErrPhase};

/// `BlockNumberIsValid(blockNumber)` (storage/block.h:71).
#[inline]
fn block_number_is_valid(block_number: BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}

/// `OffsetNumberIsValid(offsetNumber)` (storage/off.h:39).
#[inline]
fn offset_number_is_valid(offset_number: OffsetNumber) -> bool {
    offset_number != InvalidOffsetNumber && offset_number <= MaxOffsetNumber
}

/// `vacuum_error_callback()` (vacuumlazy.c:3758) — compute the `errcontext`
/// message for the given vacuum reporting state, or `None` for the cases the C
/// function does nothing for (an invalid block in heap/truncate phases, or the
/// `VACUUM_ERRCB_PHASE_UNKNOWN` arm).
pub fn vacuum_error_callback<'mcx>(errinfo: &LVRelState<'mcx>) -> Option<String> {
    let indname = errinfo.indname.as_deref().unwrap_or("");
    match errinfo.phase {
        VacErrPhase::ScanHeap => {
            if block_number_is_valid(errinfo.blkno) {
                if offset_number_is_valid(errinfo.offnum) {
                    Some(format!(
                        "while scanning block {} offset {} of relation \"{}.{}\"",
                        errinfo.blkno, errinfo.offnum, errinfo.relnamespace, errinfo.relname
                    ))
                } else {
                    Some(format!(
                        "while scanning block {} of relation \"{}.{}\"",
                        errinfo.blkno, errinfo.relnamespace, errinfo.relname
                    ))
                }
            } else {
                Some(format!(
                    "while scanning relation \"{}.{}\"",
                    errinfo.relnamespace, errinfo.relname
                ))
            }
        }
        VacErrPhase::VacuumHeap => {
            if block_number_is_valid(errinfo.blkno) {
                if offset_number_is_valid(errinfo.offnum) {
                    Some(format!(
                        "while vacuuming block {} offset {} of relation \"{}.{}\"",
                        errinfo.blkno, errinfo.offnum, errinfo.relnamespace, errinfo.relname
                    ))
                } else {
                    Some(format!(
                        "while vacuuming block {} of relation \"{}.{}\"",
                        errinfo.blkno, errinfo.relnamespace, errinfo.relname
                    ))
                }
            } else {
                Some(format!(
                    "while vacuuming relation \"{}.{}\"",
                    errinfo.relnamespace, errinfo.relname
                ))
            }
        }
        VacErrPhase::VacuumIndex => Some(format!(
            "while vacuuming index \"{}\" of relation \"{}.{}\"",
            indname, errinfo.relnamespace, errinfo.relname
        )),
        VacErrPhase::IndexCleanup => Some(format!(
            "while cleaning up index \"{}\" of relation \"{}.{}\"",
            indname, errinfo.relnamespace, errinfo.relname
        )),
        VacErrPhase::Truncate => {
            if block_number_is_valid(errinfo.blkno) {
                Some(format!(
                    "while truncating relation \"{}.{}\" to {} blocks",
                    errinfo.relnamespace, errinfo.relname, errinfo.blkno
                ))
            } else {
                None
            }
        }
        VacErrPhase::Unknown => None,
    }
}

/// `update_vacuum_error_info()` (vacuumlazy.c:3822) — update the information
/// required for the vacuum error callback, optionally saving the current
/// information into `saved_vacrel` for later restoration.
pub fn update_vacuum_error_info<'mcx>(
    vacrel: &mut LVRelState<'mcx>,
    saved_vacrel: Option<&mut LVSavedErrInfo>,
    phase: VacErrPhase,
    blkno: BlockNumber,
    offnum: OffsetNumber,
) {
    if let Some(saved) = saved_vacrel {
        saved.offnum = vacrel.offnum;
        saved.blkno = vacrel.blkno;
        saved.phase = vacrel.phase;
    }

    vacrel.blkno = blkno;
    vacrel.offnum = offnum;
    vacrel.phase = phase;
}

/// `restore_vacuum_error_info()` (vacuumlazy.c:3841) — restore the vacuum
/// information saved by a prior [`update_vacuum_error_info`].
pub fn restore_vacuum_error_info<'mcx>(vacrel: &mut LVRelState<'mcx>, saved_vacrel: &LVSavedErrInfo) {
    vacrel.blkno = saved_vacrel.blkno;
    vacrel.offnum = saved_vacrel.offnum;
    vacrel.phase = saved_vacrel.phase;
}
