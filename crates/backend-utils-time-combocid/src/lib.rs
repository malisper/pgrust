//! Combo command ID support (`src/backend/utils/time/combocid.c`).
//!
//! `HeapTupleHeaderData` has a single `t_cid` field overlaying cmin and cmax.
//! When the inserting transaction also deletes/updates the tuple, both command
//! ids must survive, so a "combo" command id is stored instead: an index into
//! a backend-private array mapping the combo id back to its real
//! `(cmin, cmax)` pair. A hash table maps `(cmin, cmax)` pairs back to combo
//! ids so identical pairs reuse one id. Combo CIDs matter only to the
//! originating transaction; the structures live in `TopTransactionContext` and
//! are discarded at end of transaction.
//!
//! The C file's module-level statics (`comboCids`, `comboHash`,
//! `usedComboCids`, `sizeComboCids`) become [`ComboCidState`]. Like the C
//! statics — which live in `TopTransactionContext` and are reset every
//! end-of-transaction by `AtEOXact_ComboCid` — this is per-backend state with
//! a transaction lifetime, so it lives in a `thread_local!` cell (one backend
//! = one thread, the faithful model of a C backend static). The dynahash
//! becomes a `HashMap`, the array a `Vec` (whose `len()` is `usedComboCids`).
//!
//! Backing the transaction-lifetime containers with std `Vec`/`HashMap` rather
//! than `PgVec`/`PgHashMap` charged to the `TopTransactionContext` is the same
//! ledgered divergence the transaction owner (xact.c) already takes for
//! `childXids`/savepoint names (see `DESIGN_DEBT.md`): the backend-local cell
//! cannot borrow the context it would allocate in. Every allocating touch is
//! still fallible (`try_reserve`-style), carrying C's OOM `ereport(ERROR)`
//! surface via [`mcx::oom_named`] against the `ComboCidState` name; what is
//! lost is only the context-accounting coupling.
//!
//! The `HeapTupleHeaderGetCmin`/`Cmax`/`AdjustCmax` macros over the file-scope
//! state are exposed both as functions taking an explicit `&ComboCidState`
//! (used by tests and by `AdjustCmax`'s internal `GetCmin`) and as installed
//! seams (`init_seams`) that reach the live `thread_local!` state — the
//! visibility predicates and `heap_delete`/`heap_update` call the latter, with
//! no `ComboCidState` in hand, exactly as in C.

#![allow(non_snake_case)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::oom_named;
use types_core::{CommandId, Size, TransactionId};
use types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use types_tuple::heaptuple::{
    HeapTupleHeaderData, HeapTupleHeaderGetRawCommandId, HeapTupleHeaderGetRawXmin,
    HeapTupleHeaderXminCommitted, HEAP_COMBOCID, HEAP_MOVED, HEAP_XMIN_FROZEN,
};

/// The named C context this backend-local state stands in for, used for the
/// OOM `ereport` message shape (`mcx::oom_named`).
const COMBOCID_CONTEXT_NAME: &str = "TopTransactionContext";

thread_local! {
    /// The backend-private combo-CID state (C's `comboCids`/`comboHash`
    /// file-scope statics). One backend = one thread.
    static STATE: RefCell<ComboCidState> = RefCell::new(ComboCidState::new());
}

/// Run `f` with mutable access to the backend-local combo-CID state.
fn with_state<T>(f: impl FnOnce(&mut ComboCidState) -> T) -> T {
    STATE.with(|cell| f(&mut cell.borrow_mut()))
}

/// Install this crate's seam implementations. The `HeapTupleHeaderGet*`/
/// `AdjustCmax` macros resolve a combo CID against the file-scope state; the
/// installed closures reach the live `thread_local!` [`STATE`] (the C statics),
/// so visibility predicates and `heap_delete`/`heap_update` — which hold no
/// `ComboCidState` — reach the resolved cmin/cmax through them.
pub fn init_seams() {
    backend_utils_time_combocid_seams::heap_tuple_header_get_cmin::set(|tuple| {
        with_state(|s| HeapTupleHeaderGetCmin(s, tuple))
    });
    backend_utils_time_combocid_seams::heap_tuple_header_get_cmax::set(|tuple| {
        with_state(|s| HeapTupleHeaderGetCmax(s, tuple))
    });
    backend_utils_time_combocid_seams::heap_tuple_header_adjust_cmax::set(|tuple, cmax| {
        with_state(|s| HeapTupleHeaderAdjustCmax(s, tuple, cmax))
    });
    backend_utils_time_combocid_seams::at_eoxact_combocid::set(|| with_state(AtEOXact_ComboCid));

    // Parallel-worker transfer of the combo-CID state. The bodies are owned
    // here; the seam decls live in parallel-rt-seams. The DSM byte chunk is
    // self-delimiting — the first machine `int` is the entry count, so the
    // restore side reconstructs the slice length the same way the GUC transfer
    // does (cf. backend-utils-misc-guc restore_guc_state).
    {
        use backend_access_transam_parallel_rt_seams as rt;
        rt::estimate_combocid_state_space::set(|| with_state(|s| EstimateComboCIDStateSpace(s)));
        rt::serialize_combocid_state::set(|len, space| {
            // SAFETY: `space` is the start of a `len`-byte chunk shm_toc_allocate
            // reserved for the combo-CID state (EstimateComboCIDStateSpace sized
            // it); the leader writes the whole chunk here. The audited
            // DSM-pointer primitive (cf. backend-utils-misc-guc).
            let buf = unsafe { core::slice::from_raw_parts_mut(space as *mut u8, len) };
            with_state(|s| SerializeComboCIDState(s, buf))
        });
        rt::restore_combocid_state::set(|space| {
            // The first machine `int` of the stream is the entry count; read it,
            // then form the `SIZEOF_INT + count * SIZEOF_COMBO_CID_KEY_DATA`
            // slice. SAFETY: `space` points at the combo-CID chunk the leader
            // serialized; the embedded count bounds the readable extent.
            let count = unsafe {
                let head = core::slice::from_raw_parts(space as *const u8, SIZEOF_INT);
                i32::from_ne_bytes(head.try_into().expect("4-byte count prefix"))
            };
            let total = SIZEOF_INT + (count.max(0) as usize) * SIZEOF_COMBO_CID_KEY_DATA;
            let buf = unsafe { core::slice::from_raw_parts(space as *const u8, total) };
            with_state(|s| RestoreComboCIDState(s, buf))
        });
    }
}

/// `CCID_HASH_SIZE` — initial size of the hash table.
const CCID_HASH_SIZE: usize = 100;

/// `CCID_ARRAY_SIZE` — initial size of the array.
const CCID_ARRAY_SIZE: usize = 100;

/// `ComboCidKeyData` — the `(cmin, cmax)` pair a combo id stands for.
///
/// The C dynahash keys on this struct's raw bytes (`HASH_BLOBS`, "we assume
/// there is no struct padding"); derived `Hash`/`Eq` over the two fields cover
/// the same value space.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ComboCidKeyData {
    pub cmin: CommandId,
    pub cmax: CommandId,
}

/// The C file's backend-private statics as one owned value (the `thread_local!`
/// [`STATE`] cell holds the live instance; tests construct their own).
///
/// * `combo_cids` — `comboCids`/`usedComboCids`/`sizeComboCids`: combo id
///   (index) -> `(cmin, cmax)`.
/// * `combo_hash` — `comboHash`: `(cmin, cmax)` -> combo id. `None` mirrors
///   C's `comboHash == NULL` "not yet created this transaction" sentinel, so
///   the first `GetComboCommandId` performs the same one-time setup.
pub struct ComboCidState {
    combo_cids: Vec<ComboCidKeyData>,
    combo_hash: Option<HashMap<ComboCidKeyData, CommandId>>,
}

impl ComboCidState {
    /// Fresh, empty state for a transaction. Does not allocate; the C lazy
    /// creation happens on first use.
    pub fn new() -> Self {
        Self {
            combo_cids: Vec::new(),
            combo_hash: None,
        }
    }
}

impl Default for ComboCidState {
    fn default() -> Self {
        Self::new()
    }
}

/*
 * GetCmin and GetCmax assert that they are only called in situations where
 * they make sense, that is, can deliver a useful answer.  If you have
 * reason to examine a tuple's t_cid field from a transaction other than
 * the originating one, use HeapTupleHeaderGetRawCommandId() directly.
 */

/// `FrozenTransactionId` (`access/transam.h:33`).
const FROZEN_TRANSACTION_ID: TransactionId = 2;

/// `HeapTupleHeaderGetXmin(tup)` (`access/htup_details.h:329`):
/// `HeapTupleHeaderXminFrozen(tup) ? FrozenTransactionId : raw xmin`.
#[inline]
fn HeapTupleHeaderGetXmin(tup: &HeapTupleHeaderData<'_>) -> TransactionId {
    if (tup.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN {
        FROZEN_TRANSACTION_ID
    } else {
        HeapTupleHeaderGetRawXmin(tup)
    }
}

/// `HeapTupleHeaderGetCmin(tup)`.
pub fn HeapTupleHeaderGetCmin(state: &ComboCidState, tup: &HeapTupleHeaderData<'_>) -> CommandId {
    let cid = HeapTupleHeaderGetRawCommandId(tup);

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);
    // Assert(TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tup)))
    debug_assert!(
        backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(
            HeapTupleHeaderGetXmin(tup)
        ),
        "HeapTupleHeaderGetCmin: tuple was not inserted by the current transaction"
    );

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmin(state, cid)
    } else {
        cid
    }
}

/// `HeapTupleHeaderGetCmax(tup)`.
///
/// C also asserts `CritSectionCount > 0 || TransactionIdIsCurrentTransactionId
/// (HeapTupleHeaderGetUpdateXid(tup))` (weakened inside critical sections
/// because `GetUpdateXid()` can allocate when xmax is a multixact); that check
/// is waived here because `HeapTupleHeaderGetUpdateXid`'s multixact resolution
/// (`access/heap/heapam.c`'s `HeapTupleGetUpdateXid`) is unported.
pub fn HeapTupleHeaderGetCmax(state: &ComboCidState, tup: &HeapTupleHeaderData<'_>) -> CommandId {
    let cid = HeapTupleHeaderGetRawCommandId(tup);

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmax(state, cid)
    } else {
        cid
    }
}

/// `HeapTupleHeaderAdjustCmax(tup, &cmax, &iscombo)`.
///
/// Given a tuple we are about to delete, determine the correct value to store
/// into its t_cid field: `(cmax, false)` if no combo CID is needed, or
/// `(combo_cid, true)` if the tuple was inserted by (any subtransaction of)
/// our own transaction.
///
/// This is separate from the actual `HeapTupleHeaderSetCmax()` operation
/// because it can fail due to out-of-memory conditions (hence `PgResult`); it
/// must run before entering the critical section that changes the tuple in
/// shared buffers. The C out-parameters become the returned tuple.
pub fn HeapTupleHeaderAdjustCmax(
    state: &mut ComboCidState,
    tup: &HeapTupleHeaderData<'_>,
    cmax: CommandId,
) -> PgResult<(CommandId, bool)> {
    // Test for HeapTupleHeaderXminCommitted() first, because it's cheaper
    // than a TransactionIdIsCurrentTransactionId call.
    if !HeapTupleHeaderXminCommitted(tup)
        && backend_access_transam_xact_seams::transaction_id_is_current_transaction_id::call(
            HeapTupleHeaderGetRawXmin(tup),
        )
    {
        let cmin = HeapTupleHeaderGetCmin(state, tup);
        Ok((GetComboCommandId(state, cmin, cmax)?, true))
    } else {
        Ok((cmax, false))
    }
}

/// `AtEOXact_ComboCid()`.
///
/// Combo command ids are only interesting to the inserting and deleting
/// transaction, so we can forget about them at the end of transaction. C just
/// nulls the pointers and lets the `TopTransactionContext` reset reclaim the
/// memory; dropping the owned containers reclaims it here.
pub fn AtEOXact_ComboCid(state: &mut ComboCidState) {
    state.combo_hash = None;
    state.combo_cids = Vec::new();
}

/// `GetComboCommandId(cmin, cmax)` (static).
///
/// Get a combo command id that maps to `cmin` and `cmax`, reusing an existing
/// one when possible.
fn GetComboCommandId(
    state: &mut ComboCidState,
    cmin: CommandId,
    cmax: CommandId,
) -> PgResult<CommandId> {
    // Create the hash table and array the first time we need to use combo
    // cids in the transaction. Make array first; existence of hash table
    // asserts array exists.
    if state.combo_hash.is_none() {
        state
            .combo_cids
            .try_reserve_exact(CCID_ARRAY_SIZE)
            .map_err(|_| {
                oom_named(
                    COMBOCID_CONTEXT_NAME,
                    CCID_ARRAY_SIZE * core::mem::size_of::<ComboCidKeyData>(),
                )
            })?;

        let mut hash = HashMap::new();
        hash.try_reserve(CCID_HASH_SIZE).map_err(|_| {
            oom_named(
                COMBOCID_CONTEXT_NAME,
                CCID_HASH_SIZE * core::mem::size_of::<(ComboCidKeyData, CommandId)>(),
            )
        })?;
        state.combo_hash = Some(hash);
    }

    // Grow the array if there's not at least one free slot.  We must do this
    // before possibly entering a new hashtable entry, else failure to grow
    // would leave a corrupt hashtable entry behind.
    if state.combo_cids.len() >= state.combo_cids.capacity() {
        let newslots = state.combo_cids.capacity(); // newsize = sizeComboCids * 2
        state.combo_cids.try_reserve_exact(newslots).map_err(|_| {
            oom_named(
                COMBOCID_CONTEXT_NAME,
                newslots * core::mem::size_of::<ComboCidKeyData>(),
            )
        })?;
    }

    let key = ComboCidKeyData { cmin, cmax };

    // hash_search(comboHash, &key, HASH_ENTER, &found)
    let combo_cids = &mut state.combo_cids;
    let combo_hash = state.combo_hash.as_mut().expect("created above");

    if let Some(&combocid) = combo_hash.get(&key) {
        // Reuse an existing combo CID.
        return Ok(combocid);
    }

    // Entering a new hash entry allocates; in C HASH_ENTER ereports on OOM.
    combo_hash.try_reserve(1).map_err(|_| {
        oom_named(
            COMBOCID_CONTEXT_NAME,
            core::mem::size_of::<(ComboCidKeyData, CommandId)>(),
        )
    })?;

    // We have to create a new combo CID; we already made room in the array.
    let combocid = combo_cids.len() as CommandId;
    combo_cids.push(key);
    combo_hash.insert(key, combocid);

    Ok(combocid)
}

/// `GetRealCmin(combocid)` (static). The index expression carries C's
/// `Assert(combocid < usedComboCids)` (out of range panics).
fn GetRealCmin(state: &ComboCidState, combocid: CommandId) -> CommandId {
    state.combo_cids[combocid as usize].cmin
}

/// `GetRealCmax(combocid)` (static). The index expression carries C's
/// `Assert(combocid < usedComboCids)` (out of range panics).
fn GetRealCmax(state: &ComboCidState, combocid: CommandId) -> CommandId {
    state.combo_cids[combocid as usize].cmax
}

/// On-the-wire size of the leading element count (a C `int`).
const SIZEOF_INT: usize = 4;

/// On-the-wire size of `ComboCidKeyData`: two `uint32`s, no padding.
const SIZEOF_COMBO_CID_KEY_DATA: usize = 8;

/// `EstimateComboCIDStateSpace()`.
///
/// Estimate the amount of space required to serialize the current combo CID
/// state. Fallible because C computes it with `add_size`/`mul_size`, which
/// `ereport(ERROR)` on overflow.
pub fn EstimateComboCIDStateSpace(state: &ComboCidState) -> PgResult<Size> {
    // Add space required for saving usedComboCids
    let size: Size = SIZEOF_INT;

    // Add space required for saving ComboCidKeyData
    add_size(
        size,
        mul_size(SIZEOF_COMBO_CID_KEY_DATA, state.combo_cids.len())?,
    )
}

/// `SerializeComboCIDState(maxsize, start_address)`.
///
/// Serialize the combo CID state into `buf` (C's `start_address` pointer plus
/// `maxsize` become one slice; `maxsize == buf.len()`), which should be at
/// least as large as the value returned by [`EstimateComboCIDStateSpace`].
/// Layout: a native-endian C `int` count, then the raw `(cmin, cmax)` pairs.
///
/// C stores the count before performing the size check; with a bounds-checked
/// slice the check runs first, changing nothing on the success path.
pub fn SerializeComboCIDState(state: &ComboCidState, buf: &mut [u8]) -> PgResult<()> {
    let used = state.combo_cids.len();

    // If maxsize is too small, throw an error.
    let needed = SIZEOF_INT + SIZEOF_COMBO_CID_KEY_DATA * used;
    if needed > buf.len() {
        // elog(ERROR, "not enough space to serialize ComboCID state")
        return Err(PgError::error(
            "not enough space to serialize ComboCID state",
        ));
    }

    // First, we store the number of currently-existing combo CIDs.
    buf[0..SIZEOF_INT].copy_from_slice(&(used as i32).to_ne_bytes());

    // Now, copy the actual cmin/cmax pairs.
    let mut off = SIZEOF_INT;
    for entry in &state.combo_cids {
        buf[off..off + 4].copy_from_slice(&entry.cmin.to_ne_bytes());
        buf[off + 4..off + 8].copy_from_slice(&entry.cmax.to_ne_bytes());
        off += SIZEOF_COMBO_CID_KEY_DATA;
    }

    Ok(())
}

/// `RestoreComboCIDState(comboCIDstate)`.
///
/// Read the combo CID state serialized into `buf` and initialize this backend
/// with the same combo CIDs. Only valid in a backend that currently has no
/// combo CIDs (and only makes sense if the transaction state is serialized
/// and restored as well).
///
/// C trusts the producer's pointer arithmetic; the slice reads are
/// bounds-checked, with a short buffer surfacing as the same restore error.
pub fn RestoreComboCIDState(state: &mut ComboCidState, buf: &[u8]) -> PgResult<()> {
    // Assert(!comboCids && !comboHash)
    debug_assert!(state.combo_cids.is_empty() && state.combo_hash.is_none());

    // First, we retrieve the number of combo CIDs that were serialized.
    if buf.len() < SIZEOF_INT {
        return Err(PgError::error(
            "unexpected command ID while restoring combo CIDs",
        ));
    }
    let num_elements = i32::from_ne_bytes(buf[0..SIZEOF_INT].try_into().expect("4 bytes"));

    // keydata = (ComboCidKeyData *) (comboCIDstate + sizeof(int))
    let mut off = SIZEOF_INT;

    // Use GetComboCommandId to restore each combo CID.
    for i in 0..num_elements {
        if off + SIZEOF_COMBO_CID_KEY_DATA > buf.len() {
            return Err(PgError::error(
                "unexpected command ID while restoring combo CIDs",
            ));
        }
        let cmin = CommandId::from_ne_bytes(buf[off..off + 4].try_into().expect("4 bytes"));
        let cmax = CommandId::from_ne_bytes(buf[off + 4..off + 8].try_into().expect("4 bytes"));
        off += SIZEOF_COMBO_CID_KEY_DATA;

        let cid = GetComboCommandId(state, cmin, cmax)?;

        // Verify that we got the expected answer.
        if cid != i as CommandId {
            // elog(ERROR, "unexpected command ID while restoring combo CIDs")
            return Err(PgError::error(
                "unexpected command ID while restoring combo CIDs",
            ));
        }
    }

    Ok(())
}

/// `add_size(s1, s2)` (`storage/shmem.c`): overflow-checked addition raising
/// C's error. Local private mirror of the unported shmem.c helper.
fn add_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_add(s2).ok_or_else(size_overflow)
}

/// `mul_size(s1, s2)` (`storage/shmem.c`): overflow-checked multiplication
/// raising C's error. Local private mirror of the unported shmem.c helper.
fn mul_size(s1: Size, s2: Size) -> PgResult<Size> {
    s1.checked_mul(s2).ok_or_else(size_overflow)
}

fn size_overflow() -> PgError {
    PgError::error("requested shared memory size overflows size_t")
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
}

#[cfg(test)]
mod tests;
