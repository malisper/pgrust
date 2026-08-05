// Port of src/backend/nodes/queryjumblefuncs.c (query fingerprinting).
// The per-node walker (C's generated queryjumblefuncs.funcs.c/.switch.c)
// lives in walker.rs; byte-for-byte jumble parity with C is the contract —
// queryIds must be numerically identical to C's.
#![allow(non_snake_case)]

use core::sync::atomic::{AtomicBool, Ordering};

use mcx::{Mcx, PgVec};
use types_core::ParseLoc;
use types_error::PgResult;
use types_nodes::parsenodes::Query;

mod walker;

pub const JUMBLE_SIZE: usize = 1024;

pub struct LocationLen {
    pub location: i32,
    pub length: i32,
    pub squashed: bool,
    pub extern_param: bool,
}

pub struct JumbleState<'mcx> {
    jumble: [u8; JUMBLE_SIZE],
    jumble_len: usize,
    pub clocations: PgVec<'mcx, LocationLen>,
    pub highest_extern_param_id: i32,
    pub has_squashed_lists: bool,
    pending_nulls: u32,
}

// C `query_id_enabled` (queryjumblefuncs.c): armed by EnableQueryId when a
// module wants ids under compute_query_id=auto/regress. Process-global like
// the guc_tables backing vars (C's is per-backend); the AUTO arm sits on the
// per-parse path, and std thread_local costs ~40 instr/q there (measured).
static QUERY_ID_ENABLED: AtomicBool = AtomicBool::new(false);

fn compute_query_id() -> i32 {
    guc_tables::backing::compute_query_id()
}

pub fn IsQueryIdEnabled() -> bool {
    use guc_tables::consts::{COMPUTE_QUERY_ID_OFF, COMPUTE_QUERY_ID_ON};
    match compute_query_id() {
        COMPUTE_QUERY_ID_OFF => false,
        COMPUTE_QUERY_ID_ON => true,
        _ => QUERY_ID_ENABLED.load(Ordering::Relaxed),
    }
}

pub fn EnableQueryId() {
    if compute_query_id() != guc_tables::consts::COMPUTE_QUERY_ID_OFF {
        QUERY_ID_ENABLED.store(true, Ordering::Relaxed);
    }
}

fn scanner_isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\r' | b'\x0B' | b'\x0C')
}

pub fn CleanQuerytext<'a>(query: &'a str, location: &mut ParseLoc, len: &mut ParseLoc) -> &'a str {
    let bytes = query.as_bytes();
    let mut query_location = *location;
    let mut start;
    let mut query_len;

    if query_location >= 0 {
        debug_assert!(query_location as usize <= bytes.len());
        start = query_location as usize;
        query_len = *len;
        if query_len <= 0 {
            query_len = (bytes.len() - start) as i32;
        } else {
            debug_assert!(start + query_len as usize <= bytes.len());
        }
    } else {
        query_location = 0;
        start = 0;
        query_len = bytes.len() as i32;
    }

    while query_len > 0 && scanner_isspace(bytes[start]) {
        start += 1;
        query_location += 1;
        query_len -= 1;
    }
    while query_len > 0 && scanner_isspace(bytes[start + query_len as usize - 1]) {
        query_len -= 1;
    }

    *location = query_location;
    *len = query_len;
    &query[start..start + query_len as usize]
}

impl<'mcx> JumbleState<'mcx> {
    fn new(mcx: Mcx<'mcx>) -> PgResult<Self> {
        let mut clocations = PgVec::new_in(mcx);
        clocations.try_reserve(32).map_err(|_| mcx.oom(32))?;
        Ok(JumbleState {
            jumble: [0; JUMBLE_SIZE],
            jumble_len: 0,
            clocations,
            highest_extern_param_id: 0,
            has_squashed_lists: false,
            pending_nulls: 0,
        })
    }

    #[inline(always)]
    fn append_internal(&mut self, mut item: &[u8]) {
        debug_assert!(!item.is_empty());
        if item.len() <= JUMBLE_SIZE - self.jumble_len {
            self.jumble[self.jumble_len..self.jumble_len + item.len()].copy_from_slice(item);
            self.jumble_len += item.len();
            return;
        }
        // Buffer full: fold the current contents into a hash and continue
        // from that summary (C AppendJumbleInternal's slow loop).
        loop {
            if self.jumble_len >= JUMBLE_SIZE {
                let start_hash = hashfn::hash_bytes_extended(&self.jumble[..JUMBLE_SIZE], 0);
                self.jumble[..8].copy_from_slice(&start_hash.to_ne_bytes());
                self.jumble_len = 8;
            }
            let part = item.len().min(JUMBLE_SIZE - self.jumble_len);
            self.jumble[self.jumble_len..self.jumble_len + part].copy_from_slice(&item[..part]);
            self.jumble_len += part;
            item = &item[part..];
            if item.is_empty() {
                break;
            }
        }
    }

    #[inline]
    fn append(&mut self, item: &[u8]) {
        if self.pending_nulls > 0 {
            self.flush_pending_nulls();
        }
        self.append_internal(item);
    }

    #[inline]
    fn append_null(&mut self) {
        self.pending_nulls += 1;
    }

    #[inline]
    fn flush_pending_nulls(&mut self) {
        debug_assert!(self.pending_nulls > 0);
        let n = self.pending_nulls;
        self.append_internal(&n.to_ne_bytes());
        self.pending_nulls = 0;
    }

    fn record_const_location(
        &mut self,
        extern_param: bool,
        location: i32,
        len: i32,
    ) -> PgResult<()> {
        if location >= 0 {
            if self.clocations.len() == self.clocations.capacity() {
                let want = self.clocations.capacity();
                let oom = self.clocations.allocator().oom(want);
                self.clocations.try_reserve(want).map_err(|_| oom)?;
            }
            debug_assert!(len >= -1);
            self.clocations.push(LocationLen {
                location,
                length: len,
                squashed: len > -1,
                extern_param,
            });
        }
        Ok(())
    }
}

fn DoJumble<'mcx>(jstate: &mut JumbleState<'mcx>, query: &Query<'mcx>) -> PgResult<i64> {
    walker::jumble_query_struct(jstate, query)?;
    if jstate.pending_nulls > 0 {
        jstate.flush_pending_nulls();
    }
    if jstate.has_squashed_lists {
        jstate.highest_extern_param_id = 0;
    }
    Ok(hashfn::hash_bytes_extended(&jstate.jumble[..jstate.jumble_len], 0) as i64)
}

pub fn JumbleQuery<'mcx>(mcx: Mcx<'mcx>, query: &mut Query<'mcx>) -> PgResult<JumbleState<'mcx>> {
    debug_assert!(IsQueryIdEnabled());
    let mut jstate = JumbleState::new(mcx)?;
    query.queryId = DoJumble(&mut jstate, query)?;
    if query.queryId == 0 {
        query.queryId = if query.utilityStmt.is_some() { 2 } else { 1 };
    }
    Ok(jstate)
}

// Per-parse call sites discard the JumbleState (no post_parse_analyze_hook
// surface); returning it by value (>1KB) inflates every caller's frame, which
// showed up as +42 instr/q on select1 even with jumbling disabled. This cold
// entry keeps the state on its own frame.
#[cold]
#[inline(never)]
pub fn JumbleQueryDiscard<'mcx>(mcx: Mcx<'mcx>, query: &mut Query<'mcx>) -> PgResult<()> {
    JumbleQuery(mcx, query).map(|_| ())
}

#[cfg(test)]
mod tests;
