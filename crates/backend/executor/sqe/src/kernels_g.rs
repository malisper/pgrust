//! Extraction from the harness kernels_g.rs (port-study/port-map.md
//! §3.13): ColState — one column's per-thread decode state.

use crate::bank::Bank;
use crate::scan::{CurCache, Scratch};

/// One column's per-thread decode state (u64 datum lanes).
pub struct ColState {
    s: Scratch,
    c: CurCache,
}

impl ColState {
    pub fn new(attno: u32) -> ColState {
        ColState {
            s: Scratch::new(),
            c: CurCache::new(attno),
        }
    }
    /// Depot-riding twin of `new` (the scratch-init discipline): the
    /// decode arena comes reset from the calling thread's depot.
    pub fn fetch(attno: u32) -> ColState {
        ColState {
            s: crate::scan::scratch_fetch(),
            c: CurCache::new(attno),
        }
    }
    /// Park the arena back on this thread's depot (cursor drops — never
    /// parked).
    pub fn park(self) {
        crate::scan::scratch_park(self.s);
    }
    #[inline]
    pub fn dec<'a>(&'a mut self, bank: &Bank, pi: usize, g: u32, rows: usize) -> &'a [u64] {
        self.s.decode_full(self.c.get(bank, pi), g, rows)
    }
    #[inline]
    pub fn dec16<'a>(&'a mut self, bank: &Bank, pi: usize, g: u32, rows: usize) -> &'a [i16] {
        self.s.decode_i16(self.c.get(bank, pi), g, rows)
    }
}
