//! `JumbleState` working state + the `AppendJumble`/`RecordConstLocation`/
//! `FlushPendingNulls` primitives (the hand-written driver half of
//! `queryjumblefuncs.c`).

use alloc::vec::Vec;

/// `JUMBLE_SIZE` (queryjumblefuncs.c:48) — the query serialization buffer size.
pub const JUMBLE_SIZE: usize = 1024;

/// `LocationLen` (queryjumble.h) — tracks the location/length of a constant
/// during normalization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocationLen {
    /// start offset in query text
    pub location: i32,
    /// length in bytes, or -1 to ignore
    pub length: i32,
    /// Does this location represent a squashed list?
    pub squashed: bool,
    /// Is this location a PARAM_EXTERN parameter?
    pub extern_param: bool,
}

/// `JumbleState` (queryjumble.h) — working state for computing a query jumble.
/// The `jumble` byte buffer + `clocations` are owned `Vec`s rather than
/// `palloc`'d arrays; `jumble_len`/`clocations_count` are the live `Vec`
/// lengths.
pub struct JumbleState {
    /// Jumble of current query tree (`jumble` + `jumble_len`; the `Vec`'s length
    /// is `jumble_len`, capped at [`JUMBLE_SIZE`]).
    pub jumble: Vec<u8>,
    /// Array of locations of constants that should be removed.
    pub clocations: Vec<LocationLen>,
    /// ID of the highest PARAM_EXTERN parameter we've seen.
    pub highest_extern_param_id: i32,
    /// Whether squashable lists are present.
    pub has_squashed_lists: bool,
    /// Count of NULL nodes seen since last appending a value.
    pub pending_nulls: u32,
    /// The total number of bytes added to the jumble buffer (assertion aid).
    #[cfg(debug_assertions)]
    pub total_jumble_len: usize,
}

impl JumbleState {
    /// `InitJumble` (queryjumblefuncs.c:175-196).
    pub fn new() -> Self {
        JumbleState {
            jumble: Vec::with_capacity(JUMBLE_SIZE),
            clocations: Vec::with_capacity(32),
            highest_extern_param_id: 0,
            has_squashed_lists: false,
            pending_nulls: 0,
            #[cfg(debug_assertions)]
            total_jumble_len: 0,
        }
    }

    /// `AppendJumbleInternal` (queryjumblefuncs.c:206-273). Append `item`; when
    /// the buffer fills, hash its contents and reset the buffer to just that
    /// 8-byte hash, then continue — exactly as C does.
    #[inline]
    fn append_jumble_internal(&mut self, item: &[u8]) {
        debug_assert!(!item.is_empty(), "AppendJumbleInternal: size must be > 0");

        let jumble_len = self.jumble.len();

        // Fast path: enough space left in the buffer.
        if item.len() <= JUMBLE_SIZE - jumble_len {
            self.jumble.extend_from_slice(item);
            #[cfg(debug_assertions)]
            {
                self.total_jumble_len += item.len();
            }
            return;
        }

        // Slow path: the buffer is (or will become) full. Hash the current
        // JUMBLE_SIZE bytes, collapse the buffer to that 8-byte hash, then keep
        // appending the remaining `item` bytes.
        let mut item = item;
        let mut jumble_len = self.jumble.len();
        loop {
            if jumble_len >= JUMBLE_SIZE {
                let start_hash =
                    common_hashfn::hash_bytes_extended(&self.jumble[..JUMBLE_SIZE], 0) as i64;
                self.jumble.clear();
                self.jumble.extend_from_slice(&start_hash.to_ne_bytes());
                jumble_len = self.jumble.len(); // == 8
            }
            let part_size = core::cmp::min(item.len(), JUMBLE_SIZE - jumble_len);
            self.jumble.extend_from_slice(&item[..part_size]);
            jumble_len += part_size;
            item = &item[part_size..];
            #[cfg(debug_assertions)]
            {
                self.total_jumble_len += part_size;
            }
            if item.is_empty() {
                break;
            }
        }
    }

    /// `AppendJumble` (queryjumblefuncs.c:280-287). Flush any pending NULLs,
    /// then append the given bytes.
    #[inline]
    pub fn append_jumble(&mut self, value: &[u8]) {
        if self.pending_nulls > 0 {
            self.flush_pending_nulls();
        }
        self.append_jumble_internal(value);
    }

    /// `AppendJumbleNull` (queryjumblefuncs.c:293-296). Record one pending NULL.
    #[inline]
    pub fn append_jumble_null(&mut self) {
        self.pending_nulls += 1;
    }

    /// `FlushPendingNulls` (queryjumblefuncs.c:357-365). Incorporate the
    /// `pending_nulls` count into the jumble buffer, then reset it.
    #[inline]
    pub fn flush_pending_nulls(&mut self) {
        debug_assert!(self.pending_nulls > 0, "FlushPendingNulls: no pending NULL");
        let bytes = self.pending_nulls.to_ne_bytes();
        self.append_jumble_internal(&bytes);
        self.pending_nulls = 0;
    }

    /// `RecordConstLocation` (queryjumblefuncs.c:377-403). Record the location
    /// of a constant; `len == -1` marks a single constant, a positive `len` a
    /// squashable list.
    pub fn record_const_location(&mut self, extern_param: bool, location: i32, len: i32) {
        // -1 indicates unknown or undefined location.
        if location >= 0 {
            self.clocations.push(LocationLen {
                location,
                length: len,
                squashed: len > -1,
                extern_param,
            });
        }
    }
}

impl Default for JumbleState {
    fn default() -> Self {
        Self::new()
    }
}
