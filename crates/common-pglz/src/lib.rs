//! Port of PostgreSQL's builtin LZ compressor (`src/common/pg_lzcompress.c`).
//!
//! The compression and decompression byte streams are bit-for-bit identical to
//! PostgreSQL, so a buffer produced here decompresses under PostgreSQL and vice
//! versa.
//!
//! # Memory model
//!
//! The C code compresses directly into a caller-provided `dest` buffer sized
//! `PGLZ_MAX_OUTPUT(slen)`, using statically allocated scratch work arrays
//! (`hist_start` / `hist_entries`) that it re-initializes every call. This port
//! mirrors that: the transient history hash table is a per-call [`History`]
//! whose two arrays are charged to the [`Mcx`] passed in (returning their
//! charge when dropped), and the compressed output is a [`PgVec`] charged to
//! that same context — the analog of the caller's `dest`. Allocating functions
//! take an [`Mcx`] and return [`PgResult`], so allocation failure surfaces as
//! the context's OOM error rather than aborting.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![no_std]

extern crate alloc;
#[cfg(test)]
extern crate std;

use core::cmp::min;
use mcx::{vec_with_capacity_in, Mcx, PgVec};
use types_error::PgResult;

/// 16-bit signed integer, matching PostgreSQL's `int16`.
pub type int16 = i16;
/// 32-bit signed integer, matching PostgreSQL's `int32`.
pub type int32 = i32;

/// Size of the ring buffer of history entries (`PGLZ_HISTORY_SIZE`).
pub const PGLZ_HISTORY_SIZE: usize = 4096;
/// Maximum number of history hash lists (`PGLZ_MAX_HISTORY_LISTS`); power of 2.
pub const PGLZ_MAX_HISTORY_LISTS: usize = 8192;
/// Longest match the format can express (`PGLZ_MAX_MATCH`).
pub const PGLZ_MAX_MATCH: usize = 273;
/// Sentinel "no entry" index into the history ring.
pub const INVALID_ENTRY: usize = 0;

/// `PGLZ_MAX_OUTPUT` — buffer size required by [`pglz_compress`].
///
/// Allows 4 bytes of overrun before compression failure is detected.
pub const fn PGLZ_MAX_OUTPUT(dlen: usize) -> usize {
    dlen + 4
}

/// Tunables that control the compression algorithm, mirroring PostgreSQL's
/// `PGLZ_Strategy`.
///
/// - `min_input_size` / `max_input_size`: input-size window in which
///   compression is attempted at all.
/// - `min_comp_rate`: minimum required compression rate, 0-99%. Regardless of
///   this, the output must be strictly smaller than the input.
/// - `first_success_by`: abandon compression if nothing compressible is found
///   within the first this-many output bytes.
/// - `match_size_good`: initial GOOD match length when walking history.
/// - `match_size_drop`: percentage by which `match_size_good` is lowered after
///   each history check (0 = no change until end, 100 = only the latest entry).
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct PGLZ_Strategy {
    pub min_input_size: int32,
    pub max_input_size: int32,
    pub min_comp_rate: int32,
    pub first_success_by: int32,
    pub match_size_good: int32,
    pub match_size_drop: int32,
}

impl PGLZ_Strategy {
    pub const fn new(
        min_input_size: int32,
        max_input_size: int32,
        min_comp_rate: int32,
        first_success_by: int32,
        match_size_good: int32,
        match_size_drop: int32,
    ) -> Self {
        Self {
            min_input_size,
            max_input_size,
            min_comp_rate,
            first_success_by,
            match_size_good,
            match_size_drop,
        }
    }
}

/// Recommended default strategy for TOAST (`PGLZ_strategy_default`).
pub const PGLZ_STRATEGY_DEFAULT: PGLZ_Strategy =
    PGLZ_Strategy::new(32, int32::MAX, 25, 1024, 128, 10);
/// Try to compress inputs of any length (`PGLZ_strategy_always`).
pub const PGLZ_STRATEGY_ALWAYS: PGLZ_Strategy =
    PGLZ_Strategy::new(0, int32::MAX, 0, int32::MAX, 128, 6);

pub fn PGLZ_strategy_default() -> &'static PGLZ_Strategy {
    &PGLZ_STRATEGY_DEFAULT
}

pub fn PGLZ_strategy_always() -> &'static PGLZ_Strategy {
    &PGLZ_STRATEGY_ALWAYS
}

/// Failure modes of compression and decompression.
///
/// In C these are all signaled by a `-1` return; we distinguish the reasons.
/// `OutOfMemory` is the only path that ereports in C (it doesn't — C uses
/// caller/static buffers; here allocation can fail and surfaces as a real
/// [`PgError`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PglzError {
    /// The strategy rejected the input (size window or disabled).
    InvalidStrategy,
    /// Input did not compress well enough to be worth storing.
    CompressionFailed,
    /// The compressed stream is malformed.
    CorruptInput,
    /// The input length does not fit in the `int32` PostgreSQL uses.
    SizeOverflow,
}

#[derive(Clone, Copy, Debug, Default)]
struct HistEntry {
    next: usize,
    prev: usize,
    hindex: usize,
    pos: usize,
}

/// The compressor's scratch history hash table.
///
/// `start` and `entries` are the transient working arrays (PostgreSQL's static
/// `hist_start` / `hist_entries`). Their allocations are charged to the [`Mcx`]
/// passed to [`History::new`]; the charge is returned when this struct drops.
struct History<'mcx> {
    start: PgVec<'mcx, int16>,
    entries: PgVec<'mcx, HistEntry>,
    next: usize,
    recycle: bool,
    mask: usize,
}

impl<'mcx> History<'mcx> {
    /// Allocate the history hash table for the given (power-of-two) hash size.
    fn new(mcx: Mcx<'mcx>, hash_size: usize) -> PgResult<Self> {
        let mut start: PgVec<int16> = vec_with_capacity_in(mcx, hash_size)?;
        start.resize(hash_size, 0);

        let mut entries: PgVec<HistEntry> = vec_with_capacity_in(mcx, PGLZ_HISTORY_SIZE + 1)?;
        entries.resize(PGLZ_HISTORY_SIZE + 1, HistEntry::default());

        Ok(Self {
            start,
            entries,
            next: 1,
            recycle: false,
            mask: hash_size - 1,
        })
    }

    /// `pglz_hist_add`: enter the current input position into the hash table.
    fn add(&mut self, input: &[u8], pos: usize) {
        let hindex = hist_idx(input, pos, self.mask);
        let entry_index = self.next;

        // If we are about to reuse an entry that is still in some list, unlink
        // it from there first.
        if self.recycle {
            let old = self.entries[entry_index];
            if old.prev == INVALID_ENTRY {
                self.start[old.hindex] = old.next as int16;
            } else {
                self.entries[old.prev].next = old.next;
            }
            if old.next != INVALID_ENTRY {
                self.entries[old.next].prev = old.prev;
            }
        }

        // C reads `*__myhsp` (the current list head for `hindex`) only AFTER the
        // recycle/unlink block above. When the entry being recycled was the head
        // of this same bucket, the unlink moves the bucket head, so reading the
        // head before unlinking would splice the recycled entry onto itself
        // (next == prev == entry_index → a self-referential cycle that makes
        // `find_match` loop forever). Read it here, post-unlink, like C.
        let head = self.start[hindex] as usize;

        self.entries[entry_index] = HistEntry {
            next: head,
            prev: INVALID_ENTRY,
            hindex,
            pos,
        };
        self.entries[head].prev = entry_index;
        self.start[hindex] = entry_index as int16;

        self.next += 1;
        if self.next >= PGLZ_HISTORY_SIZE + 1 {
            self.next = 1;
            self.recycle = true;
        }
    }

    /// `pglz_find_match`: scan the history list for the longest match at `pos`.
    /// Returns `(best_len, best_off)` when a match longer than 2 is found.
    fn find_match(
        &self,
        input: &[u8],
        pos: usize,
        mut good_match: usize,
        good_drop: usize,
    ) -> Option<(usize, usize)> {
        let mut entry_index = self.start[hist_idx(input, pos, self.mask)] as usize;
        let mut best_len = 0;
        let mut best_off = 0;

        while entry_index != INVALID_ENTRY {
            let entry = self.entries[entry_index];
            let hp = entry.pos;
            let thisoff = pos - hp;
            // Stop if the offset does not fit in 12 bits.
            if thisoff >= 0x0fff {
                break;
            }

            let mut thislen = 0;
            // Determine length of match. A better match must be larger than the
            // best so far. And if we already have a match of 16 or more bytes,
            // it's worth the call overhead to use memcmp() to check if this
            // match is equal for the same size. After that we must fallback to
            // character by character comparison to know the exact position
            // where the diff occurred.
            if best_len >= 16 {
                if input[pos..pos + best_len] == input[hp..hp + best_len] {
                    thislen = best_len;
                    while pos + thislen < input.len()
                        && input[pos + thislen] == input[hp + thislen]
                        && thislen < PGLZ_MAX_MATCH
                    {
                        thislen += 1;
                    }
                }
            } else {
                while pos + thislen < input.len()
                    && input[pos + thislen] == input[hp + thislen]
                    && thislen < PGLZ_MAX_MATCH
                {
                    thislen += 1;
                }
            }

            if thislen > best_len {
                best_len = thislen;
                best_off = thisoff;
            }

            // Advance to the next history entry. Reduce the good match size by
            // the percentage and stop if the current best is at least as good.
            entry_index = entry.next;
            if entry_index != INVALID_ENTRY {
                if best_len >= good_match {
                    break;
                }
                good_match -= (good_match * good_drop) / 100;
            }
        }

        (best_len > 2).then_some((best_len, best_off))
    }
}

/// `pglz_compress`: compress `source` under `strategy` (or the default strategy
/// when `None`), returning the compressed bytes (charged to `mcx`) or a
/// [`PglzError`]. A `PgError` propagates only from allocation failure.
pub fn pglz_compress<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    strategy: Option<&PGLZ_Strategy>,
) -> PgResult<Result<PgVec<'mcx, u8>, PglzError>> {
    let slen = match int32::try_from(source.len()) {
        Ok(v) => v,
        Err(_) => return Ok(Err(PglzError::SizeOverflow)),
    };
    let strategy = strategy.unwrap_or(PGLZ_strategy_default());

    // If the strategy forbids compression (at all or if source chunk size out
    // of range), fail.
    if strategy.match_size_good <= 0
        || slen < strategy.min_input_size
        || slen > strategy.max_input_size
    {
        return Ok(Err(PglzError::InvalidStrategy));
    }

    // Limit the match parameters to the supported range.
    let good_match = (strategy.match_size_good as usize).clamp(17, PGLZ_MAX_MATCH);
    let good_drop = strategy.match_size_drop.clamp(0, 100) as usize;
    let need_rate = strategy.min_comp_rate.clamp(0, 99);

    // Compute the maximum result size allowed by the strategy. This had better
    // be <= slen, else we might overrun the provided output buffer.
    let result_max = if slen > int32::MAX / 100 {
        (slen / 100) * (100 - need_rate)
    } else {
        (slen * (100 - need_rate)) / 100
    } as usize;

    // Experimentally-chosen hash sizes; must be a power of two.
    let hash_size = if slen < 128 {
        512
    } else if slen < 256 {
        1024
    } else if slen < 512 {
        2048
    } else if slen < 1024 {
        4096
    } else {
        PGLZ_MAX_HISTORY_LISTS
    };

    let mut history = History::new(mcx, hash_size)?;

    // The compressed stream never exceeds PGLZ_MAX_OUTPUT(slen): we bail when
    // `output.len() >= result_max` (checked once per loop) and the loop body
    // emits at most 4 bytes. Reserve that validated upper bound up front.
    let mut output: PgVec<u8> = vec_with_capacity_in(mcx, PGLZ_MAX_OUTPUT(source.len()))?;
    let mut pos = 0;
    let mut ctrl_index: Option<usize> = None;
    let mut ctrlb = 0_u8;
    let mut ctrl = 0_u8;
    let mut found_match = false;

    // Compress the source directly into the output buffer.
    while pos < source.len() {
        // If we already exceeded the maximum result size, fail.
        if output.len() >= result_max {
            return Ok(Err(PglzError::CompressionFailed));
        }
        // If we've emitted more than first_success_by bytes without finding
        // anything compressible at all, fail. C compares the signed
        // `bp - bstart` against the signed `first_success_by`, so a negative
        // `first_success_by` fires immediately; clamp to >= 0 before the cast.
        if !found_match && output.len() >= strategy.first_success_by.max(0) as usize {
            return Ok(Err(PglzError::CompressionFailed));
        }

        if let Some((mut match_len, match_off)) =
            history.find_match(source, pos, good_match, good_drop)
        {
            // Create the tag and add history entries for all matched chars.
            out_ctrl(mcx, &mut output, &mut ctrl_index, &mut ctrlb, &mut ctrl)?;
            ctrlb |= ctrl;
            ctrl = ctrl.wrapping_shl(1);

            if match_len > 17 {
                push3(
                    mcx,
                    &mut output,
                    (((match_off & 0x0f00) >> 4) | 0x0f) as u8,
                    (match_off & 0x00ff) as u8,
                    (match_len - 18) as u8,
                )?;
            } else {
                push2(
                    mcx,
                    &mut output,
                    (((match_off & 0x0f00) >> 4) | (match_len - 3)) as u8,
                    (match_off & 0x00ff) as u8,
                )?;
            }

            while match_len != 0 {
                history.add(source, pos);
                pos += 1;
                match_len -= 1;
            }
            found_match = true;
        } else {
            // No match found. Copy one literal byte.
            out_ctrl(mcx, &mut output, &mut ctrl_index, &mut ctrlb, &mut ctrl)?;
            output.push(source[pos]);
            ctrl = ctrl.wrapping_shl(1);
            history.add(source, pos);
            pos += 1;
        }
    }

    // Write out the last control byte and check that we haven't overrun the
    // output size allowed by the strategy.
    if let Some(index) = ctrl_index {
        output[index] = ctrlb;
    }
    if output.len() >= result_max {
        return Ok(Err(PglzError::CompressionFailed));
    }

    Ok(Ok(output))
}

/// `pglz_decompress`: decompress `source` into a freshly allocated buffer of
/// `rawsize` bytes (charged to `mcx`), returning the bytes actually written.
///
/// When `check_complete` is true the stream is considered corrupt unless it
/// exactly fills `rawsize` bytes and consumes all of `source`.
pub fn pglz_decompress<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    rawsize: usize,
    check_complete: bool,
) -> PgResult<Result<PgVec<'mcx, u8>, PglzError>> {
    let mut output: PgVec<u8> = vec_with_capacity_in(mcx, rawsize)?;
    output.resize(rawsize, 0);
    match pglz_decompress_to_slice(source, &mut output, check_complete) {
        Ok(len) => {
            output.truncate(len);
            Ok(Ok(output))
        }
        Err(e) => Ok(Err(e)),
    }
}

/// Decompress `source` directly into `dest`, returning the number of bytes
/// written. Decompression stops when either the source is exhausted or `dest`
/// is full (a "slice" extraction). See [`pglz_decompress`] for `check_complete`.
///
/// This is the pure-logic core of `pglz_decompress`; it allocates nothing.
pub fn pglz_decompress_to_slice(
    source: &[u8],
    dest: &mut [u8],
    check_complete: bool,
) -> Result<usize, PglzError> {
    let mut sp = 0;
    let mut dp = 0;

    while sp < source.len() && dp < dest.len() {
        // Read one control byte and process the next 8 items (or as many as
        // remain in the compressed input).
        let mut ctrl = source[sp];
        sp += 1;

        for _ in 0..8 {
            if sp >= source.len() || dp >= dest.len() {
                break;
            }

            if ctrl & 1 != 0 {
                // Set control bit means we must read a match tag. The match is
                // coded with two bytes. First byte uses lower nibble to code
                // length - 3. Higher nibble contains upper 4 bits of the
                // offset. The next following byte contains the lower 8 bits of
                // the offset. If the length is coded as 18, another extension
                // tag byte tells how much longer the match really was (0-255).
                if sp + 2 > source.len() {
                    return Err(PglzError::CorruptInput);
                }
                let mut len = usize::from(source[sp] & 0x0f) + 3;
                let mut off = usize::from(source[sp] & 0xf0) << 4 | usize::from(source[sp + 1]);
                sp += 2;
                if len == 18 {
                    if sp >= source.len() {
                        return Err(PglzError::CorruptInput);
                    }
                    len += usize::from(source[sp]);
                    sp += 1;
                }

                // Now we copy the bytes specified by the tag from OUTPUT to
                // OUTPUT (so backwards-overlapping copies are possible). A zero
                // offset or one pointing before the start of output is corrupt.
                if off == 0 || off > dp {
                    return Err(PglzError::CorruptInput);
                }

                // Don't emit more than the destination can hold.
                len = min(len, dest.len() - dp);
                while off < len {
                    let src = dp - off;
                    dest.copy_within(src..src + off, dp);
                    len -= off;
                    dp += off;
                    off += off;
                }
                let src = dp - off;
                dest.copy_within(src..src + len, dp);
                dp += len;
            } else {
                // An unset control bit means LITERAL BYTE. So we just copy one
                // from INPUT to OUTPUT.
                dest[dp] = source[sp];
                sp += 1;
                dp += 1;
            }

            ctrl >>= 1;
        }
    }

    // Check we decompressed the right amount. If we are slicing, then we won't
    // necessarily be at the end of the source or dest buffers when we hit a
    // stop, so we only check this if requested.
    if check_complete && (dp != dest.len() || sp != source.len()) {
        return Err(PglzError::CorruptInput);
    }

    Ok(dp)
}

/// `pglz_maximum_compressed_size` — the largest compressed size that could
/// yield at least `rawsize` decompressed bytes, capped at
/// `total_compressed_size`.
pub fn pglz_maximum_compressed_size(rawsize: int32, total_compressed_size: int32) -> int32 {
    // Use int64 to prevent overflow during calculation.
    let compressed_size =
        ((i64::from(rawsize) * 9 + 7) / 8 + 2).min(i64::from(total_compressed_size));
    compressed_size as int32
}

/// `pglz_out_ctrl`: output the last control byte and allocate a new one if the
/// previous group of 8 items is complete.
fn out_ctrl<'mcx>(
    mcx: Mcx<'mcx>,
    output: &mut PgVec<'mcx, u8>,
    ctrl_index: &mut Option<usize>,
    ctrlb: &mut u8,
    ctrl: &mut u8,
) -> PgResult<()> {
    if u16::from(*ctrl) & 0xff == 0 {
        if let Some(index) = *ctrl_index {
            output[index] = *ctrlb;
        }
        *ctrl_index = Some(output.len());
        push(mcx, output, 0)?;
        *ctrlb = 0;
        *ctrl = 1;
    }
    Ok(())
}

/// Push one byte, charging the (already-reserved) growth to `mcx` and failing
/// with the context's OOM error if a reallocation cannot be satisfied.
fn push<'mcx>(mcx: Mcx<'mcx>, output: &mut PgVec<'mcx, u8>, b: u8) -> PgResult<()> {
    output
        .try_reserve(1)
        .map_err(|_| mcx.oom(output.len() + 1))?;
    output.push(b);
    Ok(())
}

fn push2<'mcx>(mcx: Mcx<'mcx>, output: &mut PgVec<'mcx, u8>, a: u8, b: u8) -> PgResult<()> {
    push(mcx, output, a)?;
    push(mcx, output, b)?;
    Ok(())
}

fn push3<'mcx>(mcx: Mcx<'mcx>, output: &mut PgVec<'mcx, u8>, a: u8, b: u8, c: u8) -> PgResult<()> {
    push(mcx, output, a)?;
    push(mcx, output, b)?;
    push(mcx, output, c)?;
    Ok(())
}

/// `pglz_hist_idx`: C reads the input through `const char *`, which is *signed*
/// `char` on all platforms PostgreSQL targets (x86-64, arm64). The shift/xor
/// terms therefore sign-extend bytes >= 0x80 before the `& mask`. We reproduce
/// that sign-extension (`u8 -> i8 -> i32`) exactly, or we pick a different hash
/// bucket and emit a different (still-valid but non-identical) compressed
/// stream. See pg_lzcompress.c:277-281.
fn hist_idx(input: &[u8], pos: usize, mask: usize) -> usize {
    let value = if input.len() - pos < 4 {
        i32::from(input[pos] as i8)
    } else {
        (i32::from(input[pos] as i8) << 6)
            ^ (i32::from(input[pos + 1] as i8) << 4)
            ^ (i32::from(input[pos + 2] as i8) << 2)
            ^ i32::from(input[pos + 3] as i8)
    };

    (value as usize) & mask
}

/// Seam adapter for `common_pglz_seams::pglz_decompress_to_slice`.
///
/// The seam contract returns `PgResult<Option<usize>>`, where `None` is C's
/// `-1` (corrupt input) — the caller then raises its own `ereport`. This is a
/// pure, allocation-free byte transform, so the `PgResult` here is always
/// `Ok`; we marshal the in-crate `Result<usize, PglzError>` into it.
fn seam_pglz_decompress_to_slice(
    source: &[u8],
    dest: &mut [u8],
    check_complete: bool,
) -> PgResult<Option<usize>> {
    match pglz_decompress_to_slice(source, dest, check_complete) {
        Ok(len) => Ok(Some(len)),
        Err(_) => Ok(None),
    }
}

/// Install this crate's seams. Contains only `set()` calls; `init_all()` in
/// `seams-init` invokes it at startup.
pub fn init_seams() {
    common_pglz_seams::pglz_decompress_to_slice::set(seam_pglz_decompress_to_slice);
    common_pglz_seams::pglz_maximum_compressed_size::set(pglz_maximum_compressed_size);
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;
    use std::vec;
    use std::vec::Vec;

    fn compress(input: &[u8], s: Option<&PGLZ_Strategy>) -> Option<Vec<u8>> {
        let ctx = MemoryContext::new("test-compress");
        let r = match pglz_compress(ctx.mcx(), input, s).unwrap() {
            Ok(out) => Some(out.iter().copied().collect()),
            Err(_) => None,
        };
        r
    }

    fn decompress(src: &[u8], rawsize: usize, complete: bool) -> Result<Vec<u8>, PglzError> {
        let ctx = MemoryContext::new("test-decompress");
        let r = pglz_decompress(ctx.mcx(), src, rawsize, complete)
            .unwrap()
            .map(|out| out.iter().copied().collect());
        r
    }

    #[test]
    fn default_strategy_rejects_small_input() {
        let ctx = MemoryContext::new("t");
        assert_eq!(
            pglz_compress(ctx.mcx(), b"short", None).unwrap(),
            Err(PglzError::InvalidStrategy)
        );
    }

    #[test]
    fn always_strategy_roundtrips_repetitive_input() {
        let input = b"abcabcabcabcabcabcabcabcabcabcabcabc";
        let compressed = compress(input, Some(PGLZ_strategy_always())).unwrap();
        assert_eq!(decompress(&compressed, input.len(), true).unwrap(), input);
        assert!(compressed.len() < input.len());
    }

    #[test]
    fn default_strategy_roundtrips_large_repetitive_input() {
        let input = vec![b'x'; 2048];
        let compressed = compress(&input, None).unwrap();
        assert_eq!(decompress(&compressed, input.len(), true).unwrap(), input);
    }

    #[test]
    fn decompresses_literal_stream() {
        let compressed = [0_u8, b'a', b'b', b'c'];
        assert_eq!(decompress(&compressed, 3, true).unwrap(), b"abc");
    }

    #[test]
    fn decompresses_match_stream() {
        let compressed = [0b0000_0010, b'a', 0x01, 0x01];
        assert_eq!(decompress(&compressed, 4, true).unwrap(), b"aaaa");
    }

    #[test]
    fn incomplete_decompression_is_corrupt_when_checked() {
        let compressed = [0_u8, b'a'];
        assert_eq!(
            decompress(&compressed, 2, true),
            Err(PglzError::CorruptInput)
        );
        assert_eq!(decompress(&compressed, 2, false).unwrap(), b"a");
    }

    #[test]
    fn rejects_bad_backreference() {
        let compressed = [1_u8, 0x00, 0x00];
        assert_eq!(
            decompress(&compressed, 3, true),
            Err(PglzError::CorruptInput)
        );
    }

    #[test]
    fn maximum_compressed_size_matches_formula() {
        assert_eq!(pglz_maximum_compressed_size(0, 100), 2);
        assert_eq!(pglz_maximum_compressed_size(8, 100), 11);
        assert_eq!(pglz_maximum_compressed_size(100, 50), 50);
    }

    #[test]
    fn max_output_matches_macro() {
        assert_eq!(PGLZ_MAX_OUTPUT(0), 4);
        assert_eq!(PGLZ_MAX_OUTPUT(100), 104);
    }

    #[test]
    fn hist_idx_sign_extends_high_bytes_four_byte_branch() {
        let input = [0x80_u8; 4];
        assert_eq!(hist_idx(&input, 0, 0x3FF), 384);
    }

    #[test]
    fn hist_idx_sign_extends_high_byte_short_branch() {
        let input = [0x80_u8];
        assert_eq!(hist_idx(&input, 0, 0x1FF), 384);
        let tail = [0x00_u8, 0x00, 0x80];
        assert_eq!(hist_idx(&tail, 2, 0x1FF), 384);
    }

    #[test]
    fn hist_idx_low_bytes_unaffected_by_sign_fix() {
        let input = b"abcd";
        let expected = ((i32::from(b'a') << 6)
            ^ (i32::from(b'b') << 4)
            ^ (i32::from(b'c') << 2)
            ^ i32::from(b'd')) as usize
            & 0xFFF;
        assert_eq!(hist_idx(input, 0, 0xFFF), expected);
    }

    #[test]
    fn negative_match_size_drop_roundtrips() {
        let strategy = PGLZ_Strategy::new(32, 1 << 20, 25, 512, 128, -50);
        let input = vec![b'z'; 4096];
        let compressed = compress(&input, Some(&strategy)).unwrap();
        assert_eq!(decompress(&compressed, input.len(), true).unwrap(), input);
        assert!(compressed.len() < input.len());
    }

    #[test]
    fn high_byte_input_roundtrips() {
        let mut input = Vec::new();
        for i in 0..2048u32 {
            input.push(0x80u8.wrapping_add((i % 7) as u8));
        }
        let compressed = compress(&input, Some(PGLZ_strategy_always())).unwrap();
        assert_eq!(decompress(&compressed, input.len(), true).unwrap(), input);
    }
}

#[cfg(test)]
mod spin_repro {
    use super::*;
    use mcx::MemoryContext;
    use std::vec::Vec;

    // Deterministic LCG mimicking `string_agg(random()::text,'')` — a long,
    // poorly-compressible run of digits/dots. This is the arrays.sql input that
    // hung the backend at 100% CPU before the `add` head-read ordering fix:
    // recycling an entry that was the head of its own bucket spliced it onto
    // itself (next == prev == self), so `find_match` looped forever.
    fn gen(n: usize) -> Vec<u8> {
        let mut s: u64 = 0x12345678;
        let mut out = Vec::with_capacity(n);
        let digits = b"0123456789.";
        while out.len() < n {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            out.push(digits[((s >> 33) as usize) % digits.len()]);
        }
        out
    }

    // Build the history exactly as `pglz_compress` does and assert no bucket
    // ever forms a cycle (a self-loop bounds the walk at > table size).
    #[test]
    fn history_never_cycles_on_large_input() {
        let input = gen(370_000);
        let ctx = MemoryContext::new("cyc");
        let mut history = History::new(ctx.mcx(), PGLZ_MAX_HISTORY_LISTS).unwrap();
        for pos in 0..input.len() {
            let hidx = hist_idx(&input, pos, history.mask);
            let mut e = history.start[hidx] as usize;
            let mut count = 0usize;
            while e != INVALID_ENTRY {
                count += 1;
                assert!(
                    count <= PGLZ_HISTORY_SIZE,
                    "bucket {hidx} cycles at pos {pos} (walked {count} > table size)"
                );
                e = history.entries[e].next;
            }
            history.add(&input, pos);
        }
    }

    // End-to-end: the full compressor must terminate and round-trip on the
    // large, poorly-compressible input that used to hang.
    #[test]
    fn large_randomish_text_terminates_and_roundtrips() {
        let input = gen(370_000);
        let ctx = MemoryContext::new("spin");
        let compressed: Option<Vec<u8>> = match pglz_compress(ctx.mcx(), &input, None).unwrap() {
            Ok(out) => Some(out.iter().copied().collect()),
            // Random text often won't meet the compression rate; that's fine —
            // the point is that compression *terminates*.
            Err(_) => None,
        };
        if let Some(bytes) = compressed {
            let back: Vec<u8> = super::pglz_decompress(ctx.mcx(), &bytes, input.len(), true)
                .unwrap()
                .unwrap()
                .iter()
                .copied()
                .collect();
            assert_eq!(back, input);
        }
    }
}
