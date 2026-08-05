//! pg_lzcompress.c: PGLZ compressor/decompressor. Streams are bit-compatible
//! with C; decompress is the per-detoast hot kernel (C's match/literal loop
//! with the doubling-offset memcpy), compress uses C's static history arrays
//! as thread-local scratch.

use core::cmp::min;
use core::ffi::c_char;
use core::mem::MaybeUninit;

pub const PGLZ_MAX_HISTORY_LISTS: usize = 8192;
pub const PGLZ_HISTORY_SIZE: usize = 4096;
pub const PGLZ_MAX_MATCH: usize = 273;

pub const fn pglz_max_output(slen: usize) -> usize {
    slen + 4
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PglzStrategy {
    pub min_input_size: i32,
    pub max_input_size: i32,
    pub min_comp_rate: i32,
    pub first_success_by: i32,
    pub match_size_good: i32,
    pub match_size_drop: i32,
}

pub const PGLZ_STRATEGY_DEFAULT: PglzStrategy = PglzStrategy {
    min_input_size: 32,
    max_input_size: i32::MAX,
    min_comp_rate: 25,
    first_success_by: 1024,
    match_size_good: 128,
    match_size_drop: 10,
};

pub const PGLZ_STRATEGY_ALWAYS: PglzStrategy = PglzStrategy {
    min_input_size: 0,
    max_input_size: i32::MAX,
    min_comp_rate: 0,
    first_success_by: i32::MAX,
    match_size_good: 128,
    match_size_drop: 6,
};

/// C `pglz_decompress`; `None` is C's -1 (corrupt input). Returns the byte
/// count written to `dest`, whose prefix of that length is then initialized.
/// Stops when source or dest is exhausted; `check_complete` demands both end
/// exactly together.
pub fn pglz_decompress(
    source: &[u8],
    dest: &mut [MaybeUninit<u8>],
    check_complete: bool,
) -> Option<usize> {
    let mut sp = source.as_ptr();
    // SAFETY: one-past-the-end pointers of live slices.
    let srcend = unsafe { sp.add(source.len()) };
    let base = dest.as_mut_ptr().cast::<u8>();
    let mut dp = base;
    let destend = unsafe { base.add(dest.len()) };

    while sp < srcend && dp < destend {
        // SAFETY: sp < srcend.
        let mut ctrl = unsafe {
            let c = *sp;
            sp = sp.add(1);
            c
        };

        let mut ctrlc = 0;
        while ctrlc < 8 && sp < srcend && dp < destend {
            if ctrl & 1 != 0 {
                // C reads both tag bytes (and the extension byte) before its
                // sp > srcend overrun check; check first instead — the corrupt
                // verdict is identical, the OOB read is not reproduced.
                if unsafe { srcend.offset_from(sp) } < 2 {
                    return None;
                }
                // SAFETY: two bytes available per the check above.
                let (mut len, mut off) = unsafe {
                    let t0 = *sp as usize;
                    let t1 = *sp.add(1) as usize;
                    sp = sp.add(2);
                    ((t0 & 0x0f) + 3, ((t0 & 0xf0) << 4) | t1)
                };
                if len == 18 {
                    if sp >= srcend {
                        return None;
                    }
                    // SAFETY: sp < srcend.
                    unsafe {
                        len += *sp as usize;
                        sp = sp.add(1);
                    }
                }

                // off = 0 would loop forever; off beyond the output start
                // reads outside the buffer (both are corrupt data).
                // SAFETY: base <= dp <= destend throughout.
                if off == 0 || off > unsafe { dp.offset_from(base) } as usize {
                    return None;
                }

                // SAFETY: dp <= destend.
                len = min(len, unsafe { destend.offset_from(dp) } as usize);

                // Copy len bytes from dp-off to dp with memcpy over provably
                // non-overlapping spans: copy off bytes, then the repeat
                // period doubles (C's comment at pg_lzcompress.c:763-798).
                // SAFETY: dp-off >= base (checked above) and dp+len <= destend
                // (len clamped); every source byte below dp was written by an
                // earlier literal/match, so reads are from initialized bytes;
                // src+count <= dp keeps each copy non-overlapping.
                unsafe {
                    while off < len {
                        core::ptr::copy_nonoverlapping(dp.sub(off), dp, off);
                        len -= off;
                        dp = dp.add(off);
                        off += off;
                    }
                    core::ptr::copy_nonoverlapping(dp.sub(off), dp, len);
                    dp = dp.add(len);
                }
            } else {
                // SAFETY: sp < srcend and dp < destend per the loop condition.
                unsafe {
                    *dp = *sp;
                    dp = dp.add(1);
                    sp = sp.add(1);
                }
            }

            ctrl >>= 1;
            ctrlc += 1;
        }
    }

    if check_complete && (dp != destend || sp != srcend) {
        return None;
    }

    // SAFETY: base <= dp <= destend.
    Some(unsafe { dp.offset_from(base) } as usize)
}

/// [`pglz_decompress`] into an already-initialized buffer.
pub fn pglz_decompress_slice(source: &[u8], dest: &mut [u8], check_complete: bool) -> Option<usize> {
    // SAFETY: [u8] viewed as [MaybeUninit<u8>]; the kernel never writes uninit.
    let dest = unsafe {
        core::slice::from_raw_parts_mut(dest.as_mut_ptr().cast::<MaybeUninit<u8>>(), dest.len())
    };
    pglz_decompress(source, dest, check_complete)
}

#[derive(Clone, Copy, Default)]
struct HistEntry {
    next: u16,
    prev: u16,
    hindex: u16,
    pos: u32,
}

const INVALID_ENTRY: u16 = 0;

// C's static hist_start/hist_entries work arrays (rule 10: backend-global is
// thread-local). Entry 0 is the invalid sentinel and is scribbled on freely.
struct Hist {
    start: [i16; PGLZ_MAX_HISTORY_LISTS],
    entries: [HistEntry; PGLZ_HISTORY_SIZE + 1],
}

thread_local! {
    static HIST: core::cell::UnsafeCell<Hist> = const {
        core::cell::UnsafeCell::new(Hist {
            start: [0; PGLZ_MAX_HISTORY_LISTS],
            entries: [HistEntry { next: 0, prev: 0, hindex: 0, pos: 0 }; PGLZ_HISTORY_SIZE + 1],
        })
    };
}

// C reads input through `char` (signed on x86-64/Apple, unsigned on
// Linux-aarch64); reproduce the platform's sign extension exactly or we pick
// different hash buckets and emit valid-but-different streams than the C
// compiled beside us.
#[inline(always)]
fn hist_idx(input: &[u8], pos: usize, mask: i32) -> usize {
    let b = |i: usize| input[pos + i] as c_char as i32;
    let v = if input.len() - pos < 4 {
        b(0)
    } else {
        (b(0) << 6) ^ (b(1) << 4) ^ (b(2) << 2) ^ b(3)
    };
    (v & mask) as usize
}

impl Hist {
    #[inline]
    fn add(
        &mut self,
        input: &[u8],
        pos: usize,
        hist_next: &mut u16,
        recycle: &mut bool,
        mask: i32,
    ) {
        let hindex = hist_idx(input, pos, mask);
        let hn = *hist_next;
        if *recycle {
            let old = self.entries[hn as usize];
            if old.prev == INVALID_ENTRY {
                self.start[old.hindex as usize] = old.next as i16;
            } else {
                self.entries[old.prev as usize].next = old.next;
            }
            if old.next != INVALID_ENTRY {
                self.entries[old.next as usize].prev = old.prev;
            }
        }
        // Head must be read AFTER the unlink: recycling the head of this same
        // bucket otherwise splices the entry onto itself (infinite find loop).
        let head = self.start[hindex] as u16;
        self.entries[hn as usize] = HistEntry {
            next: head,
            prev: INVALID_ENTRY,
            hindex: hindex as u16,
            pos: pos as u32,
        };
        self.entries[head as usize].prev = hn;
        self.start[hindex] = hn as i16;
        *hist_next += 1;
        if *hist_next as usize >= PGLZ_HISTORY_SIZE + 1 {
            *hist_next = 1;
            *recycle = true;
        }
    }

    #[inline]
    fn find_match(
        &self,
        input: &[u8],
        pos: usize,
        mut good_match: i32,
        good_drop: i32,
        mask: i32,
    ) -> Option<(usize, usize)> {
        let mut hentno = self.start[hist_idx(input, pos, mask)] as u16;
        let mut len = 0usize;
        let mut off = 0usize;

        while hentno != INVALID_ENTRY {
            let hent = self.entries[hentno as usize];
            let hp = hent.pos as usize;
            let thisoff = pos - hp;
            if thisoff >= 0x0fff {
                break;
            }

            let mut thislen = 0usize;
            if len >= 16 {
                // Slice eq is memcmp; in-bounds because the best-so-far match
                // at this same `pos` already satisfied pos+len <= input.len().
                if input[pos..pos + len] == input[hp..hp + len] {
                    thislen = len;
                    while pos + thislen < input.len()
                        && input[hp + thislen] == input[pos + thislen]
                        && thislen < PGLZ_MAX_MATCH
                    {
                        thislen += 1;
                    }
                }
            } else {
                while pos + thislen < input.len()
                    && input[hp + thislen] == input[pos + thislen]
                    && thislen < PGLZ_MAX_MATCH
                {
                    thislen += 1;
                }
            }

            if thislen > len {
                len = thislen;
                off = thisoff;
            }

            hentno = hent.next;
            if hentno != INVALID_ENTRY {
                if len as i32 >= good_match {
                    break;
                }
                good_match -= (good_match * good_drop) / 100;
            }
        }

        (len > 2).then_some((len, off))
    }
}

/// C `pglz_compress`: compress `source` into `dest` (sized at least
/// [`pglz_max_output`]`(source.len())`), returning the compressed size or
/// `None` where C returns -1 (strategy rejects the input, or it did not
/// compress well enough to be worth storing).
pub fn pglz_compress_into(
    source: &[u8],
    dest: &mut [MaybeUninit<u8>],
    strategy: &PglzStrategy,
) -> Option<usize> {
    assert!(dest.len() >= pglz_max_output(source.len()));
    let slen = i32::try_from(source.len()).ok()?;

    if strategy.match_size_good <= 0
        || slen < strategy.min_input_size
        || slen > strategy.max_input_size
    {
        return None;
    }

    let good_match = strategy.match_size_good.clamp(17, PGLZ_MAX_MATCH as i32);
    let good_drop = strategy.match_size_drop.clamp(0, 100);
    let need_rate = strategy.min_comp_rate.clamp(0, 99);

    let result_max = if slen > i32::MAX / 100 {
        (slen / 100) * (100 - need_rate)
    } else {
        (slen * (100 - need_rate)) / 100
    } as usize;

    let hashsz: usize = if slen < 128 {
        512
    } else if slen < 256 {
        1024
    } else if slen < 512 {
        2048
    } else if slen < 1024 {
        4096
    } else {
        8192
    };
    let mask = (hashsz - 1) as i32;

    HIST.with(|cell| {
        // SAFETY: per-thread scratch; pglz_compress_into never re-enters (the
        // loop below calls nothing that can reach compression again).
        let hist = unsafe { &mut *cell.get() };
        hist.start[..hashsz].fill(0);

        let base = dest.as_mut_ptr().cast::<u8>();
        let mut bp = base;
        let mut hist_next: u16 = 1;
        let mut recycle = false;
        let mut dp = 0usize;
        let dend = source.len();
        let mut ctrl_dummy: u8 = 0;
        let mut ctrlp: *mut u8 = &mut ctrl_dummy;
        let mut ctrlb: u8 = 0;
        let mut ctrl: u8 = 0;
        let mut found_match = false;

        // SAFETY of the raw writes below: the loop emits at most 4 bytes per
        // iteration and bails once bp-base >= result_max <= source.len(), so
        // writes stay inside dest's asserted pglz_max_output(slen) capacity;
        // ctrlp always points at ctrl_dummy or an already-emitted dest byte.
        macro_rules! out_ctrl {
            () => {
                if ctrl == 0 {
                    unsafe {
                        *ctrlp = ctrlb;
                        ctrlp = bp;
                        bp = bp.add(1);
                    }
                    ctrlb = 0;
                    ctrl = 1;
                }
            };
        }

        while dp < dend {
            if unsafe { bp.offset_from(base) } as usize >= result_max {
                return None;
            }
            if !found_match
                && unsafe { bp.offset_from(base) } as i64 >= strategy.first_success_by as i64
            {
                return None;
            }

            if let Some((mut match_len, match_off)) =
                hist.find_match(source, dp, good_match, good_drop, mask)
            {
                out_ctrl!();
                ctrlb |= ctrl;
                ctrl = ctrl.wrapping_shl(1);
                unsafe {
                    if match_len > 17 {
                        *bp = (((match_off & 0xf00) >> 4) | 0x0f) as u8;
                        *bp.add(1) = (match_off & 0xff) as u8;
                        *bp.add(2) = (match_len - 18) as u8;
                        bp = bp.add(3);
                    } else {
                        *bp = (((match_off & 0xf00) >> 4) | (match_len - 3)) as u8;
                        *bp.add(1) = (match_off & 0xff) as u8;
                        bp = bp.add(2);
                    }
                }
                while match_len > 0 {
                    hist.add(source, dp, &mut hist_next, &mut recycle, mask);
                    dp += 1;
                    match_len -= 1;
                }
                found_match = true;
            } else {
                out_ctrl!();
                unsafe {
                    *bp = source[dp];
                    bp = bp.add(1);
                }
                ctrl = ctrl.wrapping_shl(1);
                hist.add(source, dp, &mut hist_next, &mut recycle, mask);
                dp += 1;
            }
        }

        // SAFETY: ctrlp targets ctrl_dummy or an emitted dest byte.
        unsafe { *ctrlp = ctrlb };
        let result_size = unsafe { bp.offset_from(base) } as usize;
        if result_size >= result_max {
            return None;
        }
        Some(result_size)
    })
}

pub fn pglz_maximum_compressed_size(rawsize: i32, total_compressed_size: i32) -> i32 {
    let compressed_size = (i64::from(rawsize) * 9 + 7) / 8 + 2;
    min(compressed_size, i64::from(total_compressed_size)) as i32
}

#[cfg(test)]
mod tests;
