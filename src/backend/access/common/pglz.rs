use std::cmp::min;

const PGLZ_HISTORY_SIZE: usize = 4096;
const PGLZ_MAX_MATCH: usize = 273;

#[derive(Debug, Clone, Copy)]
pub(crate) struct PglzStrategy {
    pub(crate) min_input_size: usize,
    pub(crate) max_input_size: usize,
    pub(crate) min_comp_rate: usize,
    pub(crate) first_success_by: usize,
    pub(crate) match_size_good: usize,
    pub(crate) match_size_drop: usize,
}

pub(crate) const PGLZ_STRATEGY_DEFAULT: PglzStrategy = PglzStrategy {
    min_input_size: 32,
    max_input_size: usize::MAX,
    min_comp_rate: 25,
    first_success_by: 1024,
    match_size_good: 128,
    match_size_drop: 10,
};

#[derive(Debug, Clone, Copy, Default)]
struct HistEntry {
    next: usize,
    prev: usize,
    hindex: usize,
    pos: usize,
}

fn hist_idx(source: &[u8], pos: usize, mask: usize) -> usize {
    if source.len().saturating_sub(pos) < 4 {
        usize::from(source[pos]) & mask
    } else {
        (((usize::from(source[pos])) << 6)
            ^ ((usize::from(source[pos + 1])) << 4)
            ^ ((usize::from(source[pos + 2])) << 2)
            ^ usize::from(source[pos + 3]))
            & mask
    }
}

fn hist_add(
    hist_start: &mut [usize],
    hist_entries: &mut [HistEntry],
    hist_next: &mut usize,
    hist_recycle: &mut bool,
    source: &[u8],
    pos: usize,
    mask: usize,
) {
    let hindex = hist_idx(source, pos, mask);
    let entry_index = *hist_next;

    if *hist_recycle {
        let old = hist_entries[entry_index];
        if old.prev == 0 {
            hist_start[old.hindex] = old.next;
        } else {
            hist_entries[old.prev].next = old.next;
        }
        if old.next != 0 {
            hist_entries[old.next].prev = old.prev;
        }
    }

    let old_head = hist_start[hindex];
    hist_entries[entry_index] = HistEntry {
        next: old_head,
        prev: 0,
        hindex,
        pos,
    };
    if old_head != 0 {
        hist_entries[old_head].prev = entry_index;
    }
    hist_start[hindex] = entry_index;

    *hist_next += 1;
    if *hist_next >= PGLZ_HISTORY_SIZE + 1 {
        *hist_next = 1;
        *hist_recycle = true;
    }
}

fn find_match(
    hist_start: &[usize],
    hist_entries: &[HistEntry],
    source: &[u8],
    pos: usize,
    lenp: &mut usize,
    offp: &mut usize,
    mut good_match: usize,
    good_drop: usize,
    mask: usize,
) -> bool {
    let mut entry_index = hist_start[hist_idx(source, pos, mask)];
    let mut best_len = 0usize;
    let mut best_off = 0usize;

    while entry_index != 0 {
        let hp = hist_entries[entry_index].pos;
        let this_off = pos - hp;
        if this_off >= 0x0fff {
            break;
        }

        let mut this_len = 0usize;
        if best_len >= 16 {
            if source[hp..].starts_with(&source[pos..pos + best_len]) {
                this_len = best_len;
                while pos + this_len < source.len()
                    && source[pos + this_len] == source[hp + this_len]
                    && this_len < PGLZ_MAX_MATCH
                {
                    this_len += 1;
                }
            }
        } else {
            while pos + this_len < source.len()
                && source[pos + this_len] == source[hp + this_len]
                && this_len < PGLZ_MAX_MATCH
            {
                this_len += 1;
            }
        }

        if this_len > best_len {
            best_len = this_len;
            best_off = this_off;
        }

        entry_index = hist_entries[entry_index].next;
        if entry_index != 0 {
            if best_len >= good_match {
                break;
            }
            good_match -= (good_match * good_drop) / 100;
        }
    }

    if best_len > 2 {
        *lenp = best_len;
        *offp = best_off;
        true
    } else {
        false
    }
}

fn maybe_rotate_control(out: &mut Vec<u8>, ctrl_idx: &mut usize, ctrl_bits: &mut u8, ctrl_mask: &mut u8) {
    if *ctrl_mask == 0 {
        out[*ctrl_idx] = *ctrl_bits;
        *ctrl_idx = out.len();
        out.push(0);
        *ctrl_bits = 0;
        *ctrl_mask = 1;
    }
}

fn emit_literal(out: &mut Vec<u8>, ctrl_idx: &mut usize, ctrl_bits: &mut u8, ctrl_mask: &mut u8, byte: u8) {
    maybe_rotate_control(out, ctrl_idx, ctrl_bits, ctrl_mask);
    out.push(byte);
    *ctrl_mask = ctrl_mask.wrapping_shl(1);
}

fn emit_tag(
    out: &mut Vec<u8>,
    ctrl_idx: &mut usize,
    ctrl_bits: &mut u8,
    ctrl_mask: &mut u8,
    len: usize,
    off: usize,
) {
    maybe_rotate_control(out, ctrl_idx, ctrl_bits, ctrl_mask);
    *ctrl_bits |= *ctrl_mask;
    *ctrl_mask = ctrl_mask.wrapping_shl(1);

    if len > 17 {
        out.push((((off & 0xf00) >> 4) as u8) | 0x0f);
        out.push((off & 0xff) as u8);
        out.push((len - 18) as u8);
    } else {
        out.push((((off & 0xf00) >> 4) as u8) | ((len - 3) as u8));
        out.push((off & 0xff) as u8);
    }
}

pub(crate) fn compress(source: &[u8], strategy: Option<&PglzStrategy>) -> Option<Vec<u8>> {
    let strategy = strategy.unwrap_or(&PGLZ_STRATEGY_DEFAULT);
    if strategy.match_size_good == 0
        || source.len() < strategy.min_input_size
        || source.len() > strategy.max_input_size
    {
        return None;
    }

    let good_match = strategy.match_size_good.clamp(17, PGLZ_MAX_MATCH);
    let good_drop = strategy.match_size_drop.min(100);
    let need_rate = strategy.min_comp_rate.min(99);
    let result_max = (source.len() * (100 - need_rate)) / 100;
    let hashsz = if source.len() < 128 {
        512
    } else if source.len() < 256 {
        1024
    } else if source.len() < 512 {
        2048
    } else if source.len() < 1024 {
        4096
    } else {
        8192
    };
    let mask = hashsz - 1;

    let mut hist_start = vec![0usize; hashsz];
    let mut hist_entries = vec![HistEntry::default(); PGLZ_HISTORY_SIZE + 1];
    let mut hist_next = 1usize;
    let mut hist_recycle = false;
    let mut pos = 0usize;
    let mut out = vec![0];
    let mut ctrl_idx = 0usize;
    let mut ctrl_bits = 0u8;
    let mut ctrl_mask = 1u8;
    let mut found_match = false;

    while pos < source.len() {
        if out.len() >= result_max {
            return None;
        }
        if !found_match && out.len() >= strategy.first_success_by {
            return None;
        }

        let mut match_len = 0usize;
        let mut match_off = 0usize;
        if find_match(
            &hist_start,
            &hist_entries,
            source,
            pos,
            &mut match_len,
            &mut match_off,
            good_match,
            good_drop,
            mask,
        ) {
            emit_tag(
                &mut out,
                &mut ctrl_idx,
                &mut ctrl_bits,
                &mut ctrl_mask,
                match_len,
                match_off,
            );
            for _ in 0..match_len {
                hist_add(
                    &mut hist_start,
                    &mut hist_entries,
                    &mut hist_next,
                    &mut hist_recycle,
                    source,
                    pos,
                    mask,
                );
                pos += 1;
            }
            found_match = true;
        } else {
            emit_literal(
                &mut out,
                &mut ctrl_idx,
                &mut ctrl_bits,
                &mut ctrl_mask,
                source[pos],
            );
            hist_add(
                &mut hist_start,
                &mut hist_entries,
                &mut hist_next,
                &mut hist_recycle,
                source,
                pos,
                mask,
            );
            pos += 1;
        }
    }

    out[ctrl_idx] = ctrl_bits;
    if out.len() >= result_max {
        None
    } else {
        Some(out)
    }
}

pub(crate) fn decompress(source: &[u8], rawsize: usize, check_complete: bool) -> Option<Vec<u8>> {
    let mut src_pos = 0usize;
    let mut out = Vec::with_capacity(rawsize);

    while src_pos < source.len() && out.len() < rawsize {
        let mut ctrl = *source.get(src_pos)?;
        src_pos += 1;

        for _ in 0..8 {
            if src_pos >= source.len() || out.len() >= rawsize {
                break;
            }

            if ctrl & 1 != 0 {
                let first = *source.get(src_pos)?;
                let second = *source.get(src_pos + 1)?;
                src_pos += 2;

                let mut len = usize::from(first & 0x0f) + 3;
                let off = (usize::from(first & 0xf0) << 4) | usize::from(second);
                if len == 18 {
                    len += usize::from(*source.get(src_pos)?);
                    src_pos += 1;
                }
                if off == 0 || off > out.len() {
                    return None;
                }

                len = min(len, rawsize - out.len());
                let start = out.len() - off;
                for index in 0..len {
                    let byte = out[start + index % off];
                    out.push(byte);
                }
            } else {
                out.push(*source.get(src_pos)?);
                src_pos += 1;
            }

            ctrl >>= 1;
        }
    }

    if check_complete && (out.len() != rawsize || src_pos != source.len()) {
        None
    } else {
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_repetitive_input() {
        let input = b"abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc".to_vec();
        let compressed = compress(&input, None).expect("input should compress");
        let decompressed = decompress(&compressed, input.len(), true).expect("must decompress");
        assert_eq!(decompressed, input);
    }

    #[test]
    fn rejects_incompressible_short_input() {
        assert!(compress(b"tiny", None).is_none());
    }
}
