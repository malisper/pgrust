//! `daitch_mokotoff.c` — Daitch-Mokotoff Soundex. The C pallocs `dm_node`s
//! into a per-call temp context and shares them by pointer (children +
//! alternating leaf lists); here the nodes live in a per-call arena indexed
//! by `u32`, matching the bulk-freed sharing without `Rc`.

use crate::dm_table::{DmCode, DmCodes, DmLetter, LETTER_ROOT};
use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

const DM_CODE_DIGITS: usize = 6;
const NODE_NONE: u32 = u32::MAX;

// Mapping from ISO8859-1 to upper-case ASCII, covering the range 0x60..0xFF.
static ISO8859_1_TO_ASCII_UPPER: &[u8; 160] =
    b"`ABCDEFGHIJKLMNOPQRSTUVWXYZ{|}~                                  !                             ?AAAAAAECEEEEIIIIDNOOOOO*OUUUUYDSAAAAAAECEEEEIIIIDNOOOOO/OUUUUYDY";

static END_CODES: [DmCodes; 1] = [[*b"X\0\0", *b"X\0\0", *b"X\0\0"]];

struct DmNode {
    soundex_length: usize,
    soundex: [u8; DM_CODE_DIGITS],
    is_leaf: bool,
    last_update: i32,
    prev_code_digits: [u8; 2],
    next_code_digits: [u8; 2],
    prev_code_index: i32,
    next_code_index: i32,
    children: [u32; 10],
    next: [u32; 2],
}

fn start_node() -> DmNode {
    DmNode {
        soundex_length: 0,
        soundex: *b"000000",
        is_leaf: false,
        last_update: 0,
        prev_code_digits: [0, 0],
        next_code_digits: [0, 0],
        prev_code_index: 0,
        next_code_index: 0,
        children: [NODE_NONE; 10],
        next: [NODE_NONE; 2],
    }
}

fn read_char(s: &[u8], ix: &mut usize) -> u8 {
    // Substitute character for skipped code points.
    const NA: u8 = 0x1A;
    if *ix >= s.len() {
        return 0;
    }
    let rest = &s[*ix..];
    let mblen = wchar::pg_utf_mblen(rest) as usize;
    let c = if mblen <= rest.len() {
        wchar::utf8_to_unicode(rest)
    } else {
        // Truncated tail (non-UTF8 server encodings): C decodes into its
        // cstring NUL terminator; zero-pad to match.
        let mut buf = [0u8; 4];
        buf[..rest.len()].copy_from_slice(rest);
        wchar::utf8_to_unicode(&buf)
    };
    if c != 0 {
        *ix += mblen;
    }
    match c {
        // ASCII [, \, ] are reserved for the conversions below.
        0x5B..=0x5D => NA,
        c if c < 0x60 => c as u8,
        c if c < 0x100 => ISO8859_1_TO_ASCII_UPPER[(c - 0x60) as usize],
        0x0104 | 0x0105 => b'[',  // A with ogonek
        0x0118 | 0x0119 => b'\\', // E with ogonek
        0x0162 | 0x0163 | 0x021A | 0x021B => b']', // T with cedilla / comma below
        _ => NA,
    }
}

fn read_valid_char(s: &[u8], ix: &mut usize) -> u8 {
    loop {
        let c = read_char(s, ix);
        if c == 0 || (b'A'..=b']').contains(&c) {
            return c;
        }
    }
}

fn read_letter(s: &[u8], ix: &mut usize) -> Option<&'static [DmCodes]> {
    let c = read_valid_char(s, ix);
    if c == 0 {
        return None;
    }
    let mut letter: &'static DmLetter = &LETTER_ROOT[(c - b'A') as usize];
    let mut codes = letter.codes;
    let mut i = *ix;

    loop {
        let subs = letter.letters;
        if subs.is_empty() {
            break;
        }
        let c = read_valid_char(s, &mut i);
        if c == 0 {
            break;
        }
        let Some(next) = subs.iter().find(|l| l.letter == c) else {
            break;
        };
        letter = next;
        if !next.codes.is_empty() {
            codes = next.codes;
            *ix = i;
        }
    }
    Some(codes)
}

struct DmState<'a, 'mcx> {
    arena: PgVec<'mcx, DmNode>,
    out: &'a mut PgVec<'mcx, [u8; DM_CODE_DIGITS]>,
}

impl DmState<'_, '_> {
    fn initialize_node(&mut self, node: u32, last_update: i32) {
        let n = &mut self.arena[node as usize];
        if n.last_update < last_update {
            n.prev_code_digits = n.next_code_digits;
            n.next_code_digits = [0, 0];
            n.prev_code_index = n.next_code_index;
            n.next_code_index = 0;
            n.is_leaf = false;
            n.last_update = last_update;
        }
    }

    fn add_next_code_digit(&mut self, node: u32, code_index: i32, code_digit: u8) {
        let n = &mut self.arena[node as usize];
        n.next_code_index |= code_index;
        if n.next_code_digits[0] == 0 {
            n.next_code_digits[0] = code_digit;
        } else if n.next_code_digits[0] != code_digit {
            n.next_code_digits[1] = code_digit;
        }
    }

    fn set_leaf(&mut self, first_node: &mut [u32; 2], last_node: &mut [u32; 2], node: u32, ix: usize) {
        if self.arena[node as usize].is_leaf {
            return;
        }
        self.arena[node as usize].is_leaf = true;
        if first_node[ix] == NODE_NONE {
            first_node[ix] = node;
        } else {
            self.arena[last_node[ix] as usize].next[ix] = node;
        }
        last_node[ix] = node;
        self.arena[node as usize].next[ix] = NODE_NONE;
    }

    fn find_or_create_child(&mut self, parent: u32, code_digit: u8) -> PgResult<Option<u32>> {
        let i = (code_digit - b'0') as usize;
        let existing = self.arena[parent as usize].children[i];
        if existing != NODE_NONE {
            // Skip completed nodes.
            return Ok(
                if self.arena[existing as usize].soundex_length < DM_CODE_DIGITS {
                    Some(existing)
                } else {
                    None
                },
            );
        }

        let mut node = start_node();
        let p = &self.arena[parent as usize];
        node.soundex = p.soundex;
        node.soundex_length = p.soundex_length;
        node.soundex[node.soundex_length] = code_digit;
        node.soundex_length += 1;
        node.next_code_index = node.prev_code_index;
        let complete = node.soundex_length >= DM_CODE_DIGITS;
        let soundex = node.soundex;

        let idx = self.arena.len() as u32;
        self.arena.push(node);
        self.arena[parent as usize].children[i] = idx;

        if complete {
            self.out.push(soundex);
            Ok(None)
        } else {
            Ok(Some(idx))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn update_node(
        &mut self,
        first_node: &mut [u32; 2],
        last_node: &mut [u32; 2],
        node: u32,
        ix_node: usize,
        letter_no: i32,
        prev_code_index: i32,
        next_code_index: i32,
        next_code_digits: &DmCode,
        digit_no: usize,
    ) -> PgResult<()> {
        let mut digit_no = digit_no;
        let next_code_digit = next_code_digits[digit_no];
        let mut dirty_nodes = [NODE_NONE; 2];
        let mut num_dirty = 0;

        self.initialize_node(node, letter_no);

        let n = &self.arena[node as usize];
        if n.prev_code_index != 0 && (n.prev_code_index & prev_code_index) == 0 {
            // Letter sound (vowel/consonant) doesn't match the previous
            // letter's coding index (only "J" can be both).
            return Ok(());
        }

        if next_code_digit == b'X'
            || (digit_no == 0
                && (n.prev_code_digits[0] == next_code_digit
                    || n.prev_code_digits[1] == next_code_digit))
        {
            dirty_nodes[num_dirty] = node;
            num_dirty += 1;
        }

        let n = &self.arena[node as usize];
        if next_code_digit != b'X'
            && (digit_no > 0
                || n.prev_code_digits[0] != next_code_digit
                || n.prev_code_digits[1] != 0)
        {
            if let Some(child) = self.find_or_create_child(node, next_code_digit)? {
                self.initialize_node(child, letter_no);
                dirty_nodes[num_dirty] = child;
                num_dirty += 1;
            }
        }

        for &dirty in &dirty_nodes[..num_dirty] {
            self.add_next_code_digit(dirty, next_code_index, next_code_digit);
            // C mutates digit_no across loop iterations (++digit_no) —
            // preserved bug-for-bug.
            digit_no += 1;
            if next_code_digits[digit_no] != 0 {
                self.update_node(
                    first_node,
                    last_node,
                    dirty,
                    ix_node,
                    letter_no,
                    prev_code_index,
                    next_code_index,
                    next_code_digits,
                    digit_no,
                )?;
            } else {
                self.set_leaf(first_node, last_node, dirty, ix_node);
            }
        }
        Ok(())
    }

    fn update_leaves(
        &mut self,
        first_node: &mut [u32; 2],
        ix_node: &mut usize,
        letter_no: i32,
        codes: &[DmCodes],
        next_codes: &[DmCodes],
    ) -> PgResult<()> {
        let ix_next = (*ix_node + 1) & 1;
        let mut last_node = [NODE_NONE; 2];
        first_node[ix_next] = NODE_NONE;

        let mut node = first_node[*ix_node];
        while node != NODE_NONE {
            for code in codes.iter().take(2) {
                if code[0][0] == 0 {
                    break;
                }
                // Coding for previous letter — before vowel: 1, all other: 2.
                let prev_code_index = (code[0][0] > b'1') as i32 + 1;

                for next_code in next_codes.iter().take(2) {
                    if next_code[0][0] == 0 {
                        break;
                    }
                    let code_index = if letter_no == 0 {
                        0
                    } else if next_code[0][0] <= b'1' {
                        1
                    } else {
                        2
                    };
                    self.update_node(
                        first_node,
                        &mut last_node,
                        node,
                        ix_next,
                        letter_no,
                        prev_code_index,
                        code_index as i32,
                        &code[code_index],
                        0,
                    )?;
                }
            }
            node = self.arena[node as usize].next[*ix_node];
        }

        *ix_node = ix_next;
        Ok(())
    }
}

pub(crate) fn daitch_mokotoff_coding<'mcx>(
    mcx: Mcx<'mcx>,
    word: &[u8],
    out: &mut PgVec<'mcx, [u8; DM_CODE_DIGITS]>,
) -> PgResult<bool> {
    let mut i = 0usize;
    let mut letter_no = 0i32;
    let mut ix_node = 0usize;
    let mut first_node = [NODE_NONE; 2];

    let Some(mut codes) = read_letter(word, &mut i) else {
        return Ok(false);
    };

    let mut state = DmState { arena: mcx::vec_with_capacity_in(mcx, 16)?, out };
    state.arena.push(start_node());
    first_node[ix_node] = 0;

    loop {
        if first_node[ix_node] == NODE_NONE {
            break;
        }
        let next_codes = read_letter(word, &mut i);
        state.update_leaves(
            &mut first_node,
            &mut ix_node,
            letter_no,
            codes,
            next_codes.unwrap_or(&END_CODES),
        )?;
        match next_codes {
            Some(nc) => codes = nc,
            None => break,
        }
        letter_no += 1;
    }

    let mut node = first_node[ix_node];
    while node != NODE_NONE {
        let soundex = state.arena[node as usize].soundex;
        state.out.push(soundex);
        node = state.arena[node as usize].next[ix_node];
    }
    Ok(true)
}

pub(crate) fn fc_daitch_mokotoff(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg is a non-null text varlena (strict fn).
    let src = unsafe { fcinfo.arg_varlena_packed(0)? };

    // Per-call temp context, as the C's tmp_ctx (nodes die at return).
    let scratch = MemoryContext::new("daitch_mokotoff temporary context");
    let smcx = scratch.mcx();

    let converted = mbutils::pg_server_to_any(smcx, src.data(), wchar::PG_UTF8)?;
    let word: &[u8] = match &converted {
        Some(v) => v,
        None => src.data(),
    };

    let mut codes: PgVec<'_, [u8; DM_CODE_DIGITS]> = mcx::vec_with_capacity_in(smcx, 8)?;
    if !daitch_mokotoff_coding(smcx, word, &mut codes)? {
        // No encodable characters in input.
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    let mcx = fcinfo.result_mcx();
    let mut elems: Vec<Datum> = Vec::with_capacity(codes.len());
    for code in codes.iter() {
        elems.push(varlena_result(varlena::cstring_to_text(mcx, code)?));
    }
    let image = arrayfuncs::construct::construct_array(
        mcx,
        &elems,
        types_core::TEXTOID,
        -1,
        false,
        b'i',
    )?;
    let d = Datum::from_usize(image.as_ptr() as usize);
    core::mem::forget(image);
    Ok(d)
}
