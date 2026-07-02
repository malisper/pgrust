// flex -CF tables from the generated scan.c (vendored; parsed by build.rs).
// yy_nxt is a relative state offset; a state is an index into YY_TRANSITION,
// its action lives at [state - 1].

#[repr(C)]
pub struct YyTransInfo {
    pub verify: i32,
    pub nxt: i32,
}

const fn t(verify: i32, nxt: i32) -> YyTransInfo {
    YyTransInfo { verify, nxt }
}

include!(concat!(env!("OUT_DIR"), "/dfa.rs"));

pub const YY_STATE_EOF_BASE: i32 = YY_END_OF_BUFFER + 1;
const _: () = assert!(YY_NUM_RULES + 1 == YY_END_OF_BUFFER);

impl<'mcx> crate::Scanner<'mcx> {
    #[inline]
    pub(crate) fn dfa_match(&self) -> (i32, usize) {
        let trans: &[YyTransInfo] = &YY_TRANSITION;
        let mut state = YY_START_STATE[1 + 2 * self.state as usize] as usize;
        let mut cp = self.pos;
        loop {
            let c = self.at(cp) as usize;
            // SAFETY: build.rs proves state-1 and state+0..=256 are in bounds
            // for every state reachable from any start condition.
            let ti = unsafe { trans.get_unchecked(state + c) };
            if ti.verify as usize != c {
                break;
            }
            state = (state as isize + ti.nxt as isize) as usize;
            cp += 1;
        }
        (unsafe { trans.get_unchecked(state - 1) }.nxt, cp)
    }

    // yy_get_previous_state + yy_try_NUL_trans: rescan the token, mapping NUL
    // (buffer sentinel or embedded) to flex class 256; the jam position gives
    // the true longest match. Zero-length at end of input selects the
    // <<EOF>> action for the active start condition.
    #[cold]
    pub(crate) fn handle_eob(&self) -> (i32, usize) {
        let trans: &[YyTransInfo] = &YY_TRANSITION;
        let len = self.scanbuf.len();
        let mut state = YY_START_STATE[1 + 2 * self.state as usize] as usize;
        let mut cp = self.tok_start;
        while cp < len {
            let c = self.scanbuf[cp];
            let class = if c == 0 { 256 } else { c as usize };
            let ti = &trans[state + class];
            if ti.verify as usize != class {
                break;
            }
            state = (state as isize + ti.nxt as isize) as usize;
            cp += 1;
        }
        if cp == self.tok_start {
            if cp >= len {
                return (YY_STATE_EOF_BASE + self.state as i32, cp);
            }
            panic!("scan_fgram: scanner jammed at byte offset {cp}");
        }
        (trans[state - 1].nxt, cp)
    }
}
