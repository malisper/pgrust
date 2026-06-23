//! Family: **regex-compile** — `regcomp.c` (with the `regc_lex.c` lexer): the
//! compile front-end.
//!
//! `pg_regcomp` is the orchestrator: it sets up the `struct vars` compile
//! context ([`Vars`]), runs the lexer + recursive-descent parser
//! (`parse`/`parsebranch`/`parseqatom`) to build the NFA + subexpression tree,
//! drives the colormap/NFA optimization passes (in [`crate::regex_nfa`] /
//! [`crate::regex_foundation`]), then compacts and stows the result in the
//! `RegexT::re_guts`.
//!
//! Faithful port of `src/backend/regex/regcomp.c` and `src/backend/regex/
//! regc_lex.c` (the latter is `#include`d by regcomp.c in C, so the two share
//! one translation unit and one `struct vars`).
//!
//! Allocating functions take `Mcx<'mcx>` and return `RegResult`. The colormap
//! engine ([`crate::regex_foundation`]), NFA machinery ([`crate::regex_nfa`]),
//! and locale probes ([`crate::regex_locale`]) are reached through the other
//! families in this same unit (direct calls into their landed bodies).

extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::{Oid, PgWChar};

use crate::regex_consts::*;
use crate::regex_error::{RegError, RegResult};
use crate::regex_foundation::{newcvec, subcolor, subcolorcvec, subcoloronechr};
use crate::regex_locale::{
    allcases, casecmp, cclasscvec, cmp, eclass, element, lookupcclass, pg_set_regex_collation,
    pg_wc_isalnum, pg_wc_isalpha, pg_wc_isdigit, pg_wc_isspace, range,
};
use crate::regex_nfa::{
    cloneouts, colorcomplement, compact, copyouts, cparc, delsub, dropstate, dupnfa, dupnfa_cross,
    freearc, freecnfa, freenfa, freestate, moveins, moveouts, newarc, newnfa, newstate, okcolors,
    optimize, rainbow, removeconstraints, single_color_transition, specialcolors,
};
use crate::regguts::{
    char_classes, chr, color, ColorMap, Cnfa, Cvec, Fns, Guts, Nfa, NodeId, RegexT, StateId, Subre,
    AHEAD, BACKR, BACKREF, BEHIND, BRUSE, CAP, CCLASS, CCLASSC, CCLASSS, COLLEL, COLMARK, COLORLESS,
    DIGIT, ECLASS, EMPTY, END, EOS, FREECOL, INUSE, LACON, LONGER, MATCHALL, MIXED, NWBDRY, PLAIN,
    PSEUDO, RAINBOW, RANGE, SBEGIN, SEND, SHORTER, WBDRY,
};

// =============================================================================
// lexical contexts  (regc_lex.c: L_* defines)
// =============================================================================

/// mainline ERE/ARE
pub const L_ERE: i32 = 1;
/// mainline BRE
pub const L_BRE: i32 = 2;
/// REG_QUOTE
pub const L_Q: i32 = 3;
/// ERE/ARE bound
pub const L_EBND: i32 = 4;
/// BRE bound
pub const L_BBND: i32 = 5;
/// brackets
pub const L_BRACK: i32 = 6;
/// collating element
pub const L_CEL: i32 = 7;
/// equivalence class
pub const L_ECL: i32 = 8;
/// character class
pub const L_CCL: i32 = 9;

// =============================================================================
// inline helpers replacing C macros from regcustom.h / regguts.h
// =============================================================================

/// `CHR(c)` -- turn a char literal into a chr literal (regcustom.h:
/// `((unsigned char) (c))`).
#[inline]
const fn CHR(c: u8) -> chr {
    c as chr
}

/// `DIGITVAL(c)` -- turn a chr ASCII digit into its value (regcustom.h:
/// `((c) - '0')`).
#[inline]
const fn DIGITVAL(c: chr) -> chr {
    c.wrapping_sub(b'0' as chr)
}

/// `iscalpha(x)` -- `pg_wc_isalpha(x)` (regcustom.h), as a bool.
#[inline]
fn iscalpha(x: chr) -> bool {
    pg_wc_isalpha(x)
}

/// `iscalnum(x)` -- `pg_wc_isalnum(x)` (regcustom.h), as a bool.
#[inline]
fn iscalnum(x: chr) -> bool {
    pg_wc_isalnum(x)
}

/// `iscdigit(x)` -- `pg_wc_isdigit(x)` (regcustom.h), as a bool.
#[inline]
fn iscdigit(x: chr) -> bool {
    pg_wc_isdigit(x)
}

/// `iscspace(x)` -- `pg_wc_isspace(x)` (regcustom.h), as a bool.
#[inline]
fn iscspace(x: chr) -> bool {
    pg_wc_isspace(x)
}

// =============================================================================
// Vars  (the C `struct vars` in regcomp.c)
// =============================================================================

/// Parser recursion-depth ceiling; see [`Vars::parse_depth`]. Mirrors C's
/// `STACK_TOO_DEEP -> REG_ETOOBIG`.
pub const MAX_PARSE_DEPTH: u32 = 10_000;

/// `struct vars` -- the regex compiler's working context (regcomp.c).
///
/// In C this is a single struct threaded as `struct vars *v` through every
/// compile-time function. Here it owns its working data:
///
/// * `pattern`/`cursor` replace the C `now`/`stop` raw scan pointers.
/// * `err` is the sticky first-error-wins latch (C `int err`).
/// * `info` accumulates the `NOTE(b)` bits (C `re->re_info`).
/// * `nfa`/`cm`/`cv`/`cv2` own the working NFA, colormap, and reusable cvecs.
/// * `subs` is the capturing-subre table (replaces C `subs`/`nsubs`/`sub10`).
/// * tree nodes live in `tree_nodes` (moved into `Guts` at the end).
pub struct Vars<'mcx> {
    /// allocation context (replaces C's ambient compile context)
    pub mcx: Mcx<'mcx>,
    /// the regex pattern, already mb->wchar'd to `chr` code points (C: `now`)
    pub pattern: Vec<chr>,
    /// cursor index into `pattern` (C: `next`/`stop` pointer arithmetic)
    pub cursor: usize,
    /// copy of compile flags (C: `cflags`). MUTATED during lexing.
    pub cflags: i32,
    /// accumulated `re_info` bits (C: `re->re_info`)
    pub info: i64,
    /// error code latch (None == 0 == no error). First error wins (C `int err`).
    pub err: Option<RegError>,
    /// type of previous token (C `lasttype`)
    pub lasttype: i32,
    /// type of next token (C `nexttype`)
    pub nexttype: i32,
    /// value (if any) of next token (C `nextvalue`)
    pub nextvalue: chr,
    /// lexical context type (one of the `L_*` constants) (C `lexcon`)
    pub lexcon: i32,
    /// number of capturing subexpressions seen (C: `nsubexp`)
    pub nsubexp: i32,
    /// subRE handle vector (replaces C `subs`/`nsubs`/`sub10`).
    pub subs: Vec<Option<NodeId>>,
    /// color of newline (C `nlcolor`)
    pub nlcolor: color,
    /// state in nfa holding word-char outarcs (C `v->wordchrs`).
    pub wordchrs: Option<StateId>,
    /// the working NFA (C: `struct nfa *nfa`)
    pub nfa: Nfa,
    /// the colormap (C: `struct colormap *cm`)
    pub cm: ColorMap,
    /// interface cvec (C `v->cv`), recycled by `getcvec`.
    pub cv: Option<Cvec>,
    /// utility cvec (C `v->cv2`).
    pub cv2: Option<Cvec>,
    /// subexpression-tree node arena (moved into `Guts` at the end).
    pub tree_nodes: Vec<Subre>,
    /// root of the subexpression tree
    pub tree: Option<NodeId>,
    /// number of tree nodes, plus one (C `ntree`)
    pub ntree: i32,
    /// lookaround-constraint vector (slots 1..nlacons-1 used)
    pub lacons: Vec<Subre>,
    /// size of lacons[] (C `nlacons`)
    pub nlacons: i32,
    /// transient complexity meter, in arbitrary units (C: `spaceused`)
    pub spaceused: usize,
    /// parser recursion depth (reproduces C's STACK_TOO_DEEP guard).
    pub parse_depth: u32,
}

// =============================================================================
// scanning / token / error helpers  (the regc_lex.c scanning macros)
// =============================================================================

impl<'mcx> Vars<'mcx> {
    /// `ATEOS()` -- `v->now >= v->stop`.
    #[inline]
    pub fn ATEOS(&self) -> bool {
        self.cursor >= self.pattern.len()
    }

    /// `HAVE(n)` -- `v->stop - v->now >= n`.
    #[inline]
    pub fn HAVE(&self, n: usize) -> bool {
        self.pattern.len() - self.cursor >= n
    }

    /// `NEXT1(c)` -- `!ATEOS() && *v->now == CHR(c)`.
    #[inline]
    pub fn NEXT1(&self, c: u8) -> bool {
        !self.ATEOS() && self.pattern[self.cursor] == CHR(c)
    }

    /// `NEXT2(a,b)` -- `HAVE(2) && *v->now == CHR(a) && *(v->now+1) == CHR(b)`.
    #[inline]
    pub fn NEXT2(&self, a: u8, b: u8) -> bool {
        self.HAVE(2) && self.pattern[self.cursor] == CHR(a) && self.pattern[self.cursor + 1] == CHR(b)
    }

    /// `NEXT3(a,b,c)` -- the 3-char lookahead.
    #[inline]
    pub fn NEXT3(&self, a: u8, b: u8, c: u8) -> bool {
        self.HAVE(3)
            && self.pattern[self.cursor] == CHR(a)
            && self.pattern[self.cursor + 1] == CHR(b)
            && self.pattern[self.cursor + 2] == CHR(c)
    }

    /// `*v->now` -- the chr under the cursor (caller must ensure `!ATEOS()`).
    #[inline]
    fn peek(&self) -> chr {
        self.pattern[self.cursor]
    }

    /// `*v->now` at offset (caller must ensure in range).
    #[inline]
    fn peek_at(&self, off: usize) -> chr {
        self.pattern[self.cursor + off]
    }

    /// `c = *v->now++` -- read the chr under the cursor and advance.
    #[inline]
    fn getchr(&mut self) -> chr {
        let c = self.pattern[self.cursor];
        self.cursor += 1;
        c
    }

    /// `SET(c)` -- `v->nexttype = c`.
    #[inline]
    fn SET(&mut self, c: i32) {
        self.nexttype = c;
    }

    /// `SETV(c, n)` -- `v->nexttype = c, v->nextvalue = n`.
    #[inline]
    fn SETV(&mut self, c: i32, n: chr) {
        self.nexttype = c;
        self.nextvalue = n;
    }

    /// `NOERR()` predicate -- `v->err == 0` (no error recorded yet).
    #[inline]
    pub fn NOERR(&self) -> bool {
        self.err.is_none()
    }

    /// `NISERR()` predicate -- `v->err != 0`.
    #[inline]
    pub fn NISERR(&self) -> bool {
        self.err.is_some()
    }

    /// `ISERR()` -- `v->err != 0` (alias of `NISERR`, used by parser-side code).
    #[inline]
    pub fn ISERR(&self) -> bool {
        self.err.is_some()
    }

    /// `ERR(e)` -- `v->nexttype = EOS; v->err = v->err ? v->err : e`.
    ///
    /// The sticky first-error-wins latch: records `e` only if no error is
    /// recorded yet, and always sets the pending token to EOS.
    #[inline]
    pub fn seterr(&mut self, e: i32) {
        self.nexttype = EOS;
        if self.err.is_none() {
            self.err = Some(RegError(e));
        }
    }

    /// `NOTE(b)` -- `v->re->re_info |= b`.
    #[inline]
    fn NOTE(&mut self, b: i32) {
        self.info |= b as i64;
    }

    /// `LASTTYPE(t)` -- `v->lasttype == t`.
    #[inline]
    fn LASTTYPE(&self, t: i32) -> bool {
        self.lasttype == t
    }

    /// `INTOCON(c)` -- `v->lexcon = c`.
    #[inline]
    fn INTOCON(&mut self, c: i32) {
        self.lexcon = c;
    }

    /// `INCON(con)` -- `v->lexcon == con`.
    #[inline]
    fn INCON(&self, con: i32) -> bool {
        self.lexcon == con
    }

    /// `SEE(t)` -- `v->nexttype == t` (is the next token this?).
    #[inline]
    pub fn SEE(&self, t: i32) -> bool {
        self.nexttype == t
    }

    /// `INSIST(c, e)` -- record error `e` (first-wins) if the condition `c` is false.
    #[inline]
    pub fn insist(&mut self, c: bool, e: i32) {
        if !c {
            self.seterr(e);
        }
    }

    /// `&t` -- borrow a tree node immutably.
    #[inline]
    fn t(&self, id: NodeId) -> &Subre {
        &self.tree_nodes[id.0 as usize]
    }

    /// `&mut t` -- borrow a tree node mutably.
    #[inline]
    fn tm(&mut self, id: NodeId) -> &mut Subre {
        &mut self.tree_nodes[id.0 as usize]
    }
}

// =============================================================================
// lexstart / prefixes  (regc_lex.c)
// =============================================================================

impl<'mcx> Vars<'mcx> {
    /// lexstart - set up lexical stuff, scan leading options.
    /// C: `static void lexstart(struct vars *v)`.
    pub fn lexstart(&mut self) {
        self.prefixes(); // may turn on new type bits etc.
        if self.NISERR() {
            return;
        }

        if (self.cflags & REG_QUOTE) != 0 {
            debug_assert!(self.cflags & (REG_ADVANCED | REG_EXPANDED | REG_NEWLINE) == 0);
            self.INTOCON(L_Q);
        } else if (self.cflags & REG_EXTENDED) != 0 {
            debug_assert!(self.cflags & REG_QUOTE == 0);
            self.INTOCON(L_ERE);
        } else {
            debug_assert!(self.cflags & (REG_QUOTE | REG_ADVF) == 0);
            self.INTOCON(L_BRE);
        }

        self.nexttype = EMPTY; // remember we were at the start
        self.next(); // set up the first token
    }

    /// prefixes - implement various special prefixes.
    /// C: `static void prefixes(struct vars *v)`.
    pub fn prefixes(&mut self) {
        // literal string doesn't get any of this stuff
        if (self.cflags & REG_QUOTE) != 0 {
            return;
        }

        // initial "***" gets special things
        if self.HAVE(4) && self.NEXT3(b'*', b'*', b'*') {
            match self.peek_at(3) {
                c if c == CHR(b'?') => {
                    // "***?" error, msg shows version
                    self.seterr(REG_BADPAT);
                    return; // proceed no further
                }
                c if c == CHR(b'=') => {
                    // "***=" shifts to literal string
                    self.NOTE(REG_UNONPOSIX);
                    self.cflags |= REG_QUOTE;
                    self.cflags &= !(REG_ADVANCED | REG_EXPANDED | REG_NEWLINE);
                    self.cursor += 4;
                    return; // and there can be no more prefixes
                }
                c if c == CHR(b':') => {
                    // "***:" shifts to AREs
                    self.NOTE(REG_UNONPOSIX);
                    self.cflags |= REG_ADVANCED;
                    self.cursor += 4;
                }
                _ => {
                    // otherwise *** is just an error
                    self.seterr(REG_BADRPT);
                    return;
                }
            }
        }

        // BREs and EREs don't get embedded options
        if (self.cflags & REG_ADVANCED) != REG_ADVANCED {
            return;
        }

        // embedded options (AREs only)
        if self.HAVE(3) && self.NEXT2(b'(', b'?') && iscalpha(self.peek_at(2)) {
            self.NOTE(REG_UNONPOSIX);
            self.cursor += 2;
            while !self.ATEOS() && iscalpha(self.peek()) {
                let c = self.peek();
                if c == CHR(b'b') {
                    self.cflags &= !(REG_ADVANCED | REG_QUOTE); // BREs
                } else if c == CHR(b'c') {
                    self.cflags &= !REG_ICASE; // case sensitive
                } else if c == CHR(b'e') {
                    self.cflags |= REG_EXTENDED; // plain EREs
                    self.cflags &= !(REG_ADVF | REG_QUOTE);
                } else if c == CHR(b'i') {
                    self.cflags |= REG_ICASE; // case insensitive
                } else if c == CHR(b'm') || c == CHR(b'n') {
                    self.cflags |= REG_NEWLINE; // \n affects ^ $ . [^
                } else if c == CHR(b'p') {
                    self.cflags |= REG_NLSTOP; // ~Perl
                    self.cflags &= !REG_NLANCH;
                } else if c == CHR(b'q') {
                    self.cflags |= REG_QUOTE; // literal string
                    self.cflags &= !REG_ADVANCED;
                } else if c == CHR(b's') {
                    self.cflags &= !REG_NEWLINE; // single line
                } else if c == CHR(b't') {
                    self.cflags &= !REG_EXPANDED; // tight syntax
                } else if c == CHR(b'w') {
                    self.cflags &= !REG_NLSTOP; // weird
                    self.cflags |= REG_NLANCH;
                } else if c == CHR(b'x') {
                    self.cflags |= REG_EXPANDED; // expanded syntax
                } else {
                    self.seterr(REG_BADOPT);
                    return;
                }
                self.cursor += 1;
            }
            if !self.NEXT1(b')') {
                self.seterr(REG_BADOPT);
                return;
            }
            self.cursor += 1;
            if (self.cflags & REG_QUOTE) != 0 {
                self.cflags &= !(REG_EXPANDED | REG_NEWLINE);
            }
        }
    }
}

// =============================================================================
// next - get next token  (regc_lex.c)
// =============================================================================

impl<'mcx> Vars<'mcx> {
    /// next - get next token. Returns 1 normal, 0 failure.
    /// C: `static int next(struct vars *v)`. The C `next_restart` goto (taken
    /// after eating a comment) becomes a `'next_restart` loop.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> i32 {
        'next_restart: loop {
            // errors yield an infinite sequence of failures
            if self.NISERR() {
                return 0; // the error has set nexttype to EOS
            }

            // remember flavor of last token
            self.lasttype = self.nexttype;

            // REG_BOSONLY
            if self.nexttype == EMPTY && (self.cflags & REG_BOSONLY) != 0 {
                self.SETV(SBEGIN, 0); // same as \A
                return 1;
            }

            // skip white space etc. if appropriate (not in literal or [])
            if (self.cflags & REG_EXPANDED) != 0 {
                match self.lexcon {
                    L_ERE | L_BRE | L_EBND | L_BBND => self.skip(),
                    _ => {}
                }
            }

            // handle EOS, depending on context
            if self.ATEOS() {
                match self.lexcon {
                    L_ERE | L_BRE | L_Q => {
                        self.SET(EOS);
                        return 1;
                    }
                    L_EBND | L_BBND => {
                        self.seterr(REG_EBRACE);
                        return 0;
                    }
                    L_BRACK | L_CEL | L_ECL | L_CCL => {
                        self.seterr(REG_EBRACK);
                        return 0;
                    }
                    _ => {
                        debug_assert!(false, "NOTREACHED");
                        return 0;
                    }
                }
            }

            // okay, time to actually get a character
            let mut c = self.getchr();

            // deal with the easy contexts, punt EREs to code below
            match self.lexcon {
                L_BRE => return self.brenext(c),
                L_ERE => { /* see below */ }
                L_Q => {
                    self.SETV(PLAIN, c);
                    return 1;
                }
                L_BBND | L_EBND => {
                    if (b'0' as chr..=b'9' as chr).contains(&c) {
                        self.SETV(DIGIT, DIGITVAL(c));
                        return 1;
                    } else if c == CHR(b',') {
                        self.SET(b',' as i32);
                        return 1;
                    } else if c == CHR(b'}') {
                        // ERE bound ends with }
                        if self.INCON(L_EBND) {
                            self.INTOCON(L_ERE);
                            if (self.cflags & REG_ADVF) != 0 && self.NEXT1(b'?') {
                                self.cursor += 1;
                                self.NOTE(REG_UNONPOSIX);
                                self.SETV(b'}' as i32, 0);
                                return 1;
                            }
                            self.SETV(b'}' as i32, 1);
                            return 1;
                        } else {
                            self.seterr(REG_BADBR);
                            return 0;
                        }
                    } else if c == CHR(b'\\') {
                        // BRE bound ends with \}
                        if self.INCON(L_BBND) && self.NEXT1(b'}') {
                            self.cursor += 1;
                            self.INTOCON(L_BRE);
                            self.SETV(b'}' as i32, 1);
                            return 1;
                        } else {
                            self.seterr(REG_BADBR);
                            return 0;
                        }
                    } else {
                        self.seterr(REG_BADBR);
                        return 0;
                    }
                }
                L_BRACK => {
                    if c == CHR(b']') {
                        if self.LASTTYPE(b'[' as i32) {
                            self.SETV(PLAIN, c);
                            return 1;
                        } else {
                            self.INTOCON(if (self.cflags & REG_EXTENDED) != 0 {
                                L_ERE
                            } else {
                                L_BRE
                            });
                            self.SET(b']' as i32);
                            return 1;
                        }
                    } else if c == CHR(b'\\') {
                        self.NOTE(REG_UBBS);
                        if (self.cflags & REG_ADVF) == 0 {
                            self.SETV(PLAIN, c);
                            return 1;
                        }
                        self.NOTE(REG_UNONPOSIX);
                        if self.ATEOS() {
                            self.seterr(REG_EESCAPE);
                            return 0;
                        }
                        if self.lexescape() == 0 {
                            return 0;
                        }
                        // not all escapes okay here
                        if self.nexttype == PLAIN
                            || self.nexttype == CCLASSS
                            || self.nexttype == CCLASSC
                        {
                            return 1;
                        }
                        self.seterr(REG_EESCAPE);
                        return 0;
                    } else if c == CHR(b'-') {
                        if self.LASTTYPE(b'[' as i32) || self.NEXT1(b']') {
                            self.SETV(PLAIN, c);
                            return 1;
                        } else {
                            self.SETV(RANGE, c);
                            return 1;
                        }
                    } else if c == CHR(b'[') {
                        if self.ATEOS() {
                            self.seterr(REG_EBRACK);
                            return 0;
                        }
                        let cc = self.getchr();
                        if cc == CHR(b'.') {
                            self.INTOCON(L_CEL);
                            self.SET(COLLEL);
                            return 1;
                        } else if cc == CHR(b'=') {
                            self.INTOCON(L_ECL);
                            self.NOTE(REG_ULOCALE);
                            self.SET(ECLASS);
                            return 1;
                        } else if cc == CHR(b':') {
                            self.INTOCON(L_CCL);
                            self.NOTE(REG_ULOCALE);
                            self.SET(CCLASS);
                            return 1;
                        } else {
                            self.cursor -= 1;
                            self.SETV(PLAIN, c);
                            return 1;
                        }
                    } else {
                        self.SETV(PLAIN, c);
                        return 1;
                    }
                }
                L_CEL => {
                    if c == CHR(b'.') && self.NEXT1(b']') {
                        self.cursor += 1;
                        self.INTOCON(L_BRACK);
                        self.SETV(END, b'.' as chr);
                        return 1;
                    } else {
                        self.SETV(PLAIN, c);
                        return 1;
                    }
                }
                L_ECL => {
                    if c == CHR(b'=') && self.NEXT1(b']') {
                        self.cursor += 1;
                        self.INTOCON(L_BRACK);
                        self.SETV(END, b'=' as chr);
                        return 1;
                    } else {
                        self.SETV(PLAIN, c);
                        return 1;
                    }
                }
                L_CCL => {
                    if c == CHR(b':') && self.NEXT1(b']') {
                        self.cursor += 1;
                        self.INTOCON(L_BRACK);
                        self.SETV(END, b':' as chr);
                        return 1;
                    } else {
                        self.SETV(PLAIN, c);
                        return 1;
                    }
                }
                _ => {
                    debug_assert!(false, "NOTREACHED");
                    return 0;
                }
            }

            // that got rid of everything except EREs and AREs
            debug_assert!(self.INCON(L_ERE));

            // deal with EREs and AREs, except for backslashes
            if c == CHR(b'|') {
                self.SET(b'|' as i32);
                return 1;
            } else if c == CHR(b'*') {
                if (self.cflags & REG_ADVF) != 0 && self.NEXT1(b'?') {
                    self.cursor += 1;
                    self.NOTE(REG_UNONPOSIX);
                    self.SETV(b'*' as i32, 0);
                    return 1;
                }
                self.SETV(b'*' as i32, 1);
                return 1;
            } else if c == CHR(b'+') {
                if (self.cflags & REG_ADVF) != 0 && self.NEXT1(b'?') {
                    self.cursor += 1;
                    self.NOTE(REG_UNONPOSIX);
                    self.SETV(b'+' as i32, 0);
                    return 1;
                }
                self.SETV(b'+' as i32, 1);
                return 1;
            } else if c == CHR(b'?') {
                if (self.cflags & REG_ADVF) != 0 && self.NEXT1(b'?') {
                    self.cursor += 1;
                    self.NOTE(REG_UNONPOSIX);
                    self.SETV(b'?' as i32, 0);
                    return 1;
                }
                self.SETV(b'?' as i32, 1);
                return 1;
            } else if c == CHR(b'{') {
                // bounds start or plain character
                if (self.cflags & REG_EXPANDED) != 0 {
                    self.skip();
                }
                if self.ATEOS() || !iscdigit(self.peek()) {
                    self.NOTE(REG_UBRACES);
                    self.NOTE(REG_UUNSPEC);
                    self.SETV(PLAIN, c);
                    return 1;
                } else {
                    self.NOTE(REG_UBOUNDS);
                    self.INTOCON(L_EBND);
                    self.SET(b'{' as i32);
                    return 1;
                }
            } else if c == CHR(b'(') {
                // parenthesis, or advanced extension
                if (self.cflags & REG_ADVF) != 0 && self.NEXT1(b'?') {
                    self.NOTE(REG_UNONPOSIX);
                    self.cursor += 1;
                    if self.ATEOS() {
                        self.seterr(REG_BADRPT);
                        return 0;
                    }
                    let cc = self.getchr();
                    if cc == CHR(b':') {
                        self.SETV(b'(' as i32, 0); // non-capturing paren
                        return 1;
                    } else if cc == CHR(b'#') {
                        // comment
                        while !self.ATEOS() && self.peek() != CHR(b')') {
                            self.cursor += 1;
                        }
                        if !self.ATEOS() {
                            self.cursor += 1;
                        }
                        debug_assert!(self.nexttype == self.lasttype);
                        continue 'next_restart;
                    } else if cc == CHR(b'=') {
                        self.NOTE(REG_ULOOKAROUND); // positive lookahead
                        self.SETV(LACON, LATYPE_AHEAD_POS as chr);
                        return 1;
                    } else if cc == CHR(b'!') {
                        self.NOTE(REG_ULOOKAROUND); // negative lookahead
                        self.SETV(LACON, LATYPE_AHEAD_NEG as chr);
                        return 1;
                    } else if cc == CHR(b'<') {
                        if self.ATEOS() {
                            self.seterr(REG_BADRPT);
                            return 0;
                        }
                        let ccc = self.getchr();
                        if ccc == CHR(b'=') {
                            self.NOTE(REG_ULOOKAROUND); // positive lookbehind
                            self.SETV(LACON, LATYPE_BEHIND_POS as chr);
                            return 1;
                        } else if ccc == CHR(b'!') {
                            self.NOTE(REG_ULOOKAROUND); // negative lookbehind
                            self.SETV(LACON, LATYPE_BEHIND_NEG as chr);
                            return 1;
                        } else {
                            self.seterr(REG_BADRPT);
                            return 0;
                        }
                    } else {
                        self.seterr(REG_BADRPT);
                        return 0;
                    }
                }
                self.SETV(b'(' as i32, 1);
                return 1;
            } else if c == CHR(b')') {
                if self.LASTTYPE(b'(' as i32) {
                    self.NOTE(REG_UUNSPEC);
                }
                self.SETV(b')' as i32, c);
                return 1;
            } else if c == CHR(b'[') {
                // easy except for [[:<:]] and [[:>:]]
                if self.HAVE(6)
                    && self.peek_at(0) == CHR(b'[')
                    && self.peek_at(1) == CHR(b':')
                    && (self.peek_at(2) == CHR(b'<') || self.peek_at(2) == CHR(b'>'))
                    && self.peek_at(3) == CHR(b':')
                    && self.peek_at(4) == CHR(b']')
                    && self.peek_at(5) == CHR(b']')
                {
                    c = self.peek_at(2);
                    self.cursor += 6;
                    self.NOTE(REG_UNONPOSIX);
                    self.SET(if c == CHR(b'<') {
                        b'<' as i32
                    } else {
                        b'>' as i32
                    });
                    return 1;
                }
                self.INTOCON(L_BRACK);
                if self.NEXT1(b'^') {
                    self.cursor += 1;
                    self.SETV(b'[' as i32, 0);
                    return 1;
                }
                self.SETV(b'[' as i32, 1);
                return 1;
            } else if c == CHR(b'.') {
                self.SET(b'.' as i32);
                return 1;
            } else if c == CHR(b'^') {
                self.SET(b'^' as i32);
                return 1;
            } else if c == CHR(b'$') {
                self.SET(b'$' as i32);
                return 1;
            } else if c == CHR(b'\\') {
                if self.ATEOS() {
                    self.seterr(REG_EESCAPE);
                    return 0;
                }
                // fall through to backslash handling below
            } else {
                self.SETV(PLAIN, c);
                return 1;
            }

            // ERE/ARE backslash handling; backslash already eaten
            debug_assert!(!self.ATEOS());
            if (self.cflags & REG_ADVF) == 0 {
                // only AREs have non-trivial escapes
                if iscalnum(self.peek()) {
                    self.NOTE(REG_UBSALNUM);
                    self.NOTE(REG_UUNSPEC);
                }
                let nv = self.getchr();
                self.SETV(PLAIN, nv);
                return 1;
            }
            return self.lexescape();
        }
    }
}

// =============================================================================
// lexescape / lexdigits / brenext / skip / chrnamed  (regc_lex.c)
// =============================================================================

impl<'mcx> Vars<'mcx> {
    /// lexescape - parse an ARE backslash escape (backslash already eaten).
    /// C: `static int lexescape(struct vars *v)`.
    pub fn lexescape(&mut self) -> i32 {
        const ALERT: &[chr] = &[CHR(b'a'), CHR(b'l'), CHR(b'e'), CHR(b'r'), CHR(b't')];
        const ESC: &[chr] = &[CHR(b'E'), CHR(b'S'), CHR(b'C')];

        debug_assert!(self.cflags & REG_ADVF != 0);
        debug_assert!(!self.ATEOS());

        let mut c = self.getchr();

        // if it's not alphanumeric ASCII, treat it as a plain character
        if !((b'a' as chr <= c && c <= b'z' as chr)
            || (b'A' as chr <= c && c <= b'Z' as chr)
            || (b'0' as chr <= c && c <= b'9' as chr))
        {
            self.SETV(PLAIN, c);
            return 1;
        }

        self.NOTE(REG_UNONPOSIX);

        if c == CHR(b'a') {
            let v = self.chrnamed(ALERT, CHR(0x07));
            self.SETV(PLAIN, v);
            1
        } else if c == CHR(b'A') {
            self.SETV(SBEGIN, 0);
            1
        } else if c == CHR(b'b') {
            self.SETV(PLAIN, CHR(0x08)); // '\b'
            1
        } else if c == CHR(b'B') {
            self.SETV(PLAIN, CHR(b'\\'));
            1
        } else if c == CHR(b'c') {
            self.NOTE(REG_UUNPORT);
            if self.ATEOS() {
                self.seterr(REG_EESCAPE);
                return 0;
            }
            let nv = self.getchr();
            self.SETV(PLAIN, nv & 0o37);
            1
        } else if c == CHR(b'd') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSS, char_classes::CC_DIGIT as chr);
            1
        } else if c == CHR(b'D') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSC, char_classes::CC_DIGIT as chr);
            1
        } else if c == CHR(b'e') {
            self.NOTE(REG_UUNPORT);
            let v = self.chrnamed(ESC, CHR(0o33));
            self.SETV(PLAIN, v);
            1
        } else if c == CHR(b'f') {
            self.SETV(PLAIN, CHR(0x0c)); // '\f'
            1
        } else if c == CHR(b'm') {
            self.SET(b'<' as i32);
            1
        } else if c == CHR(b'M') {
            self.SET(b'>' as i32);
            1
        } else if c == CHR(b'n') {
            self.SETV(PLAIN, CHR(b'\n'));
            1
        } else if c == CHR(b'r') {
            self.SETV(PLAIN, CHR(b'\r'));
            1
        } else if c == CHR(b's') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSS, char_classes::CC_SPACE as chr);
            1
        } else if c == CHR(b'S') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSC, char_classes::CC_SPACE as chr);
            1
        } else if c == CHR(b't') {
            self.SETV(PLAIN, CHR(b'\t'));
            1
        } else if c == CHR(b'u') {
            c = self.lexdigits(16, 4, 4);
            if self.NISERR() || !crate::regguts::CHR_IS_IN_RANGE(c) {
                self.seterr(REG_EESCAPE);
                return 0;
            }
            self.SETV(PLAIN, c);
            1
        } else if c == CHR(b'U') {
            c = self.lexdigits(16, 8, 8);
            if self.NISERR() || !crate::regguts::CHR_IS_IN_RANGE(c) {
                self.seterr(REG_EESCAPE);
                return 0;
            }
            self.SETV(PLAIN, c);
            1
        } else if c == CHR(b'v') {
            self.SETV(PLAIN, CHR(0x0b)); // '\v'
            1
        } else if c == CHR(b'w') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSS, char_classes::CC_WORD as chr);
            1
        } else if c == CHR(b'W') {
            self.NOTE(REG_ULOCALE);
            self.SETV(CCLASSC, char_classes::CC_WORD as chr);
            1
        } else if c == CHR(b'x') {
            self.NOTE(REG_UUNPORT);
            c = self.lexdigits(16, 1, 255); // REs >255 long outside spec
            if self.NISERR() || !crate::regguts::CHR_IS_IN_RANGE(c) {
                self.seterr(REG_EESCAPE);
                return 0;
            }
            self.SETV(PLAIN, c);
            1
        } else if c == CHR(b'y') {
            self.NOTE(REG_ULOCALE);
            self.SETV(WBDRY, 0);
            1
        } else if c == CHR(b'Y') {
            self.NOTE(REG_ULOCALE);
            self.SETV(NWBDRY, 0);
            1
        } else if c == CHR(b'Z') {
            self.SETV(SEND, 0);
            1
        } else if (b'1' as chr..=b'9' as chr).contains(&c) {
            let save = self.cursor;
            self.cursor -= 1; // put first digit back
            c = self.lexdigits(10, 1, 255); // REs >255 long outside spec
            if self.NISERR() {
                self.seterr(REG_EESCAPE);
                return 0;
            }
            // ugly heuristic (first test is "exactly 1 digit?")
            if self.cursor == save || ((c as i32) > 0 && (c as i32) <= self.nsubexp) {
                self.NOTE(REG_UBACKREF);
                self.SETV(BACKREF, c);
                return 1;
            }
            // oops, doesn't look like it's a backref after all...
            self.cursor = save;
            // and fall through into octal number (the '0' arm)
            self.lexescape_octal()
        } else if c == CHR(b'0') {
            self.lexescape_octal()
        } else {
            // Throw an error for unrecognized ASCII alpha escape sequences.
            self.seterr(REG_EESCAPE);
            0
        }
    }

    /// The `case CHR('0'):` octal arm of [`lexescape`], split out so the `\1`-`\9`
    /// FALLTHROUGH can reach it (the C `/* FALLTHROUGH */`).
    fn lexescape_octal(&mut self) -> i32 {
        self.NOTE(REG_UUNPORT);
        self.cursor -= 1; // put first digit back
        let mut c = self.lexdigits(8, 1, 3);
        if self.NISERR() {
            self.seterr(REG_EESCAPE);
            return 0;
        }
        if c > 0xff {
            // out of range, so we handled one digit too much
            self.cursor -= 1;
            c >>= 3;
        }
        self.SETV(PLAIN, c);
        1
    }

    /// lexdigits - slurp up digits and return chr value.
    /// C: `static chr lexdigits(struct vars *v, int base, int minlen, int maxlen)`.
    pub fn lexdigits(&mut self, base: i32, minlen: i32, maxlen: i32) -> chr {
        let mut n: u32 = 0; // unsigned to avoid overflow misbehavior
        let ub: u32 = base as u32;

        let mut len = 0;
        while len < maxlen && !self.ATEOS() {
            let c = self.getchr();
            let mut d: i32 = if (b'0' as chr..=b'9' as chr).contains(&c) {
                DIGITVAL(c) as i32
            } else if c == CHR(b'a') || c == CHR(b'A') {
                10
            } else if c == CHR(b'b') || c == CHR(b'B') {
                11
            } else if c == CHR(b'c') || c == CHR(b'C') {
                12
            } else if c == CHR(b'd') || c == CHR(b'D') {
                13
            } else if c == CHR(b'e') || c == CHR(b'E') {
                14
            } else if c == CHR(b'f') || c == CHR(b'F') {
                15
            } else {
                self.cursor -= 1; // oops, not a digit at all
                -1
            };

            if d >= base {
                // not a plausible digit
                self.cursor -= 1;
                d = -1;
            }
            if d < 0 {
                break; // NOTE BREAK OUT
            }
            n = n.wrapping_mul(ub).wrapping_add(d as u32);
            len += 1;
        }
        if len < minlen {
            self.seterr(REG_EESCAPE);
        }

        n as chr
    }

    /// brenext - get next BRE token. Returns 1 normal, 0 failure.
    /// C: `static int brenext(struct vars *v, chr c)`.
    pub fn brenext(&mut self, mut c: chr) -> i32 {
        if c == CHR(b'*') {
            if self.LASTTYPE(EMPTY) || self.LASTTYPE(b'(' as i32) || self.LASTTYPE(b'^' as i32) {
                self.SETV(PLAIN, c);
                return 1;
            }
            self.SETV(b'*' as i32, 1);
            return 1;
        } else if c == CHR(b'[') {
            if self.HAVE(6)
                && self.peek_at(0) == CHR(b'[')
                && self.peek_at(1) == CHR(b':')
                && (self.peek_at(2) == CHR(b'<') || self.peek_at(2) == CHR(b'>'))
                && self.peek_at(3) == CHR(b':')
                && self.peek_at(4) == CHR(b']')
                && self.peek_at(5) == CHR(b']')
            {
                c = self.peek_at(2);
                self.cursor += 6;
                self.NOTE(REG_UNONPOSIX);
                self.SET(if c == CHR(b'<') {
                    b'<' as i32
                } else {
                    b'>' as i32
                });
                return 1;
            }
            self.INTOCON(L_BRACK);
            if self.NEXT1(b'^') {
                self.cursor += 1;
                self.SETV(b'[' as i32, 0);
                return 1;
            }
            self.SETV(b'[' as i32, 1);
            return 1;
        } else if c == CHR(b'.') {
            self.SET(b'.' as i32);
            return 1;
        } else if c == CHR(b'^') {
            if self.LASTTYPE(EMPTY) {
                self.SET(b'^' as i32);
                return 1;
            }
            if self.LASTTYPE(b'(' as i32) {
                self.NOTE(REG_UUNSPEC);
                self.SET(b'^' as i32);
                return 1;
            }
            self.SETV(PLAIN, c);
            return 1;
        } else if c == CHR(b'$') {
            if (self.cflags & REG_EXPANDED) != 0 {
                self.skip();
            }
            if self.ATEOS() {
                self.SET(b'$' as i32);
                return 1;
            }
            if self.NEXT2(b'\\', b')') {
                self.NOTE(REG_UUNSPEC);
                self.SET(b'$' as i32);
                return 1;
            }
            self.SETV(PLAIN, c);
            return 1;
        } else if c == CHR(b'\\') {
            // see below
        } else {
            self.SETV(PLAIN, c);
            return 1;
        }

        debug_assert!(c == CHR(b'\\'));

        if self.ATEOS() {
            self.seterr(REG_EESCAPE);
            return 0;
        }

        c = self.getchr();
        if c == CHR(b'{') {
            self.INTOCON(L_BBND);
            self.NOTE(REG_UBOUNDS);
            self.SET(b'{' as i32);
            1
        } else if c == CHR(b'(') {
            self.SETV(b'(' as i32, 1);
            1
        } else if c == CHR(b')') {
            self.SETV(b')' as i32, c);
            1
        } else if c == CHR(b'<') {
            self.NOTE(REG_UNONPOSIX);
            self.SET(b'<' as i32);
            1
        } else if c == CHR(b'>') {
            self.NOTE(REG_UNONPOSIX);
            self.SET(b'>' as i32);
            1
        } else if (b'1' as chr..=b'9' as chr).contains(&c) {
            self.NOTE(REG_UBACKREF);
            self.SETV(BACKREF, DIGITVAL(c));
            1
        } else {
            if iscalnum(c) {
                self.NOTE(REG_UBSALNUM);
                self.NOTE(REG_UUNSPEC);
            }
            self.SETV(PLAIN, c);
            1
        }
    }

    /// skip - skip white space and comments in expanded form.
    /// C: `static void skip(struct vars *v)`.
    pub fn skip(&mut self) {
        let start = self.cursor;

        debug_assert!(self.cflags & REG_EXPANDED != 0);

        loop {
            while !self.ATEOS() && iscspace(self.peek()) {
                self.cursor += 1;
            }
            if self.ATEOS() || self.peek() != CHR(b'#') {
                break; // NOTE BREAK OUT
            }
            debug_assert!(self.NEXT1(b'#'));
            while !self.ATEOS() && self.peek() != CHR(b'\n') {
                self.cursor += 1;
            }
            // leave the newline to be picked up by the iscspace loop
        }

        if self.cursor != start {
            self.NOTE(REG_UNONPOSIX);
        }
    }

    /// chrnamed - return the chr known by a given (chr string) name.
    /// C: `static chr chrnamed(struct vars *v, const chr *startp, const chr *endp,
    /// chr lastresort)`.
    ///
    /// Calls the locale `element`/`range` functions; the err save/restore-and-
    /// swallow dance mirrors C: `element` may set an error, but chrnamed saves
    /// the prior err, clears it, calls element, then restores and reports via
    /// `lastresort`.
    pub fn chrnamed(&mut self, name: &[chr], lastresort: chr) -> chr {
        // errsave = v->err; v->err = 0;
        let errsave = self.err;
        self.err = None;
        // c = element(v, startp, endp);
        let elem = element(name);
        // e = v->err; v->err = errsave;  (element returns Err directly here)
        let e_is_err = elem.is_err();
        self.err = errsave;

        if e_is_err {
            return lastresort;
        }
        // C uses the resolved chr; the REG_ULOCALE note is intentionally not
        // threaded here (chrnamed saves/restores v->err around element and only
        // cares about the resolved code point).
        let c = elem.unwrap().code;

        // cv = range(v, c, c, 0);
        match range(self.mcx, c, c, 0) {
            Ok(cvec) => {
                if cvec.chrs.is_empty() {
                    lastresort
                } else {
                    cvec.chrs[0]
                }
            }
            Err(_) => lastresort,
        }
    }
}

// =============================================================================
// newline / scannum  (regcomp.c)
// =============================================================================

/// newline - return the chr for a newline. C: `static chr newline(void)`.
pub fn newline() -> chr {
    CHR(b'\n')
}

/// scannum - scan a number (value, <= DUPMAX). C: `static int scannum(struct vars *v)`.
pub fn scannum(v: &mut Vars) -> i32 {
    let mut n: i32 = 0;

    while v.SEE(DIGIT) && n < DUPMAX {
        n = n * 10 + v.nextvalue as i32;
        v.next();
    }
    if v.SEE(DIGIT) || n > DUPMAX {
        v.seterr(REG_BADBR);
        return 0;
    }
    n
}

// =============================================================================
// DepthGuard - parser recursion-depth guard (regcomp.c STACK_TOO_DEEP)
// =============================================================================

/// RAII guard that bumps [`Vars::parse_depth`] on entry to a recursive compile
/// routine and restores it on drop, making C's relocated `STACK_TOO_DEEP(v->re)`
/// guard (checked in [`subre`]) functional.
pub struct DepthGuard<'g, 'mcx> {
    v: &'g mut Vars<'mcx>,
}

impl<'g, 'mcx> DepthGuard<'g, 'mcx> {
    /// Enter one recursive level. Returns `Err(REG_ETOOBIG)` (after latching it)
    /// if the ceiling is exceeded, mirroring C's STACK_TOO_DEEP check.
    pub fn enter(v: &'g mut Vars<'mcx>) -> RegResult<Self> {
        if v.parse_depth >= MAX_PARSE_DEPTH {
            v.seterr(REG_ETOOBIG);
            return Err(RegError(REG_ETOOBIG));
        }
        v.parse_depth += 1;
        Ok(DepthGuard { v })
    }
}

impl<'g, 'mcx> core::ops::Deref for DepthGuard<'g, 'mcx> {
    type Target = Vars<'mcx>;
    fn deref(&self) -> &Self::Target {
        self.v
    }
}

impl<'g, 'mcx> core::ops::DerefMut for DepthGuard<'g, 'mcx> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.v
    }
}

impl<'g, 'mcx> Drop for DepthGuard<'g, 'mcx> {
    fn drop(&mut self) {
        self.v.parse_depth -= 1;
    }
}

// =============================================================================
// subre flag-propagation macros (regguts.h)
// =============================================================================

/// `UPPROP` -- flags which should propagate up (regguts.h).
const UPPROP: u8 = MIXED | CAP | BACKR;

/// `LMIX(f)` -- LONGER -> MIXED (regguts.h). Computed in a wider type so the
/// shift cannot overflow-panic and the `& MIXED` masking matches C byte-for-byte.
#[inline]
const fn LMIX(f: u8) -> u32 {
    (f as u32) << 2
}

/// `SMIX(f)` -- SHORTER -> MIXED (regguts.h).
#[inline]
const fn SMIX(f: u8) -> u32 {
    (f as u32) << 1
}

/// `UP(f)` -- flags propagated up from a child (regguts.h).
#[inline]
const fn UP(f: u8) -> u8 {
    (f & UPPROP) | ((LMIX(f) & SMIX(f) & MIXED as u32) as u8)
}

/// `MESSY(f)` -- is this node "messy" (has MIXED/CAP/BACKR)? (regguts.h)
#[inline]
const fn MESSY(f: u8) -> bool {
    (f & (MIXED | CAP | BACKR)) != 0
}

/// `PREF(f)` -- greediness preference bits (regguts.h).
#[inline]
const fn PREF(f: u8) -> u8 {
    f & (LONGER | SHORTER)
}

/// `PREF2(f1, f2)` -- f1's preference if any, else f2's (regguts.h).
#[inline]
const fn PREF2(f1: u8, f2: u8) -> u8 {
    if PREF(f1) != 0 {
        PREF(f1)
    } else {
        PREF(f2)
    }
}

/// `COMBINE(f1, f2)` -- combine two flag words (regguts.h).
#[inline]
const fn COMBINE(f1: u8, f2: u8) -> u8 {
    UP(f1 | f2) | PREF2(f1, f2)
}

// =============================================================================
// subre tree arena: subre / freesrnode / freesubre / freesubreandsiblings
// =============================================================================

/// A zeroed [`Subre`] node, used to initialize reserved/lacon slots.
fn blank_subre() -> Subre {
    Subre {
        op: 0,
        flags: 0,
        latype: 0,
        id: 0,
        capno: 0,
        backno: 0,
        min: 1,
        max: 1,
        child: None,
        sibling: None,
        begin: None,
        end: None,
        cnfa: None,
        chain: None,
    }
}

/// subre - allocate a subre. C: `static struct subre *subre(struct vars *v, int
/// op, int flags, struct state *begin, struct state *end)`.
///
/// The C treechain/treefree freelist is subsumed by the `tree_nodes` arena;
/// allocation always appends a fresh slot. The `STACK_TOO_DEEP(v->re)` recursion
/// guard is relocated here: it fails with `REG_ETOOBIG` once `parse_depth`
/// reaches [`MAX_PARSE_DEPTH`].
pub fn subre(
    v: &mut Vars,
    op: u8,
    flags: u8,
    begin: Option<StateId>,
    end: Option<StateId>,
) -> Option<NodeId> {
    if v.parse_depth >= MAX_PARSE_DEPTH {
        v.seterr(REG_ETOOBIG);
        return None;
    }

    debug_assert!(matches!(op, b'=' | b'b' | b'|' | b'.' | b'*' | b'('));

    let id = NodeId(v.tree_nodes.len() as u32);
    let node = Subre {
        op,
        flags,
        latype: 0xff, // C: (char) -1
        id: 0,        // will be assigned later
        capno: 0,
        backno: 0,
        min: 1,
        max: 1,
        child: None,
        sibling: None,
        begin,
        end,
        cnfa: None, // ZAPCNFA(ret->cnfa)
        chain: None,
    };
    v.tree_nodes.push(node);
    Some(id)
}

/// freesrnode - free one node in a subRE subtree. With the arena model "freeing"
/// resets fields (clearing INUSE) and drops any attached cnfa.
pub fn freesrnode(v: &mut Vars, sr: Option<NodeId>) {
    let sr = match sr {
        Some(s) => s,
        None => return,
    };

    if v.tree_nodes[sr.0 as usize].cnfa.is_some() {
        let mut cnfa = v.tree_nodes[sr.0 as usize].cnfa.take().unwrap();
        freecnfa(&mut cnfa);
    }
    let node = &mut v.tree_nodes[sr.0 as usize];
    node.flags = 0; // in particular, not INUSE
    node.child = None;
    node.sibling = None;
    node.begin = None;
    node.end = None;
}

/// freesubre - free a subRE subtree (children too, but not siblings).
pub fn freesubre(v: &mut Vars, sr: Option<NodeId>) {
    let sr = match sr {
        Some(s) => s,
        None => return,
    };

    let child = v.tree_nodes[sr.0 as usize].child;
    if child.is_some() {
        freesubreandsiblings(v, child);
    }

    freesrnode(v, Some(sr));
}

/// freesubreandsiblings - free a subRE subtree (children and following siblings).
pub fn freesubreandsiblings(v: &mut Vars, mut sr: Option<NodeId>) {
    while let Some(node) = sr {
        let next = v.tree_nodes[node.0 as usize].sibling;
        freesubre(v, Some(node));
        sr = next;
    }
}

/// moresubs - enlarge subRE vector. C: `static void moresubs(struct vars *v, int
/// wanted)`. Grows `v->subs` to length `wanted*3/2 + 1`, padding with None.
pub fn moresubs(v: &mut Vars, wanted: i32) {
    debug_assert!(wanted > 0 && (wanted as usize) >= v.subs.len());
    let n = (wanted as usize) * 3 / 2 + 1;
    while v.subs.len() < n {
        v.subs.push(None);
    }
    debug_assert_eq!(v.subs.len(), n);
    debug_assert!((wanted as usize) < v.subs.len());
}

// =============================================================================
// onechr - fill in arcs for a plain character (+ case complements)
// =============================================================================

/// onechr - fill in arcs for a plain character, and possible case complements.
/// C: `static void onechr(struct vars *v, chr c, struct state *lp, struct state *rp)`.
pub fn onechr(v: &mut Vars, c: chr, lp: StateId, rp: StateId) -> RegResult<()> {
    if (v.cflags & REG_ICASE) == 0 {
        let mut lastsubcolor: color = COLORLESS;
        // subcoloronechr(v, c, lp, rp, &lastsubcolor)
        return subcoloronechr(v.mcx, &mut v.nfa, &mut v.cm, c, lp, rp, &mut lastsubcolor);
    }

    // rats, need general case anyway... subcolorcvec(v, allcases(v, c), lp, rp)
    let cv = allcases(v.mcx, c)?;
    let r = subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, lp, rp);
    v.cv = Some(cv);
    r
}

// =============================================================================
// charclass / charclasscomplement
// =============================================================================

/// charclass - generate arcs for a character class. The caller must call
/// okcolors() at the end of processing the atom or bracket.
/// C: `static void charclass(struct vars *v, enum char_classes cls, struct state
/// *lp, struct state *rp)`.
pub fn charclass(v: &mut Vars, cls: char_classes, lp: StateId, rp: StateId) -> RegResult<()> {
    v.NOTE(REG_ULOCALE);
    let cv = cclasscvec(v.mcx, cls as i32, v.cflags & REG_ICASE)?; // NOERR()
    let r = subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, lp, rp);
    v.cv = Some(cv);
    r
}

/// charclasscomplement - generate arcs for a complemented character class.
/// C: `static void charclasscomplement(struct vars *v, enum char_classes cls,
/// struct state *lp, struct state *rp)`.
pub fn charclasscomplement(
    v: &mut Vars,
    cls: char_classes,
    lp: StateId,
    rp: StateId,
) -> RegResult<()> {
    // make dummy state to hang temporary arcs on
    let cstate = newstate(v.mcx, &mut v.nfa)?; // NOERR()

    v.NOTE(REG_ULOCALE);
    let cv = cclasscvec(v.mcx, cls as i32, v.cflags & REG_ICASE)?; // NOERR()

    // build arcs for char class; this may cause color splitting
    subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, cstate, cstate)?; // NOERR()
    v.cv = Some(cv);

    // clean up any subcolors in the arc set
    okcolors(v.mcx, &mut v.nfa, &mut v.cm, false)?; // NOERR()

    // now build output arcs for the complement of the char class
    colorcomplement(v.mcx, &mut v.nfa, &mut v.cm, false, PLAIN, cstate, lp, rp)?; // NOERR()

    // clean up dummy state
    dropstate(&mut v.nfa, &mut v.cm, false, cstate)?;
    Ok(())
}

// =============================================================================
// nonword / word / wordchrs
// =============================================================================

/// nonword - generate arcs for non-word-character ahead or behind.
/// C: `static void nonword(struct vars *v, int dir, struct state *lp, struct state *rp)`.
pub fn nonword(v: &mut Vars, dir: i32, lp: StateId, rp: StateId) -> RegResult<()> {
    let anchor = if dir == AHEAD {
        b'$' as i32
    } else {
        b'^' as i32
    };

    debug_assert!(dir == AHEAD || dir == BEHIND);
    newarc(v.mcx, &mut v.nfa, &mut v.cm, false, anchor, 1, lp, rp)?;
    newarc(v.mcx, &mut v.nfa, &mut v.cm, false, anchor, 0, lp, rp)?;
    let wordchrs = v.wordchrs.expect("nonword: wordchrs not set up");
    colorcomplement(v.mcx, &mut v.nfa, &mut v.cm, false, dir, wordchrs, lp, rp)?;
    // (no need for special attention to \n)
    Ok(())
}

/// word - generate arcs for word character ahead or behind.
/// C: `static void word(struct vars *v, int dir, struct state *lp, struct state *rp)`.
pub fn word(v: &mut Vars, dir: i32, lp: StateId, rp: StateId) -> RegResult<()> {
    debug_assert!(dir == AHEAD || dir == BEHIND);
    let wordchrs = v.wordchrs.expect("word: wordchrs not set up");
    cloneouts(v.mcx, &mut v.nfa, &mut v.cm, false, wordchrs, lp, rp, dir)
    // (no need for special attention to \n)
}

/// wordchrs - set up word-chr list for word-boundary stuff, if needed.
/// C: `static void wordchrs(struct vars *v)`.
pub fn wordchrs(v: &mut Vars) -> RegResult<()> {
    if v.wordchrs.is_some() {
        return Ok(()); // done already
    }

    // make dummy state to hang the cache arcs on
    let cstate = newstate(v.mcx, &mut v.nfa)?; // NOERR()

    v.NOTE(REG_ULOCALE);
    let cv = cclasscvec(v.mcx, char_classes::CC_WORD as i32, v.cflags & REG_ICASE)?; // NOERR()

    // build the arcs; this may cause color splitting
    subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, cstate, cstate)?; // NOERR()
    v.cv = Some(cv);

    // close new open subcolors to ensure the cache entry is self-contained
    okcolors(v.mcx, &mut v.nfa, &mut v.cm, false)?; // NOERR()

    // success! save the cache pointer
    v.wordchrs = Some(cstate);
    Ok(())
}

// =============================================================================
// scanplain
// =============================================================================

/// scanplain - scan PLAIN contents of `[.` etc. Returns the cursor index just
/// after the end of the scanned sequence (C returns `const chr *`).
/// C: `static const chr *scanplain(struct vars *v)`.
pub fn scanplain(v: &mut Vars) -> usize {
    debug_assert!(v.SEE(COLLEL) || v.SEE(ECLASS) || v.SEE(CCLASS));
    v.next();

    let mut endp = v.cursor;
    while v.SEE(PLAIN) {
        endp = v.cursor;
        v.next();
    }

    debug_assert!(v.SEE(END) || v.ISERR());
    v.next();

    endp
}

// =============================================================================
// brackpart / bracket / cbracket / optimizebracket
// =============================================================================

/// Convert a `nextvalue` carrying an `enum char_classes` ordinal into the enum.
fn char_class_from_chr(n: chr) -> char_classes {
    use char_classes::*;
    match n {
        0 => CC_ALNUM,
        1 => CC_ALPHA,
        2 => CC_ASCII,
        3 => CC_BLANK,
        4 => CC_CNTRL,
        5 => CC_DIGIT,
        6 => CC_GRAPH,
        7 => CC_LOWER,
        8 => CC_PRINT,
        9 => CC_PUNCT,
        10 => CC_SPACE,
        11 => CC_UPPER,
        12 => CC_XDIGIT,
        13 => CC_WORD,
        _ => unreachable!("char class ordinal out of range"),
    }
}

/// brackpart - handle one item (or range) within a bracket expression.
/// C: `static void brackpart(struct vars *v, struct state *lp, struct state *rp,
/// bool *have_cclassc)`.
pub fn brackpart(
    v: &mut Vars,
    lp: StateId,
    rp: StateId,
    have_cclassc: &mut [bool; NUM_CCLASSES as usize],
) -> RegResult<()> {
    let startc: chr;
    let endc: chr;

    // The locale leaf functions signal failure via their RegResult return; on
    // the Err path we re-latch via v.seterr(e) (== C's ERR(e)) before bailing.
    macro_rules! latch {
        ($e:expr) => {
            match $e {
                Ok(val) => val,
                Err(RegError(code)) => {
                    v.seterr(code);
                    return Ok(());
                }
            }
        };
    }

    // parse something, get rid of special cases, take shortcuts
    match v.nexttype {
        t if t == RANGE => {
            v.seterr(REG_ERANGE); // a-b-c or other botch
            return Ok(());
        }
        t if t == PLAIN => {
            startc = v.nextvalue;
            v.next();
            // shortcut for ordinary chr (not range)
            if !v.SEE(RANGE) {
                return onechr(v, startc, lp, rp);
            }
            if v.ISERR() {
                return Ok(()); // NOERR()
            }
        }
        t if t == COLLEL => {
            let startp = v.cursor;
            let endp = scanplain(v);
            v.insist(startp < endp, REG_ECOLLATE);
            if v.ISERR() {
                return Ok(());
            }
            let er = latch!(element(&v.pattern[startp..endp]));
            if er.note_ulocale {
                v.NOTE(REG_ULOCALE);
            }
            startc = er.code;
        }
        t if t == ECLASS => {
            let startp = v.cursor;
            let endp = scanplain(v);
            v.insist(startp < endp, REG_ECOLLATE);
            if v.ISERR() {
                return Ok(());
            }
            let er = latch!(element(&v.pattern[startp..endp]));
            if er.note_ulocale {
                v.NOTE(REG_ULOCALE);
            }
            startc = er.code;
            let cv = latch!(eclass(v.mcx, v.cflags, startc, v.cflags & REG_ICASE));
            let r = subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, lp, rp);
            v.cv = Some(cv);
            return r;
        }
        t if t == CCLASS => {
            let startp = v.cursor;
            let endp = scanplain(v);
            v.insist(startp < endp, REG_ECTYPE);
            if v.ISERR() {
                return Ok(());
            }
            let cls = latch!(lookupcclass(&v.pattern[startp..endp]));
            return charclass(v, char_class_from_chr(cls as chr), lp, rp);
        }
        t if t == CCLASSS => {
            let cls = char_class_from_chr(v.nextvalue);
            charclass(v, cls, lp, rp)?;
            v.next();
            return Ok(());
        }
        t if t == CCLASSC => {
            // we cannot call charclasscomplement() immediately
            have_cclassc[v.nextvalue as usize] = true;
            v.next();
            return Ok(());
        }
        _ => {
            v.seterr(REG_ASSERT);
            return Ok(());
        }
    }

    if v.SEE(RANGE) {
        v.next();
        match v.nexttype {
            t if t == PLAIN || t == RANGE => {
                endc = v.nextvalue;
                v.next();
                if v.ISERR() {
                    return Ok(());
                }
            }
            t if t == COLLEL => {
                let startp = v.cursor;
                let endp = scanplain(v);
                v.insist(startp < endp, REG_ECOLLATE);
                if v.ISERR() {
                    return Ok(());
                }
                let er = latch!(element(&v.pattern[startp..endp]));
                if er.note_ulocale {
                    v.NOTE(REG_ULOCALE);
                }
                endc = er.code;
            }
            _ => {
                v.seterr(REG_ERANGE);
                return Ok(());
            }
        }
    } else {
        endc = startc;
    }

    // Ranges are unportable.
    if startc != endc {
        v.NOTE(REG_UUNPORT);
    }
    let cv = latch!(range(v.mcx, startc, endc, v.cflags & REG_ICASE)); // C: NOERR()
    let r = subcolorcvec(v.mcx, &mut v.nfa, &mut v.cm, &cv, lp, rp);
    v.cv = Some(cv);
    r
}

/// bracket - handle non-complemented bracket expression.
/// C: `static void bracket(struct vars *v, struct state *lp, struct state *rp)`.
pub fn bracket(v: &mut Vars, lp: StateId, rp: StateId) -> RegResult<()> {
    let mut have_cclassc = [false; NUM_CCLASSES as usize];

    debug_assert!(v.SEE(b'[' as i32));
    v.next();
    while !v.SEE(b']' as i32) && !v.SEE(EOS) {
        brackpart(v, lp, rp, &mut have_cclassc)?;
    }
    debug_assert!(v.SEE(b']' as i32) || v.ISERR());

    // close up open subcolors from the positive bracket elements
    okcolors(v.mcx, &mut v.nfa, &mut v.cm, false)?; // NOERR()

    // now handle any complemented elements
    let mut any_cclassc = false;
    #[allow(clippy::needless_range_loop)]
    for i in 0..(NUM_CCLASSES as usize) {
        if have_cclassc[i] {
            charclasscomplement(v, char_class_from_chr(i as chr), lp, rp)?; // NOERR()
            any_cclassc = true;
        }
    }

    // If we had any complemented elements, see if we can optimize to a rainbow.
    if any_cclassc {
        optimizebracket(v, lp, rp)?;
    }
    Ok(())
}

/// cbracket - handle complemented bracket expression.
/// C: `static void cbracket(struct vars *v, struct state *lp, struct state *rp)`.
pub fn cbracket(v: &mut Vars, lp: StateId, rp: StateId) -> RegResult<()> {
    let left = newstate(v.mcx, &mut v.nfa)?;
    let right = newstate(v.mcx, &mut v.nfa)?;

    if v.ISERR() {
        return Ok(()); // NOERR()
    }
    bracket(v, left, right)?;

    // in NLSTOP mode, ensure newline is not part of the result set
    if (v.cflags & REG_NLSTOP) != 0 {
        let nlcolor = v.nlcolor;
        newarc(v.mcx, &mut v.nfa, &mut v.cm, false, PLAIN, nlcolor, left, right)?;
    }
    if v.ISERR() {
        return Ok(()); // NOERR()
    }

    debug_assert_eq!(v.nfa.state_arena[lp.0 as usize].nouts, 0); // all outarcs ours

    // Easy part of complementing.
    colorcomplement(v.mcx, &mut v.nfa, &mut v.cm, false, PLAIN, left, lp, rp)?; // NOERR()
    dropstate(&mut v.nfa, &mut v.cm, false, left)?;
    debug_assert_eq!(v.nfa.state_arena[right.0 as usize].nins, 0);
    freestate(&mut v.nfa, right);
    Ok(())
}

/// optimizebracket - see if bracket expression can be converted to RAINBOW.
/// C: `static void optimizebracket(struct vars *v, struct state *lp, struct state *rp)`.
pub fn optimizebracket(v: &mut Vars, lp: StateId, rp: StateId) -> RegResult<()> {
    // Scan lp's out-arcs and transiently mark the mentioned colors.
    let mut cur = v.nfa.state_arena[lp.0 as usize].outs;
    while let Some(a) = cur {
        let arc = v.nfa.arc_arena[a.0 as usize];
        debug_assert_eq!(arc.type_, PLAIN);
        debug_assert!(arc.co >= 0); // i.e. not RAINBOW
        debug_assert_eq!(arc.to, Some(rp));
        let cd = &mut v.cm.cd[arc.co as usize];
        debug_assert!((cd.flags & FREECOL) == 0 && (cd.flags & PSEUDO) == 0);
        cd.flags |= COLMARK;
        cur = arc.outchain;
    }

    // Scan colors, clear transient marks, check for unmarked live colors.
    let mut israinbow = true;
    for co in 0..=v.cm.max {
        let cd = &mut v.cm.cd[co];
        if (cd.flags & COLMARK) != 0 {
            cd.flags &= !COLMARK;
        } else if (cd.flags & FREECOL) == 0 && (cd.flags & PSEUDO) == 0 {
            israinbow = false;
        }
    }

    // Can't do anything if not all colors have arcs.
    if !israinbow {
        return Ok(());
    }

    // OK, drop existing arcs and replace with a rainbow.
    while let Some(a) = v.nfa.state_arena[lp.0 as usize].outs {
        freearc(&mut v.nfa, &mut v.cm, false, a);
    }
    newarc(v.mcx, &mut v.nfa, &mut v.cm, false, PLAIN, RAINBOW, lp, rp)
}

// =============================================================================
// processlacon / newlacon
// =============================================================================

/// processlacon - generate the NFA representation of a LACON.
/// C: `static void processlacon(struct vars *v, struct state *begin, struct state
/// *end, int latype, struct state *lp, struct state *rp)`.
pub fn processlacon(
    v: &mut Vars,
    begin: StateId,
    end: StateId,
    latype: i32,
    lp: StateId,
    rp: StateId,
) -> RegResult<()> {
    // Check for lookaround RE consisting of a single plain color arc (or set).
    let s1 = single_color_transition(&v.nfa, begin, end);

    match latype {
        LATYPE_AHEAD_POS => {
            if let Some(s1) = s1 {
                return cloneouts(v.mcx, &mut v.nfa, &mut v.cm, false, s1, lp, rp, AHEAD);
            }
        }
        LATYPE_AHEAD_NEG => {
            if let Some(s1) = s1 {
                colorcomplement(v.mcx, &mut v.nfa, &mut v.cm, false, AHEAD, s1, lp, rp)?;
                newarc(v.mcx, &mut v.nfa, &mut v.cm, false, b'$' as i32, 1, lp, rp)?;
                newarc(v.mcx, &mut v.nfa, &mut v.cm, false, b'$' as i32, 0, lp, rp)?;
                return Ok(());
            }
        }
        LATYPE_BEHIND_POS => {
            if let Some(s1) = s1 {
                return cloneouts(v.mcx, &mut v.nfa, &mut v.cm, false, s1, lp, rp, BEHIND);
            }
        }
        LATYPE_BEHIND_NEG => {
            if let Some(s1) = s1 {
                colorcomplement(v.mcx, &mut v.nfa, &mut v.cm, false, BEHIND, s1, lp, rp)?;
                newarc(v.mcx, &mut v.nfa, &mut v.cm, false, b'^' as i32, 1, lp, rp)?;
                newarc(v.mcx, &mut v.nfa, &mut v.cm, false, b'^' as i32, 0, lp, rp)?;
                return Ok(());
            }
        }
        _ => {
            debug_assert!(false, "NOTREACHED");
        }
    }

    // General case: we need a LACON subre and arc.
    let n = newlacon(v, begin, end, latype as u8)?;
    newarc(v.mcx, &mut v.nfa, &mut v.cm, false, LACON, n as color, lp, rp)
}

/// newlacon - allocate a lookaround-constraint subRE (returns the lacon number).
/// C: `static int newlacon(struct vars *v, struct state *begin, struct state
/// *end, int latype)`.
pub fn newlacon(v: &mut Vars, begin: StateId, end: StateId, latype: u8) -> RegResult<i32> {
    let n: i32;
    if v.nlacons == 0 {
        n = 1; // skip 0th
        debug_assert!(v.lacons.is_empty());
        v.lacons.push(blank_subre()); // reserved 0th
        v.lacons.push(blank_subre()); // slot 1
    } else {
        n = v.nlacons;
        v.lacons.push(blank_subre());
    }
    v.nlacons = n + 1;
    let sub = &mut v.lacons[n as usize];
    sub.begin = Some(begin);
    sub.end = Some(end);
    sub.latype = latype;
    sub.cnfa = None; // ZAPCNFA(sub->cnfa)
    Ok(n)
}

// =============================================================================
// repeat - replicate subNFA for quantifiers
// =============================================================================

const REPEAT_SOME: i32 = 2;
const REPEAT_INF: i32 = 3;

/// `PAIR(x, y)` -- pack the reduced m/n pair into a switch key (regcomp.c).
#[inline]
const fn PAIR(x: i32, y: i32) -> i32 {
    x * 4 + y
}

/// `REDUCE(x)` -- collapse a count to {0, 1, SOME, INF} (regcomp.c).
#[inline]
const fn REDUCE(x: i32) -> i32 {
    if x == DUPINF {
        REPEAT_INF
    } else if x > 1 {
        REPEAT_SOME
    } else {
        x
    }
}

/// repeat - replicate subNFA for quantifiers. The sub-NFA strung from `lp` to
/// `rp` is modified to represent m to n repetitions of its initial contents.
/// C: `static void repeat(struct vars *v, struct state *lp, struct state *rp,
/// int m, int n)`.
pub fn repeat(v: &mut Vars, lp: StateId, rp: StateId, m: i32, n: i32) -> RegResult<()> {
    let mut g = DepthGuard::enter(v)?;
    let v = &mut *g;

    let rm = REDUCE(m);
    let rn = REDUCE(n);

    macro_rules! emptyarc {
        ($x:expr, $y:expr) => {
            newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, $x, $y)?
        };
    }

    let pair = PAIR(rm, rn);
    if pair == PAIR(0, 0) {
        // empty string
        delsub(&mut v.nfa, &mut v.cm, false, lp, rp)?;
        emptyarc!(lp, rp);
    } else if pair == PAIR(0, 1) {
        // do as x|
        emptyarc!(lp, rp);
    } else if pair == PAIR(0, REPEAT_SOME) {
        // do as x{1,n}|
        repeat(v, lp, rp, 1, n)?; // NOERR()
        emptyarc!(lp, rp);
    } else if pair == PAIR(0, REPEAT_INF) {
        // loop x around
        let s = newstate(v.mcx, &mut v.nfa)?; // NOERR()
        moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s)?;
        moveins(v.mcx, &mut v.nfa, &mut v.cm, false, rp, s)?;
        emptyarc!(lp, s);
        emptyarc!(s, rp);
    } else if pair == PAIR(1, 1) {
        // no action required
    } else if pair == PAIR(1, REPEAT_SOME) {
        // do as x{0,n-1}x = (x{1,n-1}|)x
        let s = newstate(v.mcx, &mut v.nfa)?; // NOERR()
        moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s)?;
        dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, s, rp, lp, s)?; // NOERR()
        repeat(v, lp, s, 1, n - 1)?; // NOERR()
        emptyarc!(lp, s);
    } else if pair == PAIR(1, REPEAT_INF) {
        // add loopback arc
        let s = newstate(v.mcx, &mut v.nfa)?;
        let s2 = newstate(v.mcx, &mut v.nfa)?; // NOERR()
        moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s)?;
        moveins(v.mcx, &mut v.nfa, &mut v.cm, false, rp, s2)?;
        emptyarc!(lp, s);
        emptyarc!(s2, rp);
        emptyarc!(s2, s);
    } else if pair == PAIR(REPEAT_SOME, REPEAT_SOME) {
        // do as x{m-1,n-1}x
        let s = newstate(v.mcx, &mut v.nfa)?; // NOERR()
        moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s)?;
        dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, s, rp, lp, s)?; // NOERR()
        repeat(v, lp, s, m - 1, n - 1)?;
    } else if pair == PAIR(REPEAT_SOME, REPEAT_INF) {
        // do as x{m-1,}x
        let s = newstate(v.mcx, &mut v.nfa)?; // NOERR()
        moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s)?;
        dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, s, rp, lp, s)?; // NOERR()
        repeat(v, lp, s, m - 1, n)?;
    } else {
        v.seterr(REG_ASSERT);
    }
    Ok(())
}

// =============================================================================
// parse / parsebranch / parseqatom
// =============================================================================

/// parse - parse an RE. The top level: a bunch of branches tied together with
/// `|`. C: `static struct subre *parse(struct vars *v, int stopper, int type,
/// struct state *init, struct state *final)`. Returns `None` if an error has
/// been latched (C returns NULL via the NOERRN checkpoints).
pub fn parse(
    v: &mut Vars,
    stopper: i32,
    type_: i32,
    init: StateId,
    final_: StateId,
) -> Option<NodeId> {
    let mut g = match DepthGuard::enter(v) {
        Ok(g) => g,
        Err(_) => return None,
    };
    let v = &mut *g;

    debug_assert!(stopper == b')' as i32 || stopper == EOS);

    let branches = subre(v, b'|', LONGER, Some(init), Some(final_))?; // NOERRN
    let mut lastbranch: Option<NodeId> = None;
    loop {
        // a branch
        let left = match newstate(v.mcx, &mut v.nfa) {
            Ok(s) => s,
            Err(_) => return None,
        };
        let right = match newstate(v.mcx, &mut v.nfa) {
            Ok(s) => s,
            Err(_) => return None,
        };
        if v.NISERR() {
            return None; // NOERRN
        }
        // EMPTYARC(init, left); EMPTYARC(right, final);
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, init, left).is_err() {
            return None;
        }
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, right, final_).is_err() {
            return None;
        }
        if v.NISERR() {
            return None; // NOERRN
        }
        let branch = parsebranch(v, stopper, type_, left, right, 0)?; // NOERRN
        if let Some(lb) = lastbranch {
            v.tm(lb).sibling = Some(branch);
        } else {
            v.tm(branches).child = Some(branch);
        }
        // branches->flags |= UP(branches->flags | branch->flags);
        let bf = v.t(branches).flags;
        let chf = v.t(branch).flags;
        v.tm(branches).flags |= UP(bf | chf);
        lastbranch = Some(branch);

        // EAT('|')
        if !(v.SEE(b'|' as i32) && v.next() != 0) {
            break;
        }
    }
    debug_assert!(v.SEE(stopper) || v.SEE(EOS));

    if !v.SEE(stopper) {
        debug_assert!(stopper == b')' as i32 && v.SEE(EOS));
        v.seterr(REG_EPAREN);
    }

    // optimize out simple cases
    let mut branches = branches;
    if lastbranch == v.t(branches).child {
        // only one branch
        debug_assert!(v.t(lastbranch.unwrap()).sibling.is_none());
        freesrnode(v, Some(branches));
        branches = lastbranch.unwrap();
    } else if !MESSY(v.t(branches).flags) {
        // no interesting innards
        let child = v.t(branches).child;
        freesubreandsiblings(v, child);
        v.tm(branches).child = None;
        v.tm(branches).op = b'=';
    }

    Some(branches)
}

/// parsebranch - parse one branch of an RE (concatenation).
/// C: `static struct subre *parsebranch(struct vars *v, int stopper, int type,
/// struct state *left, struct state *right, int partial)`.
pub fn parsebranch(
    v: &mut Vars,
    stopper: i32,
    type_: i32,
    left: StateId,
    right: StateId,
    partial: i32,
) -> Option<NodeId> {
    let mut g = match DepthGuard::enter(v) {
        Ok(g) => g,
        Err(_) => return None,
    };
    let v = &mut *g;

    let mut lp = left; // left end of current construct
    let mut seencontent = 0; // is there anything in this branch yet?
    let mut t = subre(v, b'=', 0, Some(left), Some(right))?; // op '=' is tentative; NOERRN

    while !v.SEE(b'|' as i32) && !v.SEE(stopper) && !v.SEE(EOS) {
        if seencontent != 0 {
            // implicit concat operator
            lp = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if moveins(v.mcx, &mut v.nfa, &mut v.cm, false, right, lp).is_err() {
                return None;
            }
        }
        seencontent = 1;

        // NB, recursion in parseqatom() may swallow rest of branch
        t = parseqatom(v, stopper, type_, lp, right, t)?; // NOERRN
    }

    if seencontent == 0 {
        // empty branch
        if partial == 0 {
            v.NOTE(REG_UUNSPEC);
        }
        debug_assert_eq!(lp, left);
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, left, right).is_err() {
            return None;
        }
    }

    Some(t)
}

/// The `case PLAIN:` / `)`-falls-through body of [`parseqatom`]: emit one plain
/// chr arc and advance the lexer. Returns `None` on a latched error (NOERRN).
fn onechr_and_next(v: &mut Vars, lp: StateId, rp: StateId) -> Option<()> {
    if onechr(v, v.nextvalue, lp, rp).is_err() {
        return None;
    }
    if okcolors(v.mcx, &mut v.nfa, &mut v.cm, false).is_err() {
        return None;
    }
    if v.NISERR() {
        return None; // NOERRN
    }
    v.next();
    Some(())
}

/// parseqatom - parse one quantified atom or constraint of an RE.
/// C: `static struct subre *parseqatom(struct vars *v, int stopper, int type,
/// struct state *lp, struct state *rp, struct subre *top)`.
///
/// The single riskiest function in the engine; control flow is kept line-aligned
/// with the C in regcomp.c.
pub fn parseqatom(
    v: &mut Vars,
    stopper: i32,
    type_: i32,
    lp: StateId,
    rp: StateId,
    mut top: NodeId,
) -> Option<NodeId> {
    let mut g = match DepthGuard::enter(v) {
        Ok(g) => g,
        Err(_) => return None,
    };
    let v = &mut *g;

    // ARCV(t, val) == newarc(v->nfa, t, val, lp, rp)
    macro_rules! arcv {
        ($t:expr, $val:expr) => {
            if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, $t, $val, lp, rp).is_err() {
                return None;
            }
        };
    }

    let m: i32;
    let n: i32;
    let mut atom: Option<NodeId> = None; // atom's subtree

    let cap: i32; // capturing parens?
    let latype: i32; // lookaround constraint type
    let mut subno: i32 = 0; // capturing-parens or backref number
    let mut atomtype: i32;
    let qprefer: u8; // quantifier short/long preference
    let mut f: u8;

    // initial bookkeeping
    debug_assert_eq!(v.nfa.state_arena[lp.0 as usize].nouts, 0); // must string new code
    debug_assert_eq!(v.nfa.state_arena[rp.0 as usize].nins, 0); // between lp and rp

    // ----- (a) an atom or constraint... -----
    atomtype = v.nexttype;
    'atomdone: {
        if atomtype == b'^' as i32 {
            arcv!(b'^' as i32, 1);
            if (v.cflags & REG_NLANCH) != 0 {
                let nlcolor = v.nlcolor;
                arcv!(BEHIND, nlcolor);
            }
            v.next();
            return Some(top);
        } else if atomtype == b'$' as i32 {
            arcv!(b'$' as i32, 1);
            if (v.cflags & REG_NLANCH) != 0 {
                let nlcolor = v.nlcolor;
                arcv!(AHEAD, nlcolor);
            }
            v.next();
            return Some(top);
        } else if atomtype == SBEGIN {
            arcv!(b'^' as i32, 1); // BOL
            arcv!(b'^' as i32, 0); // or BOS
            v.next();
            return Some(top);
        } else if atomtype == SEND {
            arcv!(b'$' as i32, 1); // EOL
            arcv!(b'$' as i32, 0); // or EOS
            v.next();
            return Some(top);
        } else if atomtype == b'<' as i32 {
            if wordchrs(v).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if nonword(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if word(v, AHEAD, s, rp).is_err() {
                return None;
            }
            v.next();
            return Some(top);
        } else if atomtype == b'>' as i32 {
            if wordchrs(v).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if word(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if nonword(v, AHEAD, s, rp).is_err() {
                return None;
            }
            v.next();
            return Some(top);
        } else if atomtype == WBDRY {
            if wordchrs(v).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if nonword(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if word(v, AHEAD, s, rp).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if word(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if nonword(v, AHEAD, s, rp).is_err() {
                return None;
            }
            v.next();
            return Some(top);
        } else if atomtype == NWBDRY {
            if wordchrs(v).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if word(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if word(v, AHEAD, s, rp).is_err() {
                return None;
            }
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            if nonword(v, BEHIND, lp, s).is_err() {
                return None;
            }
            if nonword(v, AHEAD, s, rp).is_err() {
                return None;
            }
            v.next();
            return Some(top);
        } else if atomtype == LACON {
            // lookaround constraint
            latype = v.nextvalue as i32;
            v.next();
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None,
            };
            let s2 = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            let lt = parse(v, b')' as i32, LACON, s, s2);
            freesubre(v, lt); // internal structure irrelevant
            if v.NISERR() {
                return None; // NOERRN
            }
            debug_assert!(v.SEE(b')' as i32));
            v.next();
            if processlacon(v, s, s2, latype, lp, rp).is_err() {
                return None;
            }
            return Some(top);
        } else if atomtype == b'*' as i32
            || atomtype == b'+' as i32
            || atomtype == b'?' as i32
            || atomtype == b'{' as i32
        {
            // then errors, to get them out of the way
            v.seterr(REG_BADRPT);
            return Some(top);
        } else if atomtype == b')' as i32 {
            // unbalanced paren
            if (v.cflags & REG_ADVANCED) != REG_EXTENDED {
                v.seterr(REG_EPAREN);
                return Some(top);
            }
            // legal in EREs due to specification botch
            v.NOTE(REG_UPBOTCH);
            // fall through into case PLAIN
            onechr_and_next(v, lp, rp)?;
            break 'atomdone;
        } else if atomtype == PLAIN {
            onechr_and_next(v, lp, rp)?;
            break 'atomdone;
        } else if atomtype == b'[' as i32 {
            if v.nextvalue == 1 {
                if bracket(v, lp, rp).is_err() {
                    return None;
                }
            } else if cbracket(v, lp, rp).is_err() {
                return None;
            }
            debug_assert!(v.SEE(b']' as i32) || v.ISERR());
            v.next();
            break 'atomdone;
        } else if atomtype == CCLASSS {
            let cls = char_class_from_chr(v.nextvalue);
            if charclass(v, cls, lp, rp).is_err() {
                return None;
            }
            if okcolors(v.mcx, &mut v.nfa, &mut v.cm, false).is_err() {
                return None;
            }
            v.next();
            break 'atomdone;
        } else if atomtype == CCLASSC {
            let cls = char_class_from_chr(v.nextvalue);
            if charclasscomplement(v, cls, lp, rp).is_err() {
                return None;
            }
            // charclasscomplement() did okcolors() internally
            v.next();
            break 'atomdone;
        } else if atomtype == b'.' as i32 {
            let but = if (v.cflags & REG_NLSTOP) != 0 {
                v.nlcolor
            } else {
                COLORLESS
            };
            if rainbow(v.mcx, &mut v.nfa, &mut v.cm, false, PLAIN, but, lp, rp).is_err() {
                return None;
            }
            v.next();
            break 'atomdone;
        } else if atomtype == b'(' as i32 {
            // value flags as capturing or non
            cap = if type_ == LACON {
                0
            } else {
                v.nextvalue as i32
            };
            if cap != 0 {
                v.nsubexp += 1;
                subno = v.nsubexp;
                if (subno as usize) >= v.subs.len() {
                    moresubs(v, subno);
                }
            } else {
                atomtype = PLAIN; // something that's not '('
            }
            v.next();

            // separate endpoint states to keep this sub-NFA distinct
            let s = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None,
            };
            let s2 = match newstate(v.mcx, &mut v.nfa) {
                Ok(s) => s,
                Err(_) => return None, // NOERRN
            };
            // We may not need these arcs, but keep things connected for now
            if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, lp, s).is_err() {
                return None;
            }
            if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, s2, rp).is_err() {
                return None;
            }
            if v.NISERR() {
                return None; // NOERRN
            }
            let parsed = parse(v, b')' as i32, type_, s, s2);
            debug_assert!(v.SEE(b')' as i32) || v.ISERR());
            v.next();
            let parsed = parsed?; // NOERRN
            atom = Some(parsed);
            if cap != 0 {
                if v.t(parsed).capno == 0 {
                    // normal case: just mark the atom as capturing
                    v.tm(parsed).flags |= CAP;
                    v.tm(parsed).capno = subno;
                } else {
                    // generate no-op wrapper node to handle "((x))"
                    let aflags = v.t(parsed).flags;
                    let tt = subre(v, b'(', aflags | CAP, Some(s), Some(s2))?; // NOERRN
                    v.tm(tt).capno = subno;
                    v.tm(tt).child = Some(parsed);
                    atom = Some(tt);
                }
                debug_assert!(v.subs[subno as usize].is_none());
                v.subs[subno as usize] = atom;
            }
            // postpone everything else pending possible {0}
        } else if atomtype == BACKREF {
            // the Feature From The Black Lagoon
            v.insist(type_ != LACON, REG_ESUBREG);
            subno = v.nextvalue as i32;
            debug_assert!(subno > 0);
            v.insist((subno as usize) < v.subs.len(), REG_ESUBREG);
            if v.NISERR() {
                return None; // NOERRN
            }
            v.insist(
                (subno as usize) < v.subs.len() && v.subs[subno as usize].is_some(),
                REG_ESUBREG,
            );
            if v.NISERR() {
                return None; // NOERRN
            }
            let a = subre(v, b'b', BACKR, Some(lp), Some(rp))?; // NOERRN
            v.tm(a).backno = subno;
            let target = v.subs[subno as usize].unwrap();
            v.tm(target).flags |= BRUSE;
            atom = Some(a);
            // EMPTYARC(lp, rp) -- temporarily, so there's something
            if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, lp, rp).is_err() {
                return None;
            }
            v.next();
        } else {
            // default: assert-can't-happen
            v.seterr(REG_ASSERT);
            return Some(top);
        }
    } // 'atomdone

    // ----- (b) ...and an atom may be followed by a quantifier -----
    if v.nexttype == b'*' as i32 {
        m = 0;
        n = DUPINF;
        qprefer = if v.nextvalue != 0 { LONGER } else { SHORTER };
        v.next();
    } else if v.nexttype == b'+' as i32 {
        m = 1;
        n = DUPINF;
        qprefer = if v.nextvalue != 0 { LONGER } else { SHORTER };
        v.next();
    } else if v.nexttype == b'?' as i32 {
        m = 0;
        n = 1;
        qprefer = if v.nextvalue != 0 { LONGER } else { SHORTER };
        v.next();
    } else if v.nexttype == b'{' as i32 {
        v.next();
        m = scannum(v);
        // EAT(',')
        if v.SEE(b',' as i32) && v.next() != 0 {
            if v.SEE(DIGIT) {
                n = scannum(v);
            } else {
                n = DUPINF;
            }
            if m > n {
                v.seterr(REG_BADBR);
                return Some(top);
            }
            // {m,n} exercises preference, even if it's {m,m}
            qprefer = if v.nextvalue != 0 { LONGER } else { SHORTER };
        } else {
            n = m;
            // {m} passes operand's preference through
            qprefer = 0;
        }
        if !v.SEE(b'}' as i32) {
            // catches errors too
            v.seterr(REG_BADBR);
            return Some(top);
        }
        v.next();
    } else {
        // no quantifier
        m = 1;
        n = 1;
        qprefer = 0;
    }

    // annoying special case: {0} or {0,0} cancels everything
    if m == 0 && n == 0 {
        // Don't destroy capturing subexpression(s) within the atom.
        if let Some(at) = atom {
            if (v.t(at).flags & CAP) != 0 {
                let abegin = v.t(at).begin.unwrap();
                let aend = v.t(at).end.unwrap();
                if delsub(&mut v.nfa, &mut v.cm, false, lp, abegin).is_err() {
                    return None;
                }
                if delsub(&mut v.nfa, &mut v.cm, false, aend, rp).is_err() {
                    return None;
                }
            } else {
                // Otherwise, clean up any subre infrastructure we made
                freesubre(v, atom);
                if delsub(&mut v.nfa, &mut v.cm, false, lp, rp).is_err() {
                    return None;
                }
            }
        } else if delsub(&mut v.nfa, &mut v.cm, false, lp, rp).is_err() {
            return None;
        }
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, lp, rp).is_err() {
            return None;
        }
        return Some(top);
    }

    // ----- (c) if not a messy case, avoid hard part -----
    debug_assert!(!MESSY(v.t(top).flags));
    f = v.t(top).flags | qprefer | atom.map_or(0, |a| v.t(a).flags);
    if atomtype != b'(' as i32 && atomtype != BACKREF && !MESSY(UP(f)) {
        if !(m == 1 && n == 1) && repeat(v, lp, rp, m, n).is_err() {
            return None;
        }
        if atom.is_some() {
            freesubre(v, atom);
        }
        v.tm(top).flags = f;
        return Some(top);
    }

    // ----- (d) hard part: something messy -----
    // (capturing parens, back reference, short/long clash, or substructure)

    // now we'll need a subre for the contents even if they're boring
    if atom.is_none() {
        atom = Some(subre(v, b'=', 0, Some(lp), Some(rp))?); // NOERRN
    }
    let atom_id = atom.unwrap();

    // Give the atom its own begin/end states distinct from lp/rp.
    let mut s: StateId;
    let mut s2: StateId;
    if v.t(atom_id).begin == Some(lp) || v.t(atom_id).end == Some(rp) {
        s = match newstate(v.mcx, &mut v.nfa) {
            Ok(x) => x,
            Err(_) => return None,
        };
        s2 = match newstate(v.mcx, &mut v.nfa) {
            Ok(x) => x,
            Err(_) => return None, // NOERRN
        };
        if moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, lp, s).is_err() {
            return None;
        }
        if moveins(v.mcx, &mut v.nfa, &mut v.cm, false, rp, s2).is_err() {
            return None;
        }
        v.tm(atom_id).begin = Some(s);
        v.tm(atom_id).end = Some(s2);
    } else {
        // disconnect atom from lp/rp (removes the EMPTY arcs made above)
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        if delsub(&mut v.nfa, &mut v.cm, false, lp, abegin).is_err() {
            return None;
        }
        if delsub(&mut v.nfa, &mut v.cm, false, aend, rp).is_err() {
            return None;
        }
    }

    // general-purpose state skeleton: make s here; s2 below if needed
    s = match newstate(v.mcx, &mut v.nfa) {
        Ok(x) => x,
        Err(_) => return None, // NOERRN
    };
    if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, lp, s).is_err() {
        return None; // NOERRN
    }

    // break remaining subRE into x{...} and what follows
    let aflags = v.t(atom_id).flags;
    let t_node = subre(v, b'.', COMBINE(qprefer, aflags), Some(lp), Some(rp))?; // NOERRN
    v.tm(t_node).child = Some(atom_id);

    // Convert top node to a concatenation of the prefix and remaining (t).
    debug_assert!(v.t(top).op == b'=' && v.t(top).child.is_none());
    let topflags = v.t(top).flags;
    let topbegin = v.t(top).begin;
    let topchild = subre(v, b'=', topflags, topbegin, Some(lp))?; // NOERRN
    v.tm(top).child = Some(topchild);
    v.tm(top).op = b'.';
    v.tm(topchild).sibling = Some(t_node);

    // if it's a backref, now is the time to replicate the subNFA
    if atomtype == BACKREF {
        debug_assert_eq!(
            v.nfa.state_arena[v.t(atom_id).begin.unwrap().0 as usize].nouts,
            1
        ); // just the EMPTY
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        if delsub(&mut v.nfa, &mut v.cm, false, abegin, aend).is_err() {
            return None;
        }
        debug_assert!(v.subs[subno as usize].is_some());

        let sub = v.subs[subno as usize].unwrap();
        let sub_begin = v.t(sub).begin.unwrap();
        let sub_end = v.t(sub).end.unwrap();
        if dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, sub_begin, sub_end, abegin, aend).is_err() {
            return None;
        }
        if v.NISERR() {
            return None; // NOERRN
        }

        // The backref node's NFA should not enforce any constraints
        if removeconstraints(v.mcx, &mut v.nfa, &mut v.cm, false, abegin, aend).is_err() {
            return None;
        }
        if v.NISERR() {
            return None; // NOERRN
        }
    }

    // It's quantifier time.
    if atomtype == BACKREF {
        // special case: backrefs have internal quantifiers
        let abegin = v.t(atom_id).begin.unwrap();
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, s, abegin).is_err() {
            return None;
        }
        // just stuff everything into atom
        let aend = v.t(atom_id).end.unwrap();
        if repeat(v, abegin, aend, m, n).is_err() {
            return None;
        }
        v.tm(atom_id).min = m as i16;
        v.tm(atom_id).max = n as i16;
        let aflags = v.t(atom_id).flags;
        v.tm(atom_id).flags |= COMBINE(qprefer, aflags);
        s2 = v.t(atom_id).end.unwrap();
    } else if m == 1
        && n == 1
        && (qprefer == 0
            || (v.t(atom_id).flags & (LONGER | SHORTER | MIXED)) == 0
            || qprefer == (v.t(atom_id).flags & (LONGER | SHORTER | MIXED)))
    {
        // no/vacuous quantifier: done
        let abegin = v.t(atom_id).begin.unwrap();
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, s, abegin).is_err() {
            return None;
        }
        s2 = v.t(atom_id).end.unwrap();
    } else if (v.t(atom_id).flags & (CAP | BACKR)) == 0 {
        // no captures nor backrefs in the atom: make a plain DFA node
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, s, abegin).is_err() {
            return None;
        }
        if repeat(v, abegin, aend, m, n).is_err() {
            return None;
        }
        let aflags = v.t(atom_id).flags;
        f = COMBINE(qprefer, aflags);
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        let tt = subre(v, b'=', f, Some(abegin), Some(aend))?; // NOERRN
        freesubre(v, Some(atom_id));
        v.tm(t_node).child = Some(tt);
        s2 = v.t(tt).end.unwrap();
    } else if m > 0 && (v.t(atom_id).flags & BACKR) == 0 {
        // turn x{m,n} into x{m-1,n-1}x, capturing parens in only the second x.
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        if dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, abegin, aend, s, abegin).is_err() {
            return None;
        }
        debug_assert!(m >= 1 && m != DUPINF && n >= 1);
        if repeat(v, s, abegin, m - 1, if n == DUPINF { n } else { n - 1 }).is_err() {
            return None;
        }
        let aflags = v.t(atom_id).flags;
        f = COMBINE(qprefer, aflags);
        let aend = v.t(atom_id).end.unwrap();
        let tt = subre(v, b'.', f, Some(s), Some(aend))?; // prefix and atom; NOERRN
        let abegin = v.t(atom_id).begin.unwrap();
        let child = subre(v, b'=', PREF(f), Some(s), Some(abegin))?; // NOERRN
        v.tm(tt).child = Some(child);
        v.tm(child).sibling = Some(atom_id);
        v.tm(t_node).child = Some(tt);
        s2 = v.t(atom_id).end.unwrap();
    } else {
        // general case: need an iteration node
        s2 = match newstate(v.mcx, &mut v.nfa) {
            Ok(x) => x,
            Err(_) => return None, // NOERRN
        };
        let aend = v.t(atom_id).end.unwrap();
        if moveouts(v.mcx, &mut v.nfa, &mut v.cm, false, aend, s2).is_err() {
            return None;
        }
        if v.NISERR() {
            return None; // NOERRN
        }
        let abegin = v.t(atom_id).begin.unwrap();
        let aend = v.t(atom_id).end.unwrap();
        if dupnfa(v.mcx, &mut v.nfa, &mut v.cm, false, abegin, aend, s, s2).is_err() {
            return None;
        }
        if repeat(v, s, s2, m, n).is_err() {
            return None;
        }
        let aflags = v.t(atom_id).flags;
        f = COMBINE(qprefer, aflags);
        let tt = subre(v, b'*', f, Some(s), Some(s2))?; // NOERRN
        v.tm(tt).min = m as i16;
        v.tm(tt).max = n as i16;
        v.tm(tt).child = Some(atom_id);
        v.tm(t_node).child = Some(tt);
    }

    // and finally, look after that postponed recursion
    // t = top->child->sibling
    let t_node = v.t(v.t(top).child.unwrap()).sibling.unwrap();
    if !(v.SEE(b'|' as i32) || v.SEE(stopper) || v.SEE(EOS)) {
        // parse the rest of the branch, insert in t->child->sibling
        let rest = parsebranch(v, stopper, type_, s2, rp, 1)?; // NOERRN
        let tchild = v.t(t_node).child.unwrap();
        v.tm(tchild).sibling = Some(rest);
        debug_assert!(v.SEE(b'|' as i32) || v.SEE(stopper) || v.SEE(EOS));

        // the promised update of the flags
        let tflags = v.t(t_node).flags;
        let restflags = v.t(rest).flags;
        v.tm(t_node).flags |= COMBINE(tflags, restflags);
        let topflags = v.t(top).flags;
        let tflags = v.t(t_node).flags;
        v.tm(top).flags |= COMBINE(topflags, tflags);

        debug_assert_eq!(v.t(t_node).capno, 0);
        debug_assert_eq!(v.t(top).capno, 0);

        // If the messy atom was first, top->child is vacuous: drop one level.
        let topchild = v.t(top).child.unwrap();
        debug_assert!(v.t(topchild).op == b'=');
        if v.t(topchild).begin == v.t(topchild).end {
            debug_assert!(!MESSY(v.t(topchild).flags));
            freesubre(v, Some(topchild));
            let tchild = v.t(t_node).child;
            v.tm(top).child = tchild;
            freesrnode(v, Some(t_node));
        } else {
            // t->child may not be messy itself but conflict with what preceded.
            let tchild = v.t(t_node).child.unwrap();
            let tchild_sib = v.t(tchild).sibling.unwrap();
            if v.t(tchild).op == b'='
                && v.t(tchild_sib).op == b'='
                && !MESSY(UP(v.t(tchild).flags | v.t(tchild_sib).flags))
            {
                v.tm(t_node).op = b'=';
                let cf = v.t(tchild).flags;
                let sf = v.t(tchild_sib).flags;
                v.tm(t_node).flags = COMBINE(cf, sf);
                freesubreandsiblings(v, Some(tchild));
                v.tm(t_node).child = None;
            }
        }
    } else {
        // Nothing left in the branch; don't need the second concat node 't'.
        if newarc(v.mcx, &mut v.nfa, &mut v.cm, false, EMPTY, 0, s2, rp).is_err() {
            return None;
        }
        let tchild = v.t(t_node).child;
        let topchild = v.t(top).child.unwrap();
        v.tm(topchild).sibling = tchild;
        let topflags = v.t(top).flags;
        let topchild_sib = v.t(topchild).sibling.unwrap();
        let sibflags = v.t(topchild_sib).flags;
        v.tm(top).flags |= COMBINE(topflags, sibflags);
        freesrnode(v, Some(t_node));

        // top->child may be vacuous: replace top with top->child->sibling.
        let topchild = v.t(top).child.unwrap();
        debug_assert!(v.t(topchild).op == b'=');
        if v.t(topchild).begin == v.t(topchild).end {
            debug_assert!(!MESSY(v.t(topchild).flags));
            let tt = v.t(topchild).sibling.unwrap();
            v.tm(topchild).sibling = None;
            freesubre(v, Some(top));
            top = tt;
        }
    }

    Some(top)
}

// =============================================================================
// tree fixups: removecaptures / numst / markst / cleanst
// =============================================================================

/// removecaptures - remove unnecessary capture subREs (only if REG_NOSUB).
/// C: `static void removecaptures(struct vars *v, struct subre *t)`.
pub fn removecaptures(v: &mut Vars, t: NodeId) {
    // If this isn't itself a backref target, clear capno and tentatively CAP.
    if (v.t(t).flags & BRUSE) == 0 {
        v.tm(t).capno = 0;
        v.tm(t).flags &= !CAP;
    }

    // recurse to children
    let mut t2 = v.t(t).child;
    while let Some(c) = t2 {
        removecaptures(v, c);
        // Propagate child CAP flag back up, if still set
        if (v.t(c).flags & CAP) != 0 {
            v.tm(t).flags |= CAP;
        }
        t2 = v.t(c).sibling;
    }

    // If t now has neither captures nor backrefs, reduce to a simple DFA node.
    if (v.t(t).flags & (CAP | BACKR)) == 0 {
        let child = v.t(t).child;
        if child.is_some() {
            freesubreandsiblings(v, child);
        }
        v.tm(t).child = None;
        v.tm(t).op = b'=';
        v.tm(t).flags &= !MIXED;
    }
}

/// numst - number tree nodes (assigning "id" indexes).
/// C: `static int numst(struct subre *t, int start)`.
pub fn numst(v: &mut Vars, t: NodeId, start: i32) -> i32 {
    let mut i = start;
    v.tm(t).id = i;
    i += 1;
    let mut t2 = v.t(t).child;
    while let Some(c) = t2 {
        i = numst(v, c, i);
        t2 = v.t(c).sibling;
    }
    i
}

/// markst - mark tree nodes as INUSE.
/// C: `static void markst(struct subre *t)`.
pub fn markst(v: &mut Vars, t: NodeId) {
    v.tm(t).flags |= INUSE;
    let mut t2 = v.t(t).child;
    while let Some(c) = t2 {
        markst(v, c);
        t2 = v.t(c).sibling;
    }
}

/// cleanst - free any tree nodes not marked INUSE.
/// C: `static void cleanst(struct vars *v)`. With the arena there is nothing to
/// physically reclaim (the un-INUSE nodes simply remain in the arena and are
/// never reached from the root); this is a no-op, matching the arena model.
pub fn cleanst(_v: &mut Vars) {}

// =============================================================================
// NFA bridge: makesearch / nfanode / nfatree
// =============================================================================

/// makesearch - turn an NFA into a search NFA (implicit prepend of `.*?`).
/// C: `static void makesearch(struct vars *v, struct nfa *nfa)`. The colormap
/// and NFA are passed explicitly so this applies to either the main NFA or a
/// transient child NFA (lookbehind lacon).
pub fn makesearch<'mcx>(
    mcx: Mcx<'mcx>,
    nfa: &mut Nfa,
    cm: &mut ColorMap,
    has_parent: bool,
) -> RegResult<()> {
    let pre = nfa.pre;

    // no loops are needed if it's anchored
    let mut anchored = true;
    let mut cur = nfa.state_arena[pre.0 as usize].outs;
    while let Some(a) = cur {
        let arc = nfa.arc_arena[a.0 as usize];
        debug_assert_eq!(arc.type_, PLAIN);
        if arc.co != nfa.bos[0] && arc.co != nfa.bos[1] {
            anchored = false;
            break;
        }
        cur = arc.outchain;
    }
    if !anchored {
        // add implicit .* in front
        rainbow(mcx, nfa, cm, has_parent, PLAIN, COLORLESS, pre, pre)?;

        // and ^* and \A* too -- not always necessary, but harmless
        let bos0 = nfa.bos[0];
        let bos1 = nfa.bos[1];
        newarc(mcx, nfa, cm, has_parent, PLAIN, bos0, pre, pre)?;
        newarc(mcx, nfa, cm, has_parent, PLAIN, bos1, pre, pre)?;

        // The pattern is still MATCHALL if it was, but max match is now infinity.
        if (nfa.flags & MATCHALL) != 0 {
            nfa.maxmatchall = DUPINF;
        }
    }

    // Split states reachable from pre AND elsewhere into progress/no-progress.
    let mut slist: Option<StateId> = None;
    let mut cur = nfa.state_arena[pre.0 as usize].outs;
    while let Some(a) = cur {
        let arc = nfa.arc_arena[a.0 as usize];
        let st = arc.to.unwrap();
        // does s have an inarc not from pre?
        let mut has_other_in = false;
        let mut b = nfa.state_arena[st.0 as usize].ins;
        while let Some(bb) = b {
            let barc = nfa.arc_arena[bb.0 as usize];
            if barc.from != Some(pre) {
                has_other_in = true;
                break;
            }
            b = barc.inchain;
        }
        // Mark states by non-None tmp; first list entry gets its own StateId.
        if has_other_in && nfa.state_arena[st.0 as usize].tmp.is_none() {
            nfa.state_arena[st.0 as usize].tmp = Some(slist.unwrap_or(st));
            slist = Some(st);
        }
        cur = arc.outchain;
    }

    // do the splits
    let mut s_opt = slist;
    while let Some(st) = s_opt {
        let s2 = newstate(mcx, nfa)?; // NOERR
        copyouts(mcx, nfa, cm, has_parent, st, s2)?; // NOERR
        let mut a_opt = nfa.state_arena[st.0 as usize].ins;
        while let Some(a) = a_opt {
            let b = nfa.arc_arena[a.0 as usize].inchain;
            let from = nfa.arc_arena[a.0 as usize].from.unwrap();
            if from != pre {
                cparc(mcx, nfa, cm, has_parent, a, from, s2)?;
                freearc(nfa, cm, has_parent, a);
            }
            a_opt = b;
        }
        let stmp = nfa.state_arena[st.0 as usize].tmp;
        let next = if stmp != Some(st) { stmp } else { None };
        nfa.state_arena[st.0 as usize].tmp = None;
        s_opt = next;
    }

    Ok(())
}

/// `latype_is_ahead(latype)` - is the lookaround type a lookahead?
/// C: `LATYPE_IS_AHEAD(latype)` == `((latype) & 0x2)`.
#[inline]
fn latype_is_ahead(latype: i32) -> bool {
    (latype & 0x2) != 0
}

/// nfanode - do one NFA for nfatree or lacons.
/// C: `static long nfanode(struct vars *v, struct subre *t, int converttosearch,
/// FILE *f)`. Here `begin`/`end` are the node's NFA endpoints, and the compacted
/// NFA is returned (the caller stows it into the node/lacon).
pub fn nfanode(
    v: &mut Vars,
    begin: StateId,
    end: StateId,
    converttosearch: bool,
) -> RegResult<(i64, Cnfa)> {
    let mut ret: i64 = 0;

    // nfa = newnfa(v, v->cm, v->nfa) -- a child NFA sharing the colormap.
    let mut nfa = newnfa(v.mcx, &mut v.cm, true)?; // NOERRZ
    let init = nfa.init;
    let final_ = nfa.final_;
    dupnfa_cross(v.mcx, &mut nfa, &mut v.nfa, &mut v.cm, begin, end, init, final_)?;
    nfa.flags = v.nfa.flags;

    let mut cnfa = Cnfa::new_empty();
    if v.NOERR() {
        // The child NFA shares the parent's colormap, so it inherits the
        // parent's BOS/EOS pseudocolors rather than allocating fresh ones
        // (C: specialcolors() follows nfa->parent).
        specialcolors(v.mcx, &mut nfa, &mut v.cm, Some((v.nfa.bos, v.nfa.eos)))?;
    }
    if v.NOERR() {
        ret = optimize(v.mcx, &mut nfa, &mut v.cm, true)?;
    }
    if converttosearch && v.NOERR() {
        makesearch(v.mcx, &mut nfa, &mut v.cm, true)?;
    }
    if v.NOERR() {
        compact(v.mcx, &nfa, &v.cm, &mut cnfa)?;
    }

    freenfa(nfa);
    Ok((ret, cnfa))
}

/// nfatree - turn a subRE subtree into a tree of compacted NFAs (children first).
/// C: `static long nfatree(struct vars *v, struct subre *t, FILE *f)`.
pub fn nfatree(v: &mut Vars, t: NodeId) -> RegResult<i64> {
    debug_assert!(v.t(t).begin.is_some());

    // children before self
    let mut t2 = v.t(t).child;
    while let Some(c) = t2 {
        let _ = nfatree(v, c)?; // (DISCARD)
        t2 = v.t(c).sibling;
    }

    let begin = v.t(t).begin.unwrap();
    let end = v.t(t).end.unwrap();
    let (ret, cnfa) = nfanode(v, begin, end, false)?;
    v.tm(t).cnfa = if cnfa.is_null() { None } else { Some(cnfa) };
    Ok(ret)
}

// =============================================================================
// pg_regcomp orchestration + freev / rfree / rstacktoodeep
// =============================================================================

/// rstacktoodeep - check for stack getting dangerously deep.
///
/// C consults the backend's `stack_is_too_deep()`. During a bare compile that
/// GUC-dependent facility is unavailable, so the port uses the explicit
/// `parse_depth` counter (checked in `subre`/`DepthGuard`); this callback always
/// reports "not too deep" and only fills the [`Fns`] vtable slot.
pub fn rstacktoodeep() -> i32 {
    0
}

/// The static [`Fns`] table stored into `re.re_fns` (C `static const struct fns
/// functions`). The C `free` destructor is subsumed by Rust ownership.
const FUNCTIONS: Fns = Fns {
    stack_too_deep: rstacktoodeep,
};

/// Construct a fresh, zero-state [`ColorMap`] ready for `initcm`. C's colormap
/// starts as a zeroed `struct colormap` that `initcm` then populates; the
/// foundation `initcm` clears/repopulates the `Vec`s, so an all-empty value is a
/// valid starting point.
fn empty_colormap() -> ColorMap {
    ColorMap {
        cd: Vec::new(),
        max: 0,
        free: 0,
        locolormap: Vec::new(),
        classbits: [0; NUM_CCLASSES as usize],
        cmranges: Vec::new(),
        hicolormap: Vec::new(),
        hiarrayrows: 0,
        hiarraycols: 0,
    }
}

/// pg_regcomp - compile regular expression.
///
/// On failure the owned working data is simply dropped (no resources remain
/// allocated, so `pg_regfree()` need not be applied). C: `int pg_regcomp(regex_t
/// *re, const chr *string, size_t len, int flags, Oid collation)`; here the
/// compiled [`RegexT`] is returned (the public seam boxes it type-erased into a
/// [`regex::RegexCompiled`] carrier); the non-`REG_OKAY` arm is the
/// `RegResult` error.
///
/// `mcx` is the allocation context charged for all compile-time allocations.
pub fn pg_regcomp<'mcx>(
    mcx: Mcx<'mcx>,
    pattern: &[PgWChar],
    cflags: i32,
    collation: Oid,
) -> RegResult<RegexT> {
    // sanity checks
    if (cflags & REG_QUOTE) != 0 && (cflags & (REG_ADVANCED | REG_EXPANDED | REG_NEWLINE)) != 0 {
        return Err(RegError(REG_INVARG));
    }
    if (cflags & REG_EXTENDED) == 0 && (cflags & REG_ADVF) != 0 {
        return Err(RegError(REG_INVARG));
    }

    // Initialize locale-dependent support (C: pg_set_regex_collation(mcx, collation)).
    // A collation-setup failure is mapped to REG_INVARG at this boundary.
    if pg_set_regex_collation(mcx, collation).is_err() {
        return Err(RegError(REG_INVARG));
    }

    // Build the colormap + NFA (C: initcm(v, &cm); v->nfa = newnfa(v, &cm, NULL)).
    let mut cm = empty_colormap();
    crate::regex_foundation::initcm(mcx, &mut cm)?;
    let nfa = newnfa(mcx, &mut cm, false)?;

    // initial setup of vars: v->subs = v->sub10; nsubs = 10; all NULL.
    let mut subs: Vec<Option<NodeId>> = Vec::new();
    subs.resize(10, None);

    let mut v = Vars {
        mcx,
        pattern: pattern.to_vec(),
        cursor: 0,
        cflags,
        info: 0,
        err: None,
        lasttype: 0,
        nexttype: 0,
        nextvalue: 0,
        lexcon: 0,
        nsubexp: 0,
        subs,
        nlcolor: COLORLESS,
        wordchrs: None,
        nfa,
        cm,
        cv: None,
        cv2: None,
        tree_nodes: Vec::new(),
        tree: None,
        ntree: 0,
        lacons: Vec::new(),
        nlacons: 0,
        spaceused: 0,
        parse_depth: 0,
    };

    // re bookkeeping
    let mut re = RegexT {
        re_magic: REMAGIC,
        re_nsub: 0,
        re_info: 0,
        re_csize: core::mem::size_of::<chr>() as i32,
        re_collation: collation,
        re_guts: None,
        re_fns: Some(FUNCTIONS),
    };

    // CNOERR() helper: if error latched, bail with the latched code.
    macro_rules! cnoerr {
        () => {
            if v.NISERR() {
                return Err(v.err.unwrap());
            }
        };
    }

    // set up a reasonably-sized transient cvec for getcvec usage
    v.cv = Some(newcvec(mcx, 100, 20)?);

    // parsing
    v.lexstart(); // also handles prefixes
    if (v.cflags & REG_NLSTOP) != 0 || (v.cflags & REG_NLANCH) != 0 {
        // assign newline a unique color
        v.nlcolor = subcolor(v.mcx, &mut v.cm, newline())?;
        okcolors(v.mcx, &mut v.nfa, &mut v.cm, false)?;
    }
    cnoerr!();

    let init = v.nfa.init;
    let final_ = v.nfa.final_;
    v.tree = parse(&mut v, EOS, PLAIN, init, final_);
    debug_assert!(v.SEE(EOS)); // even if error; ISERR() => SEE(EOS)
    cnoerr!();
    debug_assert!(v.tree.is_some());
    let tree = v.tree.unwrap();

    // finish setup of nfa and its subre tree
    specialcolors(v.mcx, &mut v.nfa, &mut v.cm, None)?;
    cnoerr!();

    if (v.cflags & REG_NOSUB) != 0 {
        removecaptures(&mut v, tree);
    }
    v.ntree = numst(&mut v, tree, 1);
    markst(&mut v, tree);
    cleanst(&mut v);

    // build compacted NFAs for tree and lacons
    let bits = nfatree(&mut v, tree)?;
    re.re_info |= bits;
    cnoerr!();
    debug_assert!(v.nlacons == 0 || !v.lacons.is_empty());
    for i in 1..v.nlacons {
        let lasub_begin = v.lacons[i as usize].begin.unwrap();
        let lasub_end = v.lacons[i as usize].end.unwrap();
        let latype = v.lacons[i as usize].latype as i32;
        // Prepend .* to pattern if it's a lookbehind LACON.
        let converttosearch = !latype_is_ahead(latype);
        let (_, cnfa) = nfanode(&mut v, lasub_begin, lasub_end, converttosearch)?;
        v.lacons[i as usize].cnfa = if cnfa.is_null() { None } else { Some(cnfa) };
    }
    cnoerr!();
    if (v.t(tree).flags & SHORTER) != 0 {
        v.NOTE(REG_USHORTEST);
    }

    // build compacted NFAs for tree, lacons, fast search; sacrifice main NFA.
    optimize(v.mcx, &mut v.nfa, &mut v.cm, false)?;
    cnoerr!();
    makesearch(v.mcx, &mut v.nfa, &mut v.cm, false)?;
    cnoerr!();
    let mut search = Cnfa::new_empty();
    compact(v.mcx, &v.nfa, &v.cm, &mut search)?;
    cnoerr!();

    // looks okay, package it up.
    // Fold the accumulated NOTE() bits into re.re_info.
    re.re_info |= v.info;
    re.re_nsub = v.nsubexp as usize;

    // Move owned working data into the Guts.
    let cmap = core::mem::replace(&mut v.cm, empty_colormap());
    let tree_nodes = core::mem::take(&mut v.tree_nodes);
    let lacons = core::mem::take(&mut v.lacons);
    let g = Guts {
        magic: GUTSMAGIC,
        cflags: v.cflags,
        info: re.re_info,
        nsub: re.re_nsub,
        tree: v.tree.take(),
        tree_nodes,
        search,
        ntree: v.ntree,
        cmap,
        // C regcomp.c: g->compare = (v->cflags & REG_ICASE) ? casecmp : cmp.
        compare: if (v.cflags & REG_ICASE) != 0 {
            Some(casecmp as crate::regguts::GutsCompare)
        } else {
            Some(cmp as crate::regguts::GutsCompare)
        },
        lacons,
        nlacons: v.nlacons,
    };

    re.re_guts = Some(Box::new(g));

    debug_assert!(v.err.is_none());
    Ok(re)
}
