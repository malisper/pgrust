//! The regex engine's INTERNAL type contract.
//!
//! Rust analogues of the internal data structures in
//! `src/include/regex/regguts.h` (plus the character type from `regcustom.h`
//! and the NFA/token type codes from `src/backend/regex/regcomp.c`).
//!
//! This is the foundational type layer (data types, constants, and trivial
//! inline helpers only). The C version uses raw pointer chains (intrusive
//! linked lists, freelists, bulk batch allocators). The idiomatic port replaces
//! those with arena vectors plus typed index handles (`StateId`, `ArcId`,
//! `NodeId`) and `Option<...>` for nullable links; the batch allocators
//! (`arcbatch`, `statebatch`) are subsumed by the arena vectors and are not
//! modeled.
//!
//! # Memory model
//!
//! The C engine `MALLOC`/`REALLOC`/`FREE`s every growable array out of its own
//! transient compile context. The faithful port charges those against `mcx`,
//! and the allocating functions take an `Mcx<'mcx>` and return `RegResult<_>`
//! (true-OOM -> `REG_ESPACE`). The arena/array fields are plain
//! [`alloc::vec::Vec`]; the allocating entry points carry `Mcx`.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::PgWChar;

use crate::regex_consts::NUM_CCLASSES;

// =============================================================================
// character type  (regcustom.h)
// =============================================================================

/// `typedef pg_wchar chr;` -- the internal character type.
pub type chr = PgWChar; // = u32

/// `typedef unsigned uchr;` -- unsigned type that will hold a chr.
pub type uchr = u32;

/// bits in a chr (regcustom.h: CHRBITS; must not use sizeof).
pub const CHRBITS: i32 = 32;
/// smallest chr value (regcustom.h: CHR_MIN).
pub const CHR_MIN: chr = 0x0000_0000;
/// largest chr value (regcustom.h: CHR_MAX). Note: 0x7ffffffe, NOT 0x7fffffff,
/// so that CHR_MAX-CHR_MIN+1 fits in an int and CHR_MAX+1 fits in a chr.
pub const CHR_MAX: chr = 0x7fff_fffe;

/// cutoff between "simple" and "complicated" color-map processing
/// (regcustom.h: MAX_SIMPLE_CHR). Suitable value for Unicode.
pub const MAX_SIMPLE_CHR: chr = 0x7FF;

/// `CHR_IS_IN_RANGE(c)`: is a chr value in [CHR_MIN, CHR_MAX]?
///
/// Matches the C macro, which only tests the upper bound because chr is
/// unsigned and CHR_MIN is zero.
#[inline]
pub const fn CHR_IS_IN_RANGE(c: chr) -> bool {
    c <= CHR_MAX
}

// =============================================================================
// colors  (regguts.h)
// =============================================================================

/// `typedef short color;` -- colors of characters. MUST stay signed (i16):
/// COLORLESS and RAINBOW are negative.
pub type color = i16;

/// max color (must fit in `color`).
pub const MAX_COLOR: color = 32767;
/// impossible color.
pub const COLORLESS: color = -1;
/// represents all colors except pseudocolors.
pub const RAINBOW: color = -2;
/// default color, parent of all others. (Various code knows WHITE is zero.)
pub const WHITE: color = 0;
/// value of colordesc "sub" when no open subcolor (== COLORLESS).
pub const NOSUB: color = COLORLESS;

// =============================================================================
// known character classes  (regguts.h: enum char_classes)
// =============================================================================

/// `enum char_classes` -- known character classes, in declaration order.
/// Values 0..13 correspond to `classbits[]`/cvec `cclasscode` indices.
#[repr(u32)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum char_classes {
    CC_ALNUM = 0,
    CC_ALPHA,
    CC_ASCII,
    CC_BLANK,
    CC_CNTRL,
    CC_DIGIT,
    CC_GRAPH,
    CC_LOWER,
    CC_PRINT,
    CC_PUNCT,
    CC_SPACE,
    CC_UPPER,
    CC_XDIGIT,
    CC_WORD,
}

// =============================================================================
// compacted-NFA arc  (regguts.h: struct carc)
// =============================================================================

/// `struct carc` -- one arc of a compacted NFA.
///
/// `co == COLORLESS` is the list terminator. Plain arcs store a transition
/// color in `co`; LACON arcs store `lookaround_number + ncolors` in `co`, so a
/// LACON arc is distinguished by `co >= ncolors`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Carc {
    /// COLORLESS is list terminator
    pub co: color,
    /// next-state number
    pub to: i32,
}

impl Carc {
    /// True iff this is a LACON arc, per `co >= cnfa.ncolors`.
    #[inline]
    pub fn is_lacon(&self, ncolors: i32) -> bool {
        (self.co as i32) >= ncolors
    }
}

// =============================================================================
// compacted NFA  (regguts.h: struct cnfa)
// =============================================================================

/// cnfa flag: uses lookaround constraints (regguts.h: HASLACONS = 01).
pub const HASLACONS: i32 = 1;
/// cnfa flag: matches all strings of a range of lengths (regguts.h: MATCHALL = 02).
pub const MATCHALL: i32 = 2;
/// nfa flag: contains CANTMATCH arcs (regguts.h: HASCANTMATCH = 04).
/// Appears in nfa structs' flags, never in cnfas.
pub const HASCANTMATCH: i32 = 4;

/// per-state flag bit: no-progress state (regguts.h: CNFA_NOPROGRESS = 01).
pub const CNFA_NOPROGRESS: u8 = 1;

/// `struct cnfa` -- compacted NFA.
///
/// In C, `states` is a vector of pointers into a single arc array, each list
/// terminated by a dummy carc with `co == COLORLESS`. Here `states[n]` is the
/// half-open range of indices into `arcs` for state `n`'s out-arc list, and
/// `stflags[n]` is the per-state flags byte for state `n`.
pub struct Cnfa {
    /// number of states
    pub nstates: i32,
    /// number of colors (max color in use + 1)
    pub ncolors: i32,
    /// bitmask of HASLACONS/MATCHALL/HASCANTMATCH
    pub flags: i32,
    /// setup state number
    pub pre: i32,
    /// teardown state number
    pub post: i32,
    /// colors, if any, assigned to BOS and BOL
    pub bos: [color; 2],
    /// colors, if any, assigned to EOS and EOL
    pub eos: [color; 2],
    /// vector of per-state flags bytes (CNFA_NOPROGRESS)
    pub stflags: Vec<u8>,
    /// per-state half-open `start..end` ranges into `arcs` (replaces
    /// `struct carc **states`)
    pub states: Vec<core::ops::Range<usize>>,
    /// the area for the out-arc lists
    pub arcs: Vec<Carc>,
    /// MATCHALL only (else -1): min number of chrs to match
    pub minmatchall: i32,
    /// MATCHALL only (else -1): max number of chrs to match, or DUPINF
    pub maxmatchall: i32,
}

impl Cnfa {
    /// An empty/NULLCNFA cnfa: `nstates == 0`, no arcs/states/stflags.
    ///
    /// Equivalent to C's `ZAPCNFA` result (a zeroed `struct cnfa`). Callers of
    /// `compact` start from this value and pass `&mut` to be filled in; it is
    /// also the post-condition of `freecnfa`. `NULLCNFA(cnfa)` corresponds to
    /// `cnfa.nstates == 0`.
    #[inline]
    pub fn new_empty() -> Self {
        Cnfa {
            nstates: 0,
            ncolors: 0,
            flags: 0,
            pre: 0,
            post: 0,
            bos: [COLORLESS, COLORLESS],
            eos: [COLORLESS, COLORLESS],
            stflags: Vec::new(),
            states: Vec::new(),
            arcs: Vec::new(),
            minmatchall: 0,
            maxmatchall: 0,
        }
    }

    /// `NULLCNFA(cnfa)` -- is this the empty cnfa (`nstates == 0`)?
    #[inline]
    pub fn is_null(&self) -> bool {
        self.nstates == 0
    }
}

// =============================================================================
// colormap  (regguts.h: struct colordesc, colormaprange, colormap, cvec)
// =============================================================================

/// colordesc flag: currently free (regguts.h: FREECOL = 01).
pub const FREECOL: i32 = 1;
/// colordesc flag: pseudocolor, no real chars (regguts.h: PSEUDO = 02).
pub const PSEUDO: i32 = 2;
/// colordesc flag: temporary marker used in some functions (regguts.h: COLMARK = 04).
pub const COLMARK: i32 = 4;

/// `struct colordesc` -- per-color data for the compile-time color machinery.
///
/// In C `arcs` is `struct arc *` (chain of all arcs of this color). Here it is
/// the head of the color's arc chain, indexed into the NFA arc arena.
#[derive(Copy, Clone, Debug)]
pub struct ColorDesc {
    /// number of simple chars of this color
    pub nschrs: i32,
    /// number of upper map entries of this color
    pub nuchrs: i32,
    /// open subcolor, if any; or free-chain ptr (NOSUB if none)
    pub sub: color,
    /// chain of all arcs of this color (head of color arc chain)
    pub arcs: Option<ArcId>,
    /// simple char first assigned to this color
    pub firstchr: chr,
    /// bitmask of FREECOL/PSEUDO/COLMARK
    pub flags: i32,
}

/// `typedef struct colormaprange` -- a range of high chrs mapping to one row.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ColorMapRange {
    /// range represents cmin..cmax inclusive
    pub cmin: chr,
    pub cmax: chr,
    /// row index in hicolormap array (>= 1)
    pub rownum: i32,
}

/// One inclusive `[from, to]` character range stored in a [`Cvec`]'s `ranges`.
///
/// In C the cvec's ranges live in a flat `chr ranges[]` array as adjacent
/// `from`/`to` pairs (`ranges[n*2]`, `ranges[n*2+1]`). The idiomatic cvec stores
/// them as this two-field `Copy` struct so the pairing is explicit.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CvecRange {
    /// first character of the range
    pub from: chr,
    /// last character of the range (inclusive)
    pub to: chr,
}

/// `struct colormap` -- the color map.
///
/// Holds both compile-time data and the chr->color mapping used at compile and
/// run time. `cd` is the array of colordescs; `locolormap` maps chrs <=
/// MAX_SIMPLE_CHR; the `cmranges` + `hicolormap` 2-D array map chrs above that.
/// The inline `cdspace[NINLINECDS]` malloc-avoidance buffer in C has no analogue
/// here, and `magic` (CMMAGIC) / the `struct vars *v` error-reporting
/// back-pointer are omitted (errors thread via `RegResult`).
pub struct ColorMap {
    /// array of colordescs (C: `struct colordesc *cd`)
    pub cd: Vec<ColorDesc>,
    /// highest color number currently in use (C: `size_t max`)
    pub max: usize,
    /// beginning of free chain (if non-0)
    pub free: color,
    /// simple array indexed by chr code, for chrs <= MAX_SIMPLE_CHR
    pub locolormap: Vec<color>,
    /// class-bit column contributions; classbits[k]==0 if class k unused
    pub classbits: [i32; NUM_CCLASSES as usize],
    /// ranges of high chrs (C: `colormaprange *cmranges`)
    pub cmranges: Vec<ColorMapRange>,
    /// 2-D array of color entries (row-major, hiarrayrows x hiarraycols)
    pub hicolormap: Vec<color>,
    /// number of array rows in use
    pub hiarrayrows: i32,
    /// number of array columns (2^N)
    pub hiarraycols: i32,
}

/// `struct cvec` -- representation of a set of characters.
///
/// `chrs[]` are individual code points; `ranges[]` are min..max inclusive pairs.
/// For a locale-specific class (e.g. `[[:alpha:]]`) `cclasscode` is the class's
/// code rather than -1. In C the `chrspace`/`rangespace` capacities are tracked
/// separately; here the `Vec` lengths/capacities serve that role.
pub struct Cvec {
    /// vector of chrs
    pub chrs: Vec<chr>,
    /// vector of [`CvecRange`] pairs (min..max inclusive)
    pub ranges: Vec<CvecRange>,
    /// value of `enum char_classes`, or -1
    pub cclasscode: i32,
}

// =============================================================================
// NFA arena: index handles  (replace C raw pointers)
// =============================================================================

/// state arena index. `FREESTATE` (-1) in C becomes `Option<StateId>::None`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct StateId(pub u32);

/// arc arena index.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArcId(pub u32);

// =============================================================================
// NFA arc/state type codes  (src/backend/regex/regcomp.c)
//
// These are token type codes, some also used as NFA arc types. In C they are
// character literals; we reproduce the exact ASCII code values.
// =============================================================================

pub const EMPTY: i32 = b'n' as i32; /* no token present */
pub const EOS: i32 = b'e' as i32; /* end of string */
pub const PLAIN: i32 = b'p' as i32; /* ordinary character */
pub const DIGIT: i32 = b'd' as i32; /* digit (in bound) */
pub const BACKREF: i32 = b'b' as i32; /* back reference */
pub const COLLEL: i32 = b'I' as i32; /* start of [. */
pub const ECLASS: i32 = b'E' as i32; /* start of [= */
pub const CCLASS: i32 = b'C' as i32; /* start of [: */
pub const END: i32 = b'X' as i32; /* end of [. [= [: */
pub const CCLASSS: i32 = b's' as i32; /* char class shorthand escape */
pub const CCLASSC: i32 = b'c' as i32; /* complement char class shorthand escape */
pub const RANGE: i32 = b'R' as i32; /* - within [] which might be range delim. */
pub const LACON: i32 = b'L' as i32; /* lookaround constraint subRE */
pub const AHEAD: i32 = b'a' as i32; /* color-lookahead arc */
pub const BEHIND: i32 = b'r' as i32; /* color-lookbehind arc */
pub const WBDRY: i32 = b'w' as i32; /* word boundary constraint */
pub const NWBDRY: i32 = b'W' as i32; /* non-word-boundary constraint */
pub const CANTMATCH: i32 = b'x' as i32; /* arc that cannot match anything */
pub const SBEGIN: i32 = b'A' as i32; /* beginning of string (even if not BOL) */
pub const SEND: i32 = b'Z' as i32; /* end of string (even if not EOL) */
/// `'^'` arc type: beginning-of-line / beginning-of-string constraint.
pub const ARC_BOS: i32 = b'^' as i32;
/// `'$'` arc type: end-of-line / end-of-string constraint.
pub const ARC_EOS: i32 = b'$' as i32;

// =============================================================================
// NFA internal representation  (regguts.h: struct arc, struct state, struct nfa)
// =============================================================================

/// `struct state` -- one NFA state.
///
/// `no == FREESTATE (-1)` marks a free state in C; here a free state lives on
/// the `Nfa::free_states` chain. The intrusive in/out arc chains are kept as
/// head links into the arc arena; `tmp`/`next`/`prev` are traversal/chain links
/// into the state arena.
#[derive(Copy, Clone, Debug)]
pub struct State {
    /// state number, zero and up; or FREESTATE
    pub no: i32,
    /// marks special states
    pub flag: u8,
    /// number of inarcs
    pub nins: i32,
    /// number of outarcs
    pub nouts: i32,
    /// chain of inarcs (head)
    pub ins: Option<ArcId>,
    /// chain of outarcs (head)
    pub outs: Option<ArcId>,
    /// temporary for traversal algorithms
    pub tmp: Option<StateId>,
    /// chain for traversing all live states (also the free-state chain)
    pub next: Option<StateId>,
    /// back-link in chain of all live states
    pub prev: Option<StateId>,
}

/// `struct arc` -- one NFA arc.
///
/// `type == 0` means free in C. The six chain links replace C's
/// outchain/outchainRev/inchain/inchainRev/colorchain/colorchainRev raw
/// pointers (`freechain` aliases `outchain`; there is no freechainRev). The
/// colorchain links are unused when `co == RAINBOW`.
///
/// `from`/`to` are `Option<StateId>` because C `freearc()` nulls them out
/// (`victim->from = NULL; victim->to = NULL;`) when an arc is freed, per the
/// documented nullable-pointer -> `Option` mapping. They are `Some(..)` for
/// every live arc.
#[derive(Copy, Clone, Debug)]
pub struct Arc {
    /// 0 if free, else an NFA arc type code (PLAIN, EMPTY, AHEAD, ...)
    pub type_: i32,
    /// color the arc matches (possibly RAINBOW)
    pub co: color,
    /// where it's from (None on a freed arc)
    pub from: Option<StateId>,
    /// where it's to (None on a freed arc)
    pub to: Option<StateId>,
    /// link in *from's outs chain or free chain
    pub outchain: Option<ArcId>,
    /// back-link in *from's outs chain
    pub outchainRev: Option<ArcId>,
    /// link in *to's ins chain
    pub inchain: Option<ArcId>,
    /// back-link in *to's ins chain
    pub inchainRev: Option<ArcId>,
    /// link in color's arc chain (unused when co == RAINBOW)
    pub colorchain: Option<ArcId>,
    /// back-link in color's arc chain (unused when co == RAINBOW)
    pub colorchainRev: Option<ArcId>,
}

/// `struct nfa` -- the working NFA.
///
/// Arena-based: `state_arena`/`arc_arena` own all node storage and are indexed
/// by `StateId`/`ArcId`.
///
/// The C `struct nfa` field named `states` is NOT the arena: it is the *head* of
/// the intrusive doubly-linked chain of live states (paired with `slast`, the
/// tail). That head is modeled here by `live_states`; the arena is given a
/// distinct name to avoid the C-name collision. The live chain is
/// order-significant: algorithms (newstate, freestate, ~20 traversals) walk it
/// head-to-tail via `State::next`, and the arena order is NOT a substitute
/// because the arena also holds free (interspersed) states.
///
/// `free_states`/`free_arcs` head the respective freelists (C:
/// `freestates`/`freearcs`). The C batch allocators (`lastsb`/`lastab`/
/// `lastsbused`/`lastabused` and `statebatch`/`arcbatch`) are subsumed by the
/// arena vectors and not modeled. The colormap back-pointer (`struct colormap
/// *cm`), error-reporting vars (`struct vars *v`), and `parent` link are not
/// stored on this struct: `cm` is threaded in as a `&mut ColorMap` argument and
/// the parent link as a `has_parent: bool` flag (colorchain bookkeeping runs
/// only when `!has_parent`, matching C's `nfa->parent == NULL`). `final` is
/// named `final_` (reserved word).
pub struct Nfa {
    /// state arena (NOT the C `nfa.states` field; see `live_states`).
    pub state_arena: Vec<State>,
    /// arc arena (NOT a C `nfa` field; arcs are batch-allocated in C).
    pub arc_arena: Vec<Arc>,
    /// head of the chain of live states (C: `struct state *states`). Paired
    /// with `slast` (tail); traversed forward via `State::next`.
    pub live_states: Option<StateId>,
    /// chain of free states (C: `freestates`)
    pub free_states: Option<StateId>,
    /// chain of free arcs (C: `freearcs`)
    pub free_arcs: Option<ArcId>,
    /// pre-initial state
    pub pre: StateId,
    /// initial state
    pub init: StateId,
    /// final state (`final` is a reserved word)
    pub final_: StateId,
    /// post-final state
    pub post: StateId,
    /// for numbering states
    pub nstates: i32,
    /// tail of the chain of live states (C: `slast`)
    pub slast: Option<StateId>,
    /// colors, if any, assigned to BOS and BOL
    pub bos: [color; 2],
    /// colors, if any, assigned to EOS and EOL
    pub eos: [color; 2],
    /// flags to pass forward to cNFA
    pub flags: i32,
    /// min number of chrs to match, if matchall
    pub minmatchall: i32,
    /// max number of chrs to match, or DUPINF
    pub maxmatchall: i32,
    /// transient compile-time space charged against `REG_MAX_COMPILE_SPACE`.
    ///
    /// NOTE: in C this field lives in `struct vars` (regcomp.c), not `struct
    /// nfa`; all accounting goes through `(*(*nfa).v).spaceused`. It is relocated
    /// onto the NFA here because the NFA is the entity being charged. This meter
    /// bounds NFA *complexity* (its overflow is `REG_ETOOBIG`); it is separate
    /// from the `mcx` byte counter (whose `try_reserve` failure is the true-OOM
    /// `REG_ESPACE` path), exactly as C separates `v->spaceused` from a failing
    /// `MALLOC`.
    pub spaceused: usize,
}

// =============================================================================
// subexpression tree  (regguts.h: struct subre)
// =============================================================================

/// subre flag: prefers longer match (regguts.h: LONGER = 01).
pub const LONGER: u8 = 1;
/// subre flag: prefers shorter match (regguts.h: SHORTER = 02).
pub const SHORTER: u8 = 2;
/// subre flag: mixed preference below (regguts.h: MIXED = 04).
pub const MIXED: u8 = 4;
/// subre flag: capturing parens here or below (regguts.h: CAP = 010).
pub const CAP: u8 = 8;
/// subre flag: back reference here or below (regguts.h: BACKR = 020).
pub const BACKR: u8 = 16;
/// subre flag: is referenced by a back reference (regguts.h: BRUSE = 040).
pub const BRUSE: u8 = 32;
/// subre flag: in use in final tree (regguts.h: INUSE = 0100).
pub const INUSE: u8 = 64;

/// tree-node arena index (for the subexpression-tree arena / `Subre::child` etc.).
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

/// `struct subre` -- one node of the subexpression tree.
///
/// `op` is one of '=' 'b' '(' '.' '|' '*' (see regguts.h for meanings). The
/// raw-pointer links (child/sibling/chain to other subres, begin/end to NFA
/// states) become arena handles; the embedded `struct cnfa cnfa` becomes an
/// `Option<Cnfa>` (present only when this node has a compacted NFA).
///
/// REDESIGN NOTE re `cnfa`: C embeds `struct cnfa` by value (always present) and
/// uses the sentinel `NULLCNFA` (`cnfa.nstates == 0`) for "no cnfa". This port
/// shifts that sentinel to the `Option`: downstream phases must map
/// `NULLCNFA`/`nstates == 0` -> `cnfa.is_none()`, `ZAPCNFA` -> `cnfa = None`,
/// and lacon-vector empty slots -> `cnfa: None`.
pub struct Subre {
    /// op type code: one of '=' 'b' '(' '.' '|' '*'
    pub op: u8,
    /// bitmask of LONGER/SHORTER/MIXED/CAP/BACKR/BRUSE/INUSE
    pub flags: u8,
    /// LATYPE code, if lookaround constraint
    pub latype: u8,
    /// ID of subre (1..ntree-1)
    pub id: i32,
    /// if capture node, subno to capture into
    pub capno: i32,
    /// if backref node, subno it refers to
    pub backno: i32,
    /// min repetitions for iteration or backref
    pub min: i16,
    /// max repetitions for iteration or backref
    pub max: i16,
    /// first child, if any (also freelist chain)
    pub child: Option<NodeId>,
    /// next child of same parent, if any
    pub sibling: Option<NodeId>,
    /// outarcs from here...
    pub begin: Option<StateId>,
    /// ...ending in inarcs here
    pub end: Option<StateId>,
    /// compacted NFA, if any
    pub cnfa: Option<Cnfa>,
    /// for bookkeeping and error cleanup
    pub chain: Option<NodeId>,
}

// =============================================================================
// function-pointer vtable  (regguts.h: struct fns)
// =============================================================================

/// `int (*stack_too_deep)(void)` -- stack-depth check. Plain Rust `fn` pointer.
///
/// The C `void (*free)(regex_t *)` destructor field has no idiomatic analogue:
/// the owned regex is dropped by Rust ownership (its `Guts` is freed when the
/// regex is dropped), so only the stack-depth callback remains.
pub type FnsStackTooDeep = fn() -> i32;

/// `struct fns` -- table of function pointers for generic manipulation.
///
/// In C the regex_t's `re_fns` points to one of these. Idiomatically the
/// `free` destructor field is dropped (Rust ownership frees the guts), so only
/// the `stack_too_deep` callback survives. The field is a plain Rust `fn`
/// pointer using the default Rust ABI because dispatch here is Rust-to-Rust.
#[derive(Copy, Clone)]
pub struct Fns {
    pub stack_too_deep: FnsStackTooDeep,
}

/// `g->compare`: `int (*compare)(const chr *, const chr *, size_t)` -- the
/// collation comparison callback used during execution. Plain Rust `fn` pointer.
/// The ported comparators take chr slices rather than raw pointers, so this is
/// the safe slice-based signature they share. The `usize` length is exact
/// (matches C's `size_t len`); callers pass slices at least `len` long.
pub type GutsCompare = fn(&[chr], &[chr], usize) -> i32;

// =============================================================================
// the insides of a regex_t  (regguts.h: struct guts)
// =============================================================================

/// `struct guts` -- the hidden innards of a regex_t (behind `re_guts`).
///
/// The C `struct subre *tree` and `struct subre *lacons` raw pointers become an
/// arena (`tree_nodes`) plus a root handle (`tree`) and a `lacons` vector. The
/// `g->compare` collation callback is included as `Option<GutsCompare>`; it may
/// be left `None` until the locale phase wires it up.
pub struct Guts {
    /// GUTSMAGIC
    pub magic: i32,
    /// copy of compile flags
    pub cflags: i32,
    /// copy of re_info
    pub info: i64,
    /// copy of re_nsub
    pub nsub: usize,
    /// root of the subexpression tree (index into `tree_nodes`)
    pub tree: Option<NodeId>,
    /// arena backing the subexpression tree
    pub tree_nodes: Vec<Subre>,
    /// compacted NFA for fast preliminary search
    pub search: Cnfa,
    /// number of subre's, plus one
    pub ntree: i32,
    /// the color map
    pub cmap: ColorMap,
    /// collation comparison callback (C: `g->compare`); may be None until wired
    pub compare: Option<GutsCompare>,
    /// lookaround-constraint vector (slots 1..nlacons-1 are used)
    pub lacons: Vec<Subre>,
    /// size of lacons[]
    pub nlacons: i32,
}

// =============================================================================
// the public regex_t  (regex.h: typedef struct ... regex_t)
// =============================================================================

/// `regex_t` -- the public compiled-regex handle (regex.h).
///
/// C hides the engine state behind `char *re_guts`/`char *re_fns` void
/// pointers; here `guts`/`fns` hold the real owned types (opacity is inherited
/// from the engine's compile/exec split, not introduced -- the consumer still
/// only reads `re_nsub`). The compiled regex crosses the public seam boundary
/// as the owned `RegexT` value itself, carried type-erased inside
/// [`regex::RegexCompiled`] and downcast back in the export-free-error
/// family's seam adapters.
pub struct RegexT {
    /// `re_magic` -- REMAGIC
    pub re_magic: i32,
    /// `re_nsub` -- number of subexpressions
    pub re_nsub: usize,
    /// `re_info` -- bitmask of REG_U* info flags
    pub re_info: i64,
    /// `re_csize` -- sizeof(character)
    pub re_csize: i32,
    /// `re_collation` -- collation that defines LC_CTYPE behavior
    pub re_collation: ::types_core::Oid,
    /// `re_guts` -- the engine-owned innards (C: `char *re_guts`)
    pub re_guts: Option<alloc::boxed::Box<Guts>>,
    /// `re_fns` -- the manipulation-function table (C: `char *re_fns`)
    pub re_fns: Option<Fns>,
}
