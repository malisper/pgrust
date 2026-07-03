
extern crate alloc;

use alloc::vec::Vec;

use ::types_core::PgWChar;

use crate::regex_consts::NUM_CCLASSES;


pub type chr = PgWChar; // = u32

pub type uchr = u32;

pub const CHRBITS: i32 = 32;
pub const CHR_MIN: chr = 0x0000_0000;
pub const CHR_MAX: chr = 0x7fff_fffe;

pub const MAX_SIMPLE_CHR: chr = 0x7FF;

#[inline]
pub const fn CHR_IS_IN_RANGE(c: chr) -> bool {
    c <= CHR_MAX
}


pub type color = i16;

pub const MAX_COLOR: color = 32767;
pub const COLORLESS: color = -1;
pub const RAINBOW: color = -2;
pub const WHITE: color = 0;
pub const NOSUB: color = COLORLESS;


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


#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Carc {
    pub co: color,
    pub to: i32,
}

impl Carc {
    #[inline]
    pub fn is_lacon(&self, ncolors: i32) -> bool {
        (self.co as i32) >= ncolors
    }
}


pub const HASLACONS: i32 = 1;
pub const MATCHALL: i32 = 2;
pub const HASCANTMATCH: i32 = 4;

pub const CNFA_NOPROGRESS: u8 = 1;

pub struct Cnfa {
    pub nstates: i32,
    pub ncolors: i32,
    pub flags: i32,
    pub pre: i32,
    pub post: i32,
    pub bos: [color; 2],
    pub eos: [color; 2],
    pub stflags: Vec<u8>,
    pub states: Vec<core::ops::Range<usize>>,
    pub arcs: Vec<Carc>,
    pub minmatchall: i32,
    pub maxmatchall: i32,
}

impl Cnfa {
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

    #[inline]
    pub fn is_null(&self) -> bool {
        self.nstates == 0
    }
}


pub const FREECOL: i32 = 1;
pub const PSEUDO: i32 = 2;
pub const COLMARK: i32 = 4;

#[derive(Copy, Clone, Debug)]
pub struct ColorDesc {
    pub nschrs: i32,
    pub nuchrs: i32,
    pub sub: color,
    pub arcs: Option<ArcId>,
    pub firstchr: chr,
    pub flags: i32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ColorMapRange {
    pub cmin: chr,
    pub cmax: chr,
    pub rownum: i32,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct CvecRange {
    pub from: chr,
    pub to: chr,
}

pub struct ColorMap {
    pub cd: Vec<ColorDesc>,
    pub max: usize,
    pub free: color,
    pub locolormap: Vec<color>,
    pub classbits: [i32; NUM_CCLASSES as usize],
    pub cmranges: Vec<ColorMapRange>,
    pub hicolormap: Vec<color>,
    pub hiarrayrows: i32,
    pub hiarraycols: i32,
}

pub struct Cvec {
    pub chrs: Vec<chr>,
    pub ranges: Vec<CvecRange>,
    pub cclasscode: i32,
}


#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct StateId(pub u32);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ArcId(pub u32);


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
pub const ARC_BOS: i32 = b'^' as i32;
pub const ARC_EOS: i32 = b'$' as i32;


#[derive(Copy, Clone, Debug)]
pub struct State {
    pub no: i32,
    pub flag: u8,
    pub nins: i32,
    pub nouts: i32,
    pub ins: Option<ArcId>,
    pub outs: Option<ArcId>,
    pub tmp: Option<StateId>,
    pub next: Option<StateId>,
    pub prev: Option<StateId>,
}

#[derive(Copy, Clone, Debug)]
pub struct Arc {
    pub type_: i32,
    pub co: color,
    pub from: Option<StateId>,
    pub to: Option<StateId>,
    pub outchain: Option<ArcId>,
    pub outchainRev: Option<ArcId>,
    pub inchain: Option<ArcId>,
    pub inchainRev: Option<ArcId>,
    pub colorchain: Option<ArcId>,
    pub colorchainRev: Option<ArcId>,
}

pub struct Nfa {
    pub state_arena: Vec<State>,
    pub arc_arena: Vec<Arc>,
    pub live_states: Option<StateId>,
    pub free_states: Option<StateId>,
    pub free_arcs: Option<ArcId>,
    pub pre: StateId,
    pub init: StateId,
    pub final_: StateId,
    pub post: StateId,
    pub nstates: i32,
    pub slast: Option<StateId>,
    pub bos: [color; 2],
    pub eos: [color; 2],
    pub flags: i32,
    pub minmatchall: i32,
    pub maxmatchall: i32,
    pub spaceused: usize,
}


pub const LONGER: u8 = 1;
pub const SHORTER: u8 = 2;
pub const MIXED: u8 = 4;
pub const CAP: u8 = 8;
pub const BACKR: u8 = 16;
pub const BRUSE: u8 = 32;
pub const INUSE: u8 = 64;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

pub struct Subre {
    pub op: u8,
    pub flags: u8,
    pub latype: u8,
    pub id: i32,
    pub capno: i32,
    pub backno: i32,
    pub min: i16,
    pub max: i16,
    pub child: Option<NodeId>,
    pub sibling: Option<NodeId>,
    pub begin: Option<StateId>,
    pub end: Option<StateId>,
    pub cnfa: Option<Cnfa>,
    pub chain: Option<NodeId>,
}


pub type FnsStackTooDeep = fn() -> i32;

#[derive(Copy, Clone)]
pub struct Fns {
    pub stack_too_deep: FnsStackTooDeep,
}

pub type GutsCompare = fn(&[chr], &[chr], usize) -> i32;


pub struct Guts {
    pub magic: i32,
    pub cflags: i32,
    pub info: i64,
    pub nsub: usize,
    pub tree: Option<NodeId>,
    pub tree_nodes: Vec<Subre>,
    pub search: Cnfa,
    pub ntree: i32,
    pub cmap: ColorMap,
    pub compare: Option<GutsCompare>,
    pub lacons: Vec<Subre>,
    pub nlacons: i32,
}


pub struct RegexT {
    pub re_magic: i32,
    pub re_nsub: usize,
    pub re_info: i64,
    pub re_csize: i32,
    pub re_collation: ::types_core::Oid,
    pub re_guts: Option<alloc::boxed::Box<Guts>>,
    pub re_fns: Option<Fns>,
}
