pub mod build;
pub mod builtins;
pub mod dict_ispell;
pub mod normalize;
pub mod regis;

#[cfg(test)]
mod tests;

use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_CONFIG_FILE_ERROR, ERRCODE_INTERNAL_ERROR};

pub use regis::{rs_compile, rs_execute, rs_is_regis, Regis, RegisNodeKind};

pub const FF_COMPOUNDONLY: i32 = 0x01;
pub const FF_COMPOUNDBEGIN: i32 = 0x02;
pub const FF_COMPOUNDMIDDLE: i32 = 0x04;
pub const FF_COMPOUNDLAST: i32 = 0x08;
pub const FF_COMPOUNDFLAG: i32 = FF_COMPOUNDBEGIN | FF_COMPOUNDMIDDLE | FF_COMPOUNDLAST;
pub const FF_COMPOUNDFLAGMASK: i32 = 0x0f;
pub const FF_COMPOUNDPERMITFLAG: i32 = 0x10;
pub const FF_COMPOUNDFORBIDFLAG: i32 = 0x20;
pub const FF_CROSSPRODUCT: i32 = 0x40;

pub const FF_SUFFIX: i32 = 1;
pub const FF_PREFIX: i32 = 0;

pub const FLAGNUM_MAXSIZE: i32 = 1 << 16;

const MAX_NORM: usize = 1024;
const MAXNORMLEN: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlagMode {
    Char,
    Long,
    Num,
}

pub struct Spell<'mcx> {
    pub word: PgVec<'mcx, u8>,
    pub flag: PgVec<'mcx, u8>,
    pub affix: i32,
    pub len: i32,
}

pub enum AffixReg<'mcx> {
    Simple,
    Regis(Regis<'mcx>),
    Regex(::regex::RegexCompiled),
}

pub struct Affix<'mcx> {
    pub flag: PgVec<'mcx, u8>,
    pub type_: i32,
    pub flagflags: i32,
    pub find: PgVec<'mcx, u8>,
    pub repl: PgVec<'mcx, u8>,
    pub reg: AffixReg<'mcx>,
}

impl Affix<'_> {
    #[inline]
    fn replen(&self) -> i32 {
        self.repl.len() as i32
    }
}

#[derive(Clone, Copy)]
pub struct SpNodeData {
    pub val: u8,
    pub isword: bool,
    pub compoundflag: u32,
    pub affix: u32,
    pub node: Option<usize>,
}

impl SpNodeData {
    fn empty() -> Self {
        SpNodeData { val: 0, isword: false, compoundflag: 0, affix: 0, node: None }
    }
}

pub struct SpNode<'mcx> {
    pub data: PgVec<'mcx, SpNodeData>,
}

pub struct AffixNodeData<'mcx> {
    pub val: u8,
    pub aff: PgVec<'mcx, usize>,
    pub node: Option<usize>,
}

impl<'mcx> AffixNodeData<'mcx> {
    fn empty(mcx: Mcx<'mcx>) -> Self {
        AffixNodeData { val: 0, aff: PgVec::new_in(mcx), node: None }
    }
    #[inline]
    fn naff(&self) -> usize {
        self.aff.len()
    }
}

pub struct AffixNode<'mcx> {
    pub isvoid: bool,
    pub data: PgVec<'mcx, AffixNodeData<'mcx>>,
}

pub struct CmpdAffix<'mcx> {
    pub affix: PgVec<'mcx, u8>,
    pub len: i32,
    pub issuffix: bool,
}

#[derive(Clone)]
pub enum FlagKey {
    Str(Vec<u8>),
    Num(u32),
}

#[derive(Clone)]
pub struct CompoundAffixFlag {
    pub flag: FlagKey,
    pub flag_mode: FlagMode,
    pub value: u32,
}

pub struct IspellDict<'mcx> {
    mcx: Mcx<'mcx>,
    pub affixes: PgVec<'mcx, Affix<'mcx>>,
    pub suffix: Option<usize>,
    pub prefix: Option<usize>,
    pub af_arena: PgVec<'mcx, AffixNode<'mcx>>,
    pub dictionary: Option<usize>,
    pub sp_arena: PgVec<'mcx, SpNode<'mcx>>,
    pub affix_data: PgVec<'mcx, PgVec<'mcx, u8>>,
    pub use_flag_aliases: bool,
    pub compound_affix: PgVec<'mcx, CmpdAffix<'mcx>>,
    pub usecompound: bool,
    pub flag_mode: FlagMode,
    pub compound_affix_flags: PgVec<'mcx, CompoundAffixFlag>,
    pub spell: PgVec<'mcx, Spell<'mcx>>,
    building: bool,
}

impl<'mcx> IspellDict<'mcx> {
    pub fn new(mcx: Mcx<'mcx>) -> Self {
        IspellDict {
            mcx,
            affixes: PgVec::new_in(mcx),
            suffix: None,
            prefix: None,
            af_arena: PgVec::new_in(mcx),
            dictionary: None,
            sp_arena: PgVec::new_in(mcx),
            affix_data: PgVec::new_in(mcx),
            use_flag_aliases: false,
            compound_affix: PgVec::new_in(mcx),
            usecompound: false,
            flag_mode: FlagMode::Char,
            compound_affix_flags: PgVec::new_in(mcx),
            spell: PgVec::new_in(mcx),
            building: false,
        }
    }

    pub fn ni_start_build(&mut self) -> PgResult<()> {
        self.building = true;
        Ok(())
    }

    // C MemoryContextDelete(buildCxt): drop the construction-only scratch.
    pub fn ni_finish_build(&mut self) -> PgResult<()> {
        let mcx = self.mcx;
        self.spell = PgVec::new_in(mcx);
        self.compound_affix_flags = PgVec::new_in(mcx);
        self.building = false;
        Ok(())
    }
}

fn config_file_error(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_CONFIG_FILE_ERROR)
}

fn elog_internal(msg: String) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INTERNAL_ERROR)
}

#[inline]
fn pg_mblen(s: &[u8]) -> usize {
    if s.is_empty() {
        return 1;
    }
    ::mbutils::pg_mblen_range(s).unwrap_or(s.len() as i32) as usize
}

#[inline]
fn pg_mblen_clamped(s: &[u8]) -> usize {
    pg_mblen(s).min(s.len()).max(1)
}

#[inline]
fn t_isalpha(s: &[u8]) -> bool {
    !s.is_empty() && ::ts_locale::t_isalpha(s)
}

#[inline]
fn t_iseq(s: &[u8], x: u8) -> bool {
    !s.is_empty() && s[0] == x
}

#[inline]
fn str_tolower(mcx: Mcx<'_>, src: &[u8]) -> PgResult<Vec<u8>> {
    let folded = ::ts_locale::lowerstr(mcx, src)?;
    Ok(folded.as_slice().to_vec())
}

#[inline]
fn check_stack_depth() -> PgResult<()> {
    ::stack_depth::check_stack_depth()
}

fn bcmp(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let n = a.len().min(b.len());
    for i in 0..n {
        match a[i].cmp(&b[i]) {
            core::cmp::Ordering::Equal => {}
            ord => return ord,
        }
    }
    a.len().cmp(&b.len())
}

fn bncmp(a: &[u8], b: &[u8], n: usize) -> core::cmp::Ordering {
    let alim = a.len().min(n);
    let blim = b.len().min(n);
    bcmp(&a[..alim], &b[..blim])
}

// spell.c strbcmp: compare from the ends; the shorter string sorts first.
fn strbcmp(a: &[u8], b: &[u8]) -> core::cmp::Ordering {
    let mut ia = a.iter().rev();
    let mut ib = b.iter().rev();
    loop {
        match (ia.next(), ib.next()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            },
            (None, Some(_)) => return core::cmp::Ordering::Less,
            (Some(_), None) => return core::cmp::Ordering::Greater,
            (None, None) => return core::cmp::Ordering::Equal,
        }
    }
}

fn strbncmp(a: &[u8], b: &[u8], count: usize) -> core::cmp::Ordering {
    let mut ia = a.iter().rev();
    let mut ib = b.iter().rev();
    let mut l = count;
    while l > 0 {
        match (ia.next(), ib.next()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                core::cmp::Ordering::Equal => {}
                ord => return ord,
            },
            (None, Some(_)) => return core::cmp::Ordering::Less,
            (Some(_), None) => return core::cmp::Ordering::Greater,
            (None, None) => return core::cmp::Ordering::Equal,
        }
        l -= 1;
    }
    core::cmp::Ordering::Equal
}

fn findchar(s: &[u8], c: u8) -> Option<usize> {
    let mut off = 0usize;
    while off < s.len() {
        if t_iseq(&s[off..], c) {
            return Some(off);
        }
        off += pg_mblen(&s[off..]);
    }
    None
}

fn findchar2(s: &[u8], c1: u8, c2: u8) -> Option<usize> {
    let mut off = 0usize;
    while off < s.len() {
        if t_iseq(&s[off..], c1) || t_iseq(&s[off..], c2) {
            return Some(off);
        }
        off += pg_mblen(&s[off..]);
    }
    None
}

fn bstrchr(s: &[u8], c: u8) -> Option<usize> {
    s.iter().position(|&b| b == c)
}

fn bstrstr(hay: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }
    if needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len()).position(|w| w == needle)
}

#[inline]
fn isspace(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}
#[inline]
fn isprint(c: u8) -> bool {
    (0x20..=0x7e).contains(&c)
}
#[inline]
fn isdigit(c: u8) -> bool {
    c.is_ascii_digit()
}

fn bytes_lossy(s: &[u8]) -> String {
    String::from_utf8_lossy(s).into_owned()
}

fn new_bytes<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = PgVec::new_in(mcx);
    ::mcx::vec_append_bytes(&mut v, bytes)?;
    Ok(v)
}

#[inline]
fn reserve_one<'mcx, T>(mcx: Mcx<'mcx>, v: &mut PgVec<'mcx, T>) -> PgResult<()> {
    v.try_reserve(1)
        .map_err(|_| mcx.oom(core::mem::size_of::<T>()))
        .map_err(::core::convert::Into::into)
}

// libc strtol(s, &next, 10): (value, consumed_len, ok); overflow clamps and
// clears ok, no-digits gives consumed_len 0.
fn strtol(s: &[u8]) -> (i64, usize, bool) {
    let mut p = 0usize;
    while p < s.len() && isspace(s[p]) {
        p += 1;
    }
    let mut neg = false;
    if p < s.len() && s[p] == b'+' {
        p += 1;
    } else if p < s.len() && s[p] == b'-' {
        neg = true;
        p += 1;
    }
    let digits_start = p;
    let mut val: i64 = 0;
    let mut overflow = false;
    while p < s.len() && isdigit(s[p]) {
        let d = i64::from(s[p] - b'0');
        match val.checked_mul(10).and_then(|v| v.checked_sub(d)) {
            Some(v) => val = v,
            None => {
                overflow = true;
                val = i64::MIN;
            }
        }
        p += 1;
    }
    if p == digits_start {
        return (0, 0, false);
    }
    let val = if neg {
        if overflow {
            i64::MIN
        } else {
            val
        }
    } else if overflow {
        i64::MAX
    } else {
        -val
    };
    (val, p, !overflow)
}

fn atoi(s: &[u8]) -> i32 {
    strtol(s).0 as i32
}
