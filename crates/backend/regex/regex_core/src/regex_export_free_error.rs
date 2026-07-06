
extern crate alloc;

use alloc::format;
use alloc::rc::Rc;
use alloc::string::{String, ToString};

use ::mcx::{slice_in, Mcx, MemoryContext};
use ::types_core::PgWChar;
use ::types_error::PgResult;
use ::regex::{
    RegMatch, RegcompResult, RegexCompiled, RegexFailure, RegexecResult, RegprefixResult,
};

use crate::regex_consts::{
    REG_ATOI, REG_BADBR, REG_BADOPT, REG_BADPAT, REG_BADRPT, REG_ASSERT, REG_ECOLLATE,
    REG_ECOLORS, REG_ECTYPE, REG_EBRACE, REG_EBRACK, REG_EESCAPE, REG_EPAREN, REG_ERANGE,
    REG_ESPACE, REG_ESUBREG, REG_ETOOBIG, REG_EXACT, REG_INVARG, REG_ITOA, REG_MIXED,
    REG_NOMATCH, REG_OKAY, REG_PREFIX, REMAGIC,
};
use crate::regguts::{chr, RegexT, COLORLESS, CHR_MIN, MAX_SIMPLE_CHR, PSEUDO};


fn regex_of(re: &RegexCompiled) -> &RegexT {
    re.engine
        .downcast_ref::<RegexT>()
        .expect("RegexCompiled.engine is not a RegexT")
}

pub fn seam_pg_regcomp(
    pattern: &[PgWChar],
    cflags: i32,
    collation: ::types_core::Oid,
) -> PgResult<RegcompResult> {
    let cx = MemoryContext::new("RegexpCompileContext");
    crate::regex_locale::pg_set_regex_collation(cx.mcx(), collation)?;
    match crate::regex_compile::pg_regcomp(cx.mcx(), pattern, cflags, collation) {
        Ok(re) => {
            let re_nsub = re.re_nsub;
            let engine: Rc<dyn core::any::Any> = Rc::new(re);
            Ok(RegcompResult::Compiled(RegexCompiled { engine, re_nsub }))
        }
        Err(e) => Ok(RegcompResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
    }
}

pub fn seam_pg_regexec(
    re: &RegexCompiled,
    data: &[PgWChar],
    search_start: i32,
    pmatch: &mut [RegMatch],
) -> PgResult<RegexecResult> {
    let re = regex_of(re);
    let guts = re
        .re_guts
        .as_ref()
        .expect("pg_regexec: compiled regex has no guts");
    let res = crate::regex_exec::pg_regexec(guts, data, search_start, pmatch, 0);

    match res {
        Ok(true) => Ok(RegexecResult::Matched),
        Ok(false) => Ok(RegexecResult::NoMatch),
        Err(e) if e.code() == REG_NOMATCH => Ok(RegexecResult::NoMatch),
        Err(e) => Ok(RegexecResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
    }
}

pub fn seam_pg_regprefix<'mcx>(
    mcx: Mcx<'mcx>,
    re: &RegexCompiled,
) -> PgResult<RegprefixResult<'mcx>> {
    let re = regex_of(re);
    let guts = re
        .re_guts
        .as_ref()
        .expect("pg_regprefix: compiled regex has no guts");
    let res = crate::regex_exec::pg_regprefix(mcx, guts);

    match res {
        Ok(pr) => match pr.code {
            REG_PREFIX => {
                let v = slice_in(mcx, &pr.prefix)?;
                Ok(RegprefixResult::Prefix(v))
            }
            REG_EXACT => {
                let v = slice_in(mcx, &pr.prefix)?;
                Ok(RegprefixResult::Exact(v))
            }
            REG_NOMATCH => Ok(RegprefixResult::NoMatch),
            code => Ok(RegprefixResult::Failed(RegexFailure {
                message: pg_regerror(code),
            })),
        },
        Err(e) if e.code() == REG_NOMATCH => Ok(RegprefixResult::NoMatch),
        Err(e) => Ok(RegprefixResult::Failed(RegexFailure {
            message: pg_regerror(e.code()),
        })),
    }
}

pub fn seam_pg_regfree(re: RegexCompiled) {
    pg_regfree(re);
}


pub fn pg_regfree(re: RegexCompiled) {
    drop(re);
}


struct Rerr {
    code: i32,
    name: &'static str,
    explain: &'static str,
}

static RERRS: &[Rerr] = &[
    Rerr { code: REG_OKAY, name: "REG_OKAY", explain: "no errors detected" },
    Rerr { code: REG_NOMATCH, name: "REG_NOMATCH", explain: "failed to match" },
    Rerr { code: REG_BADPAT, name: "REG_BADPAT", explain: "invalid regexp (reg version 0.8)" },
    Rerr { code: REG_ECOLLATE, name: "REG_ECOLLATE", explain: "invalid collating element" },
    Rerr { code: REG_ECTYPE, name: "REG_ECTYPE", explain: "invalid character class" },
    Rerr { code: REG_EESCAPE, name: "REG_EESCAPE", explain: "invalid escape \\ sequence" },
    Rerr { code: REG_ESUBREG, name: "REG_ESUBREG", explain: "invalid backreference number" },
    Rerr { code: REG_EBRACK, name: "REG_EBRACK", explain: "brackets [] not balanced" },
    Rerr { code: REG_EPAREN, name: "REG_EPAREN", explain: "parentheses () not balanced" },
    Rerr { code: REG_EBRACE, name: "REG_EBRACE", explain: "braces {} not balanced" },
    Rerr { code: REG_BADBR, name: "REG_BADBR", explain: "invalid repetition count(s)" },
    Rerr { code: REG_ERANGE, name: "REG_ERANGE", explain: "invalid character range" },
    Rerr { code: REG_ESPACE, name: "REG_ESPACE", explain: "out of memory" },
    Rerr { code: REG_BADRPT, name: "REG_BADRPT", explain: "quantifier operand invalid" },
    Rerr { code: REG_ASSERT, name: "REG_ASSERT", explain: "\"cannot happen\" -- you found a bug" },
    Rerr { code: REG_INVARG, name: "REG_INVARG", explain: "invalid argument to regex function" },
    Rerr { code: REG_MIXED, name: "REG_MIXED", explain: "character widths of regex and string differ" },
    Rerr { code: REG_BADOPT, name: "REG_BADOPT", explain: "invalid embedded option" },
    Rerr { code: REG_ETOOBIG, name: "REG_ETOOBIG", explain: "regular expression is too complex" },
    Rerr { code: REG_ECOLORS, name: "REG_ECOLORS", explain: "too many colors" },
];

pub fn pg_regerror(errcode: i32) -> String {
    let msg: String = match errcode {
        REG_ATOI => {
            "-1".to_string()
        }
        REG_ITOA => {
            match RERRS.iter().find(|r| r.code == 0) {
                Some(r) => r.name.to_string(),
                None => "REG_0".to_string(),
            }
        }
        _ => match RERRS.iter().find(|r| r.code == errcode) {
            Some(r) => r.explain.to_string(),
            None => format!("*** unknown regex error code 0x{:x} ***", errcode),
        },
    };
    msg
}


#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct RegexArc {
    pub co: i32,
    pub to: i32,
}

#[inline]
fn guts_of(re: &RegexT) -> &crate::regguts::Guts {
    debug_assert!(re.re_magic == REMAGIC);
    re.re_guts
        .as_ref()
        .expect("regexport: compiled regex has no guts")
}

pub fn pg_reg_getnumstates(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.nstates
}

pub fn pg_reg_getinitialstate(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.pre
}

pub fn pg_reg_getfinalstate(re: &RegexT) -> i32 {
    let cnfa = &guts_of(re).search;
    cnfa.post
}

fn traverse_lacons(
    cnfa: &crate::regguts::Cnfa,
    st: i32,
    arcs_count: &mut i32,
    arcs: &mut [RegexArc],
) {
    stack_depth::check_stack_depth()
        .expect("traverse_lacons: stack too deep");

    let range = cnfa.states[st as usize].clone();
    for idx in range {
        let ca = cnfa.arcs[idx];
        if ca.co == COLORLESS {
            break;
        }
        if (ca.co as i32) < cnfa.ncolors {
            let ndx = *arcs_count;
            *arcs_count += 1;
            if ndx < arcs.len() as i32 {
                arcs[ndx as usize].co = ca.co as i32;
                arcs[ndx as usize].to = ca.to;
            }
        } else {
            debug_assert!(ca.to != cnfa.post);
            traverse_lacons(cnfa, ca.to, arcs_count, arcs);
        }
    }
}

pub fn pg_reg_getnumoutarcs(re: &RegexT, st: i32) -> i32 {
    let cnfa = &guts_of(re).search;

    if st < 0 || st >= cnfa.nstates {
        return 0;
    }
    let mut arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, &mut []);
    arcs_count
}

pub fn pg_reg_getoutarcs(re: &RegexT, st: i32, arcs: &mut [RegexArc]) {
    let cnfa = &guts_of(re).search;

    if st < 0 || st >= cnfa.nstates || arcs.is_empty() {
        return;
    }
    let mut arcs_count = 0;
    traverse_lacons(cnfa, st, &mut arcs_count, arcs);
}

pub fn pg_reg_getnumcolors(re: &RegexT) -> i32 {
    let cm = &guts_of(re).cmap;
    cm.max as i32 + 1
}

pub fn pg_reg_colorisbegin(re: &RegexT, co: i32) -> bool {
    let cnfa = &guts_of(re).search;
    co == cnfa.bos[0] as i32 || co == cnfa.bos[1] as i32
}

pub fn pg_reg_colorisend(re: &RegexT, co: i32) -> bool {
    let cnfa = &guts_of(re).search;
    co == cnfa.eos[0] as i32 || co == cnfa.eos[1] as i32
}

pub fn pg_reg_getnumcharacters(re: &RegexT, co: i32) -> i32 {
    let cm = &guts_of(re).cmap;

    if co <= 0 || co as usize > cm.max {
        return -1;
    }
    if cm.cd[co as usize].flags & PSEUDO != 0 {
        return -1;
    }
    if cm.cd[co as usize].nuchrs != 0 {
        return -1;
    }
    cm.cd[co as usize].nschrs
}

pub fn pg_reg_getcharacters(re: &RegexT, co: i32, chars: &mut [PgWChar]) {
    let cm = &guts_of(re).cmap;

    if co <= 0 || co as usize > cm.max || chars.is_empty() {
        return;
    }
    if cm.cd[co as usize].flags & PSEUDO != 0 {
        return;
    }

    let mut chars_len = chars.len();
    let mut out = 0usize;
    let mut c: chr = CHR_MIN;
    loop {
        if cm.locolormap[(c - CHR_MIN) as usize] as i32 == co {
            chars[out] = c;
            out += 1;
            chars_len -= 1;
            if chars_len == 0 {
                break;
            }
        }
        if c == MAX_SIMPLE_CHR {
            break;
        }
        c += 1;
    }
}
