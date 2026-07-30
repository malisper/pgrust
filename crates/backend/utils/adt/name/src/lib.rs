#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod builtins;
#[cfg(test)]
mod tests;

use datum::Bytea;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::{Oid, C_COLLATION_OID, NAMEDATALEN};
use types_error::{ErrorLocation, PgResult, ERRCODE_NAME_TOO_LONG, ERROR};
use types_tuple::NameData;

pub(crate) const NAMELEN: usize = NAMEDATALEN as usize;

pub fn namein(s: &[u8]) -> NameData {
    let mut len = s.len();
    if len >= NAMELEN {
        len = mbutils_seams::pg_mbcliplen::call(s, len as i32, NAMEDATALEN - 1) as usize;
    }
    let mut result = NameData::default();
    result.data[..len].copy_from_slice(&s[..len]);
    result
}

pub fn nameout_into(s: &NameData, buf: &mut Vec<u8>) {
    buf.clear();
    buf.extend_from_slice(s.name_str());
    buf.push(0);
}

pub fn nameout<'mcx>(mcx: Mcx<'mcx>, s: &NameData) -> PgResult<PgVec<'mcx, u8>> {
    mcx::slice_in(mcx, s.name_str())
}

pub fn namerecv(mcx: Mcx<'_>, buf: &mut StringInfo<'_>) -> PgResult<NameData> {
    let rawbytes = buf.len().saturating_sub(buf.cursor);
    let str = pqformat::pq_getmsgtext(mcx, buf, rawbytes)?;
    if str.len() >= NAMELEN {
        elog::ereport(ERROR)
            .errcode(ERRCODE_NAME_TOO_LONG)
            .errmsg("identifier too long")
            .errdetail(format!(
                "Identifier must be less than {NAMEDATALEN} characters."
            ))
            .finish(loc("namerecv"))?;
    }
    let mut result = NameData::default();
    result.data[..str.len()].copy_from_slice(&str);
    Ok(result)
}

pub fn namesend<'mcx>(mcx: Mcx<'mcx>, s: &NameData) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendtext(&mut buf, s.name_str())?;
    Ok(pqformat::pq_endtypsend(buf))
}

// strncmp semantics, word-at-a-time like glibc's (2.9x-instr byte loop
// refused). Returns the RAW byte difference at the first mismatch like libc
// strncmp — C's btnamecmp exposes that magnitude at the SQL level (fnconf
// batch-1 cmp-magnitude family; C 18.3: btnamecmp('a','c') → -2).
fn strncmp_name(a: &[u8; NAMELEN], b: &[u8; NAMELEN]) -> i32 {
    const LO: u64 = 0x0101_0101_0101_0101;
    const HI: u64 = 0x8080_8080_8080_8080;
    let mut i = 0;
    while i < NAMELEN {
        let xa = u64::from_le_bytes(a[i..i + 8].try_into().unwrap());
        let xb = u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
        let diff = xa ^ xb;
        let zero = xa.wrapping_sub(LO) & !xa & HI;
        if diff == 0 {
            if zero != 0 {
                return 0;
            }
            i += 8;
            continue;
        }
        let di = (diff.trailing_zeros() >> 3) as usize;
        if zero != 0 && ((zero.trailing_zeros() >> 3) as usize) < di {
            return 0;
        }
        return a[i + di] as i32 - b[i + di] as i32;
    }
    0
}

fn namecmp(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<i32> {
    // Fast path for the common system-catalog case.
    if collid == C_COLLATION_OID {
        return Ok(strncmp_name(&arg1.data, &arg2.data));
    }
    varlena::varstr_cmp(arg1.name_str(), arg2.name_str(), collid)
}

// name-vs-text comparisons (varlena.c). The eq/ne pair short-circuits on
// length under C collation exactly as C does.
pub fn btnametextcmp(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<i32> {
    varlena::varstr_cmp(arg1.name_str(), arg2, collid)
}

pub fn bttextnamecmp(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<i32> {
    varlena::varstr_cmp(arg1, arg2.name_str(), collid)
}

pub fn nameeqtext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    if collid == C_COLLATION_OID {
        return Ok(arg1.name_str() == arg2);
    }
    Ok(btnametextcmp(arg1, arg2, collid)? == 0)
}

pub fn namenetext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(!nameeqtext(arg1, arg2, collid)?)
}

pub fn namelttext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(arg1, arg2, collid)? < 0)
}

pub fn nameletext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(arg1, arg2, collid)? <= 0)
}

pub fn namegttext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(arg1, arg2, collid)? > 0)
}

pub fn namegetext(arg1: &NameData, arg2: &[u8], collid: Oid) -> PgResult<bool> {
    Ok(btnametextcmp(arg1, arg2, collid)? >= 0)
}

pub fn texteqname(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    if collid == C_COLLATION_OID {
        return Ok(arg1 == arg2.name_str());
    }
    Ok(bttextnamecmp(arg1, arg2, collid)? == 0)
}

pub fn textnename(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(!texteqname(arg1, arg2, collid)?)
}

pub fn textltname(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(arg1, arg2, collid)? < 0)
}

pub fn textlename(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(arg1, arg2, collid)? <= 0)
}

pub fn textgtname(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(arg1, arg2, collid)? > 0)
}

pub fn textgename(arg1: &[u8], arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(bttextnamecmp(arg1, arg2, collid)? >= 0)
}

pub fn nameeq(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? == 0)
}

pub fn namene(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? != 0)
}

pub fn namelt(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? < 0)
}

pub fn namele(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? <= 0)
}

pub fn namegt(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? > 0)
}

pub fn namege(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<bool> {
    Ok(namecmp(arg1, arg2, collid)? >= 0)
}

pub fn btnamecmp(arg1: &NameData, arg2: &NameData, collid: Oid) -> PgResult<i32> {
    namecmp(arg1, arg2, collid)
}

pub fn namestrcpy(name: &mut NameData, str: &str) {
    name.namestrcpy(str);
}

pub fn namestrcmp(name: Option<&NameData>, str: Option<&[u8]>) -> i32 {
    match (name, str) {
        (None, None) => 0,
        (None, Some(_)) => -1,
        (Some(_), None) => 1,
        (Some(name), Some(str)) => {
            let mut b = [0u8; NAMELEN];
            let n = str.len().min(NAMELEN);
            b[..n].copy_from_slice(&str[..n]);
            strncmp_name(&name.data, &b)
        }
    }
}

pub fn current_user(mcx: Mcx<'_>) -> PgResult<NameData> {
    let name = miscinit::GetUserNameFromId(mcx, miscinit::GetUserId(), false)?
        .expect("GetUserNameFromId(noerr=false) returns a name or errors");
    Ok(namein(name.as_bytes()))
}

pub fn session_user(mcx: Mcx<'_>) -> PgResult<NameData> {
    let name =
        miscinit::GetUserNameFromId(mcx, miscinit::GetSessionUserId(), false)?
            .expect("GetUserNameFromId(noerr=false) returns a name or errors");
    Ok(namein(name.as_bytes()))
}

pub fn current_schema(mcx: Mcx<'_>) -> PgResult<Option<NameData>> {
    let search_path = namespace_seams::fetch_search_path::call(mcx, false)?;
    let Some(&first) = search_path.first() else {
        return Ok(None);
    };
    match lsyscache::misc::get_namespace_name(mcx, first)? {
        // None: recently-deleted namespace?
        None => Ok(None),
        Some(nspname) => Ok(Some(namein(nspname.as_bytes()))),
    }
}

// C's current_schemas up to construct_array_builtin; the fc wrapper builds
// the array image.
pub fn current_schemas<'mcx>(
    mcx: Mcx<'mcx>,
    include_implicit: bool,
) -> PgResult<PgVec<'mcx, NameData>> {
    let search_path = namespace_seams::fetch_search_path::call(mcx, include_implicit)?;
    let mut names: PgVec<'mcx, NameData> = mcx::vec_with_capacity_in(mcx, search_path.len())?;
    for &oid in search_path.iter() {
        // Watch out for deleted namespace.
        if let Some(nspname) = lsyscache::misc::get_namespace_name(mcx, oid)? {
            names.push(namein(nspname.as_bytes()));
        }
    }
    Ok(names)
}

pub fn nameconcatoid(nam: &NameData, oid: Oid) -> NameData {
    let mut suffix = [0u8; 20];
    let suflen = {
        use std::io::Write;
        let mut cur = &mut suffix[..];
        write!(cur, "_{oid}").unwrap();
        20 - cur.len()
    };
    let name_str = nam.name_str();
    let mut namlen = name_str.len();
    if namlen + suflen >= NAMELEN {
        namlen = mbutils_seams::pg_mbcliplen::call(
            name_str,
            namlen as i32,
            NAMEDATALEN - 1 - suflen as i32,
        ) as usize;
    }
    let mut result = NameData::default();
    result.data[..namlen].copy_from_slice(&name_str[..namlen]);
    result.data[namlen..namlen + suflen].copy_from_slice(&suffix[..suflen]);
    result
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}
