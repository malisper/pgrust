#![no_std]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod canonical;
pub mod ported;
#[cfg(test)]
mod tests;

use ::datum::Datum;
use ::fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData, TRACK_FUNC_ALL};
use ::types_core::{primitive::InvalidOid, Oid};
use ::types_error::PgResult;

pub use ::fmgr::{
    direct_function_call1_coll, direct_function_call2_coll, direct_function_call3_coll,
    function_call0_coll, function_call1_coll, function_call2_coll, function_call3_coll,
    function_call4_coll, function_call5_coll, function_call6_coll, function_call7_coll,
    function_call8_coll, function_call9_coll,
};
pub use canonical::{CANONICAL, CANONICAL_LAST_BUILTIN_OID};

pub fn init_seams() {
    fmgr_seams::fmgr_info::set(fmgr_info);
}

/// C: `InvalidOidBuiltinMapping` (fmgrtab.h).
pub const INVALID_OID_BUILTIN_MAPPING: u16 = u16::MAX;

pub const FMGR_NBUILTINS: usize = CANONICAL.len();
pub const FMGR_LAST_BUILTIN_OID: Oid = CANONICAL_LAST_BUILTIN_OID;
pub const FMGR_OID_INDEX_SIZE: usize = FMGR_LAST_BUILTIN_OID as usize + 1;

/// C: `fmgr_builtin_oid_index[]` — dense OID -> table-row map, `N == last+1`.
pub struct BuiltinOidIndex<const N: usize>([u16; N]);

impl<const N: usize> BuiltinOidIndex<N> {
    pub const fn build(entries: &[FmgrBuiltin]) -> Self {
        assert!(entries.len() < INVALID_OID_BUILTIN_MAPPING as usize);
        let mut map = [INVALID_OID_BUILTIN_MAPPING; N];
        let mut i = 0;
        let mut prev = 0u32;
        while i < entries.len() {
            let oid = entries[i].foid;
            assert!(i == 0 || oid > prev, "entries must be strictly OID-ascending");
            assert!((oid as usize) < N, "entry OID exceeds index span");
            prev = oid;
            map[oid as usize] = i as u16;
            i += 1;
        }
        Self(map)
    }

    /// C: `fmgr_isbuiltin` — bounds test + one u16 load + one row borrow.
    #[inline]
    pub fn lookup<'a>(&self, entries: &'a [FmgrBuiltin], id: Oid) -> Option<&'a FmgrBuiltin> {
        if id as usize >= N {
            return None;
        }
        let i = self.0[id as usize];
        if i == INVALID_OID_BUILTIN_MAPPING {
            return None;
        }
        // SAFETY: `build` wrote only indices < entries.len() for this table.
        Some(unsafe { entries.get_unchecked(i as usize) })
    }
}

fn builtin_not_ported(
    flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let oid = flinfo.map_or(InvalidOid, |f| f.fn_oid);
    panic!("fmgr: builtin function {oid} is in the canonical table but not ported");
}

const fn build_builtins() -> [FmgrBuiltin; FMGR_NBUILTINS] {
    let mut t = [FmgrBuiltin {
        foid: InvalidOid,
        name: "",
        nargs: 0,
        strict: false,
        retset: false,
        func: builtin_not_ported,
    }; FMGR_NBUILTINS];
    let mut i = 0;
    while i < FMGR_NBUILTINS {
        let (foid, name, nargs, strict, retset) = CANONICAL[i];
        t[i] = FmgrBuiltin {
            foid,
            name,
            nargs,
            strict,
            retset,
            func: builtin_not_ported,
        };
        i += 1;
    }
    let mut p = 0;
    let mut prev = 0u32;
    while p < ported::PORTED.len() {
        let (oid, func) = ported::PORTED[p];
        assert!(p == 0 || oid > prev, "PORTED must be strictly OID-ascending");
        prev = oid;
        let mut lo = 0;
        let mut hi = FMGR_NBUILTINS;
        let mut hit = false;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if t[mid].foid == oid {
                t[mid].func = func;
                hit = true;
                break;
            } else if t[mid].foid < oid {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        assert!(hit, "PORTED OID missing from the canonical table");
        p += 1;
    }
    t
}

const BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = build_builtins();
const OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = BuiltinOidIndex::build(&BUILTINS);

/// C: `fmgr_builtins[]`.
pub static FMGR_BUILTINS: [FmgrBuiltin; FMGR_NBUILTINS] = BUILTINS;
pub static FMGR_BUILTIN_OID_INDEX: BuiltinOidIndex<FMGR_OID_INDEX_SIZE> = OID_INDEX;

#[inline]
pub fn fmgr_isbuiltin(id: Oid) -> Option<&'static FmgrBuiltin> {
    FMGR_BUILTIN_OID_INDEX.lookup(&FMGR_BUILTINS, id)
}

/// C: `fmgr_lookupByName` — linear, validator/alias resolution only (cold).
pub fn fmgr_lookup_by_name(name: &str) -> Option<&'static FmgrBuiltin> {
    FMGR_BUILTINS.iter().find(|b| b.name == name)
}

/// C: `fmgr_internal_function` (`fmgr_internal_validator`'s lookup glue).
pub fn fmgr_internal_function(proname: &str) -> Oid {
    match fmgr_lookup_by_name(proname) {
        Some(fbp) => fbp.foid,
        None => InvalidOid,
    }
}

/// The builtin fast path's FmgrInfo fill (C: fmgr_info_cxt_security's fbp arm).
#[inline]
pub fn fmgr_info_from_builtin_into(fbp: &FmgrBuiltin, function_id: Oid, finfo: &mut FmgrInfo) {
    finfo.fn_addr = fbp.func;
    finfo.fn_nargs = fbp.nargs;
    finfo.fn_strict = fbp.strict;
    finfo.fn_retset = fbp.retset;
    finfo.fn_stats = TRACK_FUNC_ALL;
    finfo.fn_extra = None;
    finfo.fn_expr = None;
    finfo.fn_oid = function_id;
}

#[inline]
pub fn fmgr_info_from_builtin(fbp: &FmgrBuiltin, function_id: Oid) -> FmgrInfo {
    let mut finfo = FmgrInfo::unresolved();
    fmgr_info_from_builtin_into(fbp, function_id, &mut finfo);
    finfo
}

/// C: `fmgr_info`/`fmgr_info_cxt` (fn_mcxt dropped: fn_extra owns its storage).
/// Field-wise fill of the caller's carrier, like C — a by-value return spills
/// the 56B carrier through an sret and its droppy slots stop folding (bench).
/// Non-builtin legs (pg_proc syscache, secdef, C/SQL/PL languages) are not
/// ported; C's "cache lookup failed for function %u" cannot be told apart from
/// an unported leg without pg_proc, so both panic loudly.
#[inline]
pub fn fmgr_info_into(function_id: Oid, finfo: &mut FmgrInfo) -> PgResult<()> {
    match fmgr_isbuiltin(function_id) {
        Some(fbp) => {
            fmgr_info_from_builtin_into(fbp, function_id, finfo);
            Ok(())
        }
        None => non_builtin_unported(function_id),
    }
}

#[inline]
pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo> {
    match fmgr_isbuiltin(function_id) {
        Some(fbp) => Ok(fmgr_info_from_builtin(fbp, function_id)),
        None => non_builtin_unported(function_id),
    }
}

#[cold]
#[inline(never)]
fn non_builtin_unported(function_id: Oid) -> ! {
    panic!("fmgr: function {function_id} is not a builtin; pg_proc resolution not ported");
}

pub fn oid_function_call0_coll(function_id: Oid, collation: Oid) -> PgResult<Datum> {
    let mut flinfo = FmgrInfo::unresolved();
    fmgr_info_into(function_id, &mut flinfo)?;
    function_call0_coll(&mut flinfo, collation)
}

macro_rules! define_oid_calls {
    ($($oname:ident $cname:ident ($($arg:ident),+);)*) => {$(
        pub fn $oname(
            function_id: Oid,
            collation: Oid,
            $($arg: Datum,)+
        ) -> PgResult<Datum> {
            let mut flinfo = FmgrInfo::unresolved();
            fmgr_info_into(function_id, &mut flinfo)?;
            ::fmgr::$cname(&mut flinfo, collation, $($arg,)+)
        }
    )*};
}

define_oid_calls! {
    oid_function_call1_coll function_call1_coll (a1);
    oid_function_call2_coll function_call2_coll (a1, a2);
    oid_function_call3_coll function_call3_coll (a1, a2, a3);
    oid_function_call4_coll function_call4_coll (a1, a2, a3, a4);
    oid_function_call5_coll function_call5_coll (a1, a2, a3, a4, a5);
    oid_function_call6_coll function_call6_coll (a1, a2, a3, a4, a5, a6);
    oid_function_call7_coll function_call7_coll (a1, a2, a3, a4, a5, a6, a7);
    oid_function_call8_coll function_call8_coll (a1, a2, a3, a4, a5, a6, a7, a8);
    oid_function_call9_coll function_call9_coll (a1, a2, a3, a4, a5, a6, a7, a8, a9);
}
