//! `contrib/pgstattuple` — tuple-level statistics (exact heap scan, btree /
//! hash / gist page walkers, GIN/hash metapage stats, VM+FSM approximation).

#![allow(non_snake_case)]

mod stat;
mod statapprox;
mod statindex;

use datum::Datum;
use mcx::Mcx;
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
};
use types_fmgr::{byref_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_storage::bufpage::SizeOfPageHeaderData;
use types_tuple::tupdesc::TupleDescData;

const LIBRARY: &str = "pgstattuple";
pub(crate) const BLCKSZ: usize = types_core::BLCKSZ;

pub(crate) fn require_superuser() -> PgResult<()> {
    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error("must be superuser to use pgstattuple functions")
                .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    Ok(())
}

pub(crate) fn index_not_valid_err(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("index \"{name}\" is not valid"))
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
    )
}

// ------------------------------------------------- raw page-header reads

pub(crate) fn r_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(b[off..off + 2].try_into().unwrap())
}

pub(crate) fn r_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

pub(crate) fn pd_lower(b: &[u8]) -> u16 {
    r_u16(b, 12)
}
pub(crate) fn pd_upper(b: &[u8]) -> u16 {
    r_u16(b, 14)
}
pub(crate) fn pd_special(b: &[u8]) -> u16 {
    r_u16(b, 16)
}

pub(crate) fn page_is_new(b: &[u8]) -> bool {
    pd_upper(b) == 0
}

pub(crate) fn page_is_empty(b: &[u8]) -> bool {
    pd_lower(b) as usize <= SizeOfPageHeaderData
}

pub(crate) fn page_special_size(b: &[u8]) -> u16 {
    (r_u16(b, 18) & 0xFF00).wrapping_sub(pd_special(b))
}

pub(crate) fn page_max_offset_number(b: &[u8]) -> usize {
    let lower = pd_lower(b) as usize;
    if lower <= SizeOfPageHeaderData {
        0
    } else {
        (lower - SizeOfPageHeaderData) / 4
    }
}

pub(crate) fn page_exact_free_space(b: &[u8]) -> usize {
    let space = pd_upper(b) as i32 - pd_lower(b) as i32;
    if space < 0 {
        0
    } else {
        space as usize
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ItemIdView {
    pub off: u16,
    pub flags: u8,
    pub len: u16,
}

impl ItemIdView {
    pub(crate) fn is_used(&self) -> bool {
        self.flags != types_storage::bufpage::LP_UNUSED as u8
    }
    pub(crate) fn is_normal(&self) -> bool {
        self.flags == types_storage::bufpage::LP_NORMAL as u8
    }
    pub(crate) fn is_dead(&self) -> bool {
        self.flags == types_storage::bufpage::LP_DEAD as u8
    }
    pub(crate) fn is_redirected(&self) -> bool {
        self.flags == types_storage::bufpage::LP_REDIRECT as u8
    }
}

pub(crate) fn page_item_id(b: &[u8], offnum: usize) -> ItemIdView {
    let pos = SizeOfPageHeaderData + (offnum - 1) * 4;
    let raw = if pos + 4 <= b.len() { r_u32(b, pos) } else { 0 };
    ItemIdView {
        off: (raw & 0x7FFF) as u16,
        flags: ((raw >> 15) & 0x3) as u8,
        len: (raw >> 17) as u16,
    }
}

pub(crate) const fn maxalign(n: usize) -> usize {
    (n + 7) & !7
}

// -------------------------------------------------------------- result glue

pub(crate) fn composite_tupdesc<'m>(
    mcx: Mcx<'m>,
    flinfo: &FmgrInfo,
) -> PgResult<TupleDescData<'m>> {
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    Ok(resolved.result_tuple_desc.expect("composite result has tupdesc"))
}

pub(crate) fn composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Datum],
    nulls: &[bool],
) -> PgResult<Datum> {
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, values, nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

/// BuildTupleFromCStrings: strings through the declared columns' input
/// functions, exactly as the C functions built their results.
pub(crate) fn cstrings_composite_result(
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'_>,
    values: &[Option<String>],
) -> PgResult<Datum> {
    let n = tupdesc.natts as usize;
    debug_assert!(values.len() >= n);
    let mut datums = vec![Datum::null(); n];
    let mut nulls = vec![false; n];
    for i in 0..n {
        match &values[i] {
            Some(s) => {
                let att = tupdesc.attr(i);
                let (infunc, typioparam) = lsyscache::getTypeInputInfo(att.atttypid)?;
                let mut flinfo = fmgr_core::fmgr_info(infunc)?;
                let cstr = std::ffi::CString::new(s.as_str())
                    .expect("cstrings_composite_result: interior NUL");
                datums[i] = types_fmgr::input_function_call(
                    &mut flinfo,
                    Some(&cstr),
                    typioparam,
                    att.atttypmod,
                    mcx,
                )?;
            }
            None => nulls[i] = true,
        }
    }
    let tup = heaptuple::heap_form_tuple(mcx, tupdesc, &datums, &nulls)?;
    byref_result(mcx, tup.image())
}

pub(crate) fn relation_is_other_temp(rel: &types_rel::RelationData<'_>) -> bool {
    rel.rd_rel.relpersistence == types_core::catalog::RELPERSISTENCE_TEMP && !rel.rd_islocaltemp
}

pub(crate) fn other_temp_tables_err() -> Box<PgError> {
    Box::new(
        PgError::error("cannot access temporary tables of other sessions")
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

pub(crate) fn other_temp_indexes_err() -> Box<PgError> {
    Box::new(
        PgError::error("cannot access temporary indexes of other sessions")
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

/// textToQualifiedNameList + makeRangeVarFromNameList + relation_openrv.
pub(crate) fn relation_open_by_text_arg<'m>(
    mcx: Mcx<'m>,
    fcinfo: &Fcinfo,
    i: usize,
    lockmode: types_rel::LOCKMODE,
) -> PgResult<types_rel::Relation<'m>> {
    // SAFETY: arg i is a non-null text (STRICT).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let rawname = String::from_utf8_lossy(v.data()).into_owned();
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    let names = varlena::split_identifier_string(mcx, &rawname, b'.', encoding)?
        .filter(|l| !l.is_empty())
        .ok_or_else(|| {
            Box::new(
                PgError::error("invalid name syntax")
                    .with_sqlstate(types_error::ERRCODE_INVALID_NAME),
            )
        })?;
    let (catalogname, schemaname, relname) = match names.as_slice() {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {rawname}"
                ))
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    relation::relation_openrv(mcx, &rv, lockmode)
}

/// Copy one MAIN-fork block out under a share lock (optionally strategy-read).
pub(crate) fn read_rel_page(
    rel: &types_rel::RelationData<'_>,
    blkno: types_core::BlockNumber,
    strategy: &types_storage::buf::BufferAccessStrategy,
) -> PgResult<Vec<u8>> {
    let buf = bufmgr::ReadBufferExtended(
        rel,
        types_core::ForkNumber::MAIN_FORKNUM,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        strategy.clone(),
    )?;
    bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
    let ptr = bufmgr::BufferGetPagePtr(buf);
    let mut page = vec![0u8; BLCKSZ];
    // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
    unsafe {
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), page.as_mut_ptr(), BLCKSZ);
    }
    bufmgr::UnlockReleaseBuffer(buf)?;
    Ok(page)
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pgstattuple" => stat::fc_pgstattuple,
        "pgstattuple_v1_5" => stat::fc_pgstattuple_v1_5,
        "pgstattuplebyid" => stat::fc_pgstattuplebyid,
        "pgstattuplebyid_v1_5" => stat::fc_pgstattuplebyid_v1_5,
        "pgstatindex" => statindex::fc_pgstatindex,
        "pgstatindex_v1_5" => statindex::fc_pgstatindex_v1_5,
        "pgstatindexbyid" => statindex::fc_pgstatindexbyid,
        "pgstatindexbyid_v1_5" => statindex::fc_pgstatindexbyid_v1_5,
        "pg_relpages" => statindex::fc_pg_relpages,
        "pg_relpages_v1_5" => statindex::fc_pg_relpages_v1_5,
        "pg_relpagesbyid" => statindex::fc_pg_relpagesbyid,
        "pg_relpagesbyid_v1_5" => statindex::fc_pg_relpagesbyid_v1_5,
        "pgstatginindex" => statindex::fc_pgstatginindex,
        "pgstatginindex_v1_5" => statindex::fc_pgstatginindex_v1_5,
        "pgstathashindex" => statindex::fc_pgstathashindex,
        "pgstattuple_approx" => statapprox::fc_pgstattuple_approx,
        "pgstattuple_approx_v1_5" => statapprox::fc_pgstattuple_approx_v1_5,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // pgstattuple.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
