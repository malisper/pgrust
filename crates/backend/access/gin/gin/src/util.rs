//! ginutil.c: GinState init, page/buffer initialization, entry compare and
//! extraction, metapage stats.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, PgVec};
use ::types_core::{Buffer, ForkNumber, InvalidBlockNumber, InvalidOid, OffsetNumber, BLCKSZ};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::PageMut;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::{
    meta_of, opclass, page_bytes_mut, page_mut, page_ref, relation_needs_wal,
    write_meta_to, write_opaque_to, RM_GIN,
};

/// initGinState (ginutil.c:101-215): one resolved support proc per slot and
/// column. Each slot is looked up on its own through the opfamily (C's
/// index_getprocid / index_getprocinfo), so any pairing of support procs an
/// opclass registers builds and scans as it does in C.
pub fn initGinState(rel: &Relation<'_>) -> PgResult<GinState> {
    let natts = rel.rd_att.natts;
    // INVARIANT: 1 <= natts <= GIN_MAX_KEY_COLS (32) == INDEX_MAX_KEYS, enforced at
    // DDL time (commands/indexcmds/src/define.rs:547 rejects 0 key columns,
    // define.rs:551-556 rejects >32 with ERRCODE_TOO_MANY_COLUMNS), as C does in
    // DefineIndex (indexcmds.c).
    if natts < 1 || natts as usize > GIN_MAX_KEY_COLS {
        panic!("gin index with {natts} key columns outside 1..={GIN_MAX_KEY_COLS}, which DDL rejects (indexcmds/src/define.rs:547,551-556)");
    }
    let mut cols = [GinColState::array_ops(GinCompareFn::Int4, true, 4); GIN_MAX_KEY_COLS];
    for i in 0..natts as usize {
        cols[i] = init_gin_col(rel, i)?;
    }
    Ok(GinState {
        natts: natts as u16,
        one_col: natts == 1,
        cols,
    })
}

/// indexam.c index_getprocinfo: elog(ERROR) XX000 "missing support function
/// %d for attribute %d of index \"%s\"".
fn missing_support_function(
    rel: &Relation<'_>,
    procnum: u16,
    i: usize,
) -> PgResult<Box<::types_error::PgError>> {
    let cx = ::mcx::MemoryContext::new("gin support proc probe");
    let relname = lsyscache::get_rel_name(cx.mcx(), rel.rd_id)?
        .map_or_else(String::new, |n| n.as_str().to_string());
    Ok(Box::new(
        ::types_error::PgError::error(format!(
            "missing support function {procnum} for attribute {} of index \"{relname}\"",
            i + 1
        ))
        .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR),
    ))
}

/// The identity fmgr dispatches a support proc by (fmgr.c
/// fmgr_info_cxt_security): a core proc by its own oid; a LANGUAGE internal
/// function by the fmgr_builtins row its prosrc names (so a
/// `CREATE FUNCTION ... LANGUAGE internal AS 'ginarrayextract'` alias IS
/// ginarrayextract); a LANGUAGE C function by its link symbol (prosrc; the
/// proname of every in-tree extension proc). Anything else (SQL / PL
/// bodies) is `Other`, callable only through fmgr.
enum ProcIdentity {
    Builtin(::types_core::Oid),
    Symbol(String),
    Other,
}

fn support_proc_identity(proc_oid: ::types_core::Oid) -> PgResult<ProcIdentity> {
    if proc_oid < ::types_core::catalog::FirstNormalObjectId {
        return Ok(ProcIdentity::Builtin(proc_oid));
    }
    if ::fmgr_seams::internal_builtin_oid::is_installed() {
        if let Some(builtin) = ::fmgr_seams::internal_builtin_oid::call(proc_oid)? {
            return Ok(ProcIdentity::Builtin(builtin));
        }
    }
    let Some(row) = ::syscache_seams::lookup_pg_proc_fmgr::call(proc_oid)? else {
        // Deleted under us: the cache lookup fmgr would make next reports it.
        return Ok(ProcIdentity::Other);
    };
    if row.prolang != C_LANGUAGE_ID {
        return Ok(ProcIdentity::Other);
    }
    let cx = ::mcx::MemoryContext::new("gin support proc probe");
    let prosrc = ::syscache_seams::lookup_pg_proc_prosrc::call(cx.mcx(), proc_oid)?;
    let identity = match prosrc {
        Some(prosrc) => ProcIdentity::Symbol(prosrc.as_str().to_string()),
        None => ProcIdentity::Other,
    };
    Ok(identity)
}

/// pg_language.dat: the `c` language's oid.
const C_LANGUAGE_ID: ::types_core::Oid = 13;

/// The "GIN operator class with <slot> support function N is not supported"
/// refusal: reached only by an internal-argument support proc outside the
/// tree's set, which no loadable function can be (C-language functions
/// resolve to in-tree symbols, SQL / PL functions cannot take `internal`).
#[cold]
fn unsupported_support_proc(slot: &str, proc_oid: ::types_core::Oid) -> Box<::types_error::PgError> {
    crate::unsupported(format!(
        "GIN operator class with {slot} support function {proc_oid} is not supported"
    ))
}

/// compareFn: a core comparator with a specialized arm, a btree_gin
/// comparator (the type's core btree cmp, or gin_numeric_cmp / gin_enum_cmp)
/// through gin_btree_seams, else fmgr (C's only mechanism). `proc_oid` is
/// GIN_COMPARE_PROC or the typcache comparator C's initGinState falls back to.
fn resolve_compare(proc_oid: ::types_core::Oid) -> PgResult<GinCompareFn> {
    let identity = support_proc_identity(proc_oid)?;
    let builtin = match identity {
        ProcIdentity::Builtin(oid) => oid,
        ProcIdentity::Symbol(ref name) => match name.as_str() {
            "gin_numeric_cmp" => return Ok(GinCompareFn::Btree(GinBtreeType::Numeric)),
            "gin_enum_cmp" => return Ok(GinCompareFn::Btree(GinBtreeType::Enum)),
            _ => InvalidOid,
        },
        ProcIdentity::Other => InvalidOid,
    };
    Ok(match builtin {
        opclass::F_BTINT2CMP => GinCompareFn::Int2,
        opclass::F_BTINT4CMP => GinCompareFn::Int4,
        opclass::F_BTINT8CMP => GinCompareFn::Int8,
        opclass::F_BTOIDCMP => GinCompareFn::Oid,
        opclass::F_BTTEXTCMP => GinCompareFn::Text,
        opclass::F_GIN_COMPARE_JSONB => GinCompareFn::Jsonb,
        opclass::F_GIN_CMP_TSLEXEME => GinCompareFn::TsLexeme,
        _ => match opclass::btree_type_of_core_cmp(builtin) {
            Some(ty) => GinCompareFn::Btree(ty),
            None => {
                // C's fmgr_info_copy site: resolve eagerly so a missing or
                // not-ported comparator raises a catchable ERROR here rather
                // than mid-compare (gist state.rs pattern; the gate covers
                // test mocks that install only fmgr_info).
                let finfo = ::fmgr_seams::fmgr_info::call(proc_oid)?;
                if ::fmgr_seams::fmgr_info_not_ported_name::is_installed() {
                    if let Some(name) = ::fmgr_seams::fmgr_info_not_ported_name::call(&finfo) {
                        return Err(crate::unsupported(format!(
                            "GIN compare support function {name} (oid {proc_oid}) is not supported"
                        )));
                    }
                }
                GinCompareFn::Fmgr(proc_oid)
            }
        },
    })
}

fn resolve_extract_value(proc_oid: ::types_core::Oid) -> PgResult<GinExtractValueFn> {
    Ok(match support_proc_identity(proc_oid)? {
        ProcIdentity::Builtin(oid) => match oid {
            opclass::F_GIN_EXTRACT_JSONB => GinExtractValueFn::Jsonb,
            opclass::F_GIN_EXTRACT_JSONB_PATH => GinExtractValueFn::JsonbPath,
            // gin_extract_tsvector and its 2-argument compatibility row
            // (tsginidx.c gin_extract_tsvector_2args forwards to it).
            opclass::F_GIN_EXTRACT_TSVECTOR | opclass::F_GIN_EXTRACT_TSVECTOR_2ARGS => {
                GinExtractValueFn::Tsvector
            }
            // ginarrayextract and ginarrayextract_2args (ginarrayproc.c:68).
            opclass::F_GINARRAYEXTRACT | opclass::F_GINARRAYEXTRACT_2ARGS => {
                GinExtractValueFn::Array
            }
            other => return Err(unsupported_support_proc("extractValue", other)),
        },
        ProcIdentity::Symbol(name) => match name.as_str() {
            "gin_extract_value_trgm" => GinExtractValueFn::Trgm,
            "gin_extract_hstore" => GinExtractValueFn::Hstore,
            other => match other
                .strip_prefix("gin_extract_value_")
                .and_then(GinBtreeType::from_type_name)
            {
                Some(ty) => GinExtractValueFn::Btree(ty),
                None => return Err(unsupported_support_proc("extractValue", proc_oid)),
            },
        },
        ProcIdentity::Other => return Err(unsupported_support_proc("extractValue", proc_oid)),
    })
}

pub(crate) fn resolve_extract_query(proc_oid: ::types_core::Oid) -> PgResult<GinExtractQueryFn> {
    Ok(match support_proc_identity(proc_oid)? {
        ProcIdentity::Builtin(oid) => match oid {
            opclass::F_GIN_EXTRACT_JSONB_QUERY => GinExtractQueryFn::Jsonb,
            opclass::F_GIN_EXTRACT_JSONB_QUERY_PATH => GinExtractQueryFn::JsonbPath,
            // gin_extract_tsquery and its compatibility rows (tsginidx.c
            // gin_extract_tsquery_5args / _oldsig forward to it).
            opclass::F_GIN_EXTRACT_TSQUERY
            | opclass::F_GIN_EXTRACT_TSQUERY_5ARGS
            | opclass::F_GIN_EXTRACT_TSQUERY_OLDSIG => GinExtractQueryFn::Tsquery,
            opclass::F_GINQUERYARRAYEXTRACT => GinExtractQueryFn::Array,
            other => return Err(unsupported_support_proc("extractQuery", other)),
        },
        ProcIdentity::Symbol(name) => match name.as_str() {
            "gin_extract_query_trgm" => GinExtractQueryFn::Trgm,
            "gin_extract_hstore_query" => GinExtractQueryFn::Hstore,
            "ginint4_queryextract" => GinExtractQueryFn::IntArray,
            other => match other
                .strip_prefix("gin_extract_query_")
                .and_then(GinBtreeType::from_type_name)
            {
                Some(ty) => GinExtractQueryFn::Btree(ty),
                None => return Err(unsupported_support_proc("extractQuery", proc_oid)),
            },
        },
        ProcIdentity::Other => return Err(unsupported_support_proc("extractQuery", proc_oid)),
    })
}

fn resolve_consistent(proc_oid: ::types_core::Oid) -> PgResult<GinConsistentFn> {
    Ok(match support_proc_identity(proc_oid)? {
        ProcIdentity::Builtin(oid) => match oid {
            opclass::F_GIN_CONSISTENT_JSONB => GinConsistentFn::Jsonb,
            opclass::F_GIN_CONSISTENT_JSONB_PATH => GinConsistentFn::JsonbPath,
            // gin_tsquery_consistent and its compatibility rows (tsginidx.c
            // gin_tsquery_consistent_6args / _oldsig forward to it).
            opclass::F_GIN_TSQUERY_CONSISTENT
            | opclass::F_GIN_TSQUERY_CONSISTENT_6ARGS
            | opclass::F_GIN_TSQUERY_CONSISTENT_OLDSIG => GinConsistentFn::Tsquery,
            opclass::F_GINARRAYCONSISTENT => GinConsistentFn::Array,
            other => return Err(unsupported_support_proc("consistent", other)),
        },
        ProcIdentity::Symbol(name) => match name.as_str() {
            "gin_trgm_consistent" => GinConsistentFn::Trgm,
            "gin_consistent_hstore" => GinConsistentFn::Hstore,
            "ginint4_consistent" => GinConsistentFn::IntArray,
            "gin_btree_consistent" => GinConsistentFn::Btree,
            _ => return Err(unsupported_support_proc("consistent", proc_oid)),
        },
        ProcIdentity::Other => return Err(unsupported_support_proc("consistent", proc_oid)),
    })
}

fn resolve_tri_consistent(proc_oid: ::types_core::Oid) -> PgResult<GinTriConsistentFn> {
    Ok(match support_proc_identity(proc_oid)? {
        ProcIdentity::Builtin(oid) => match oid {
            opclass::F_GIN_TRICONSISTENT_JSONB => GinTriConsistentFn::Jsonb,
            opclass::F_GIN_TRICONSISTENT_JSONB_PATH => GinTriConsistentFn::JsonbPath,
            opclass::F_GIN_TSQUERY_TRICONSISTENT => GinTriConsistentFn::Tsquery,
            opclass::F_GINARRAYTRICONSISTENT => GinTriConsistentFn::Array,
            other => return Err(unsupported_support_proc("triConsistent", other)),
        },
        ProcIdentity::Symbol(name) => match name.as_str() {
            "gin_trgm_triconsistent" => GinTriConsistentFn::Trgm,
            _ => return Err(unsupported_support_proc("triConsistent", proc_oid)),
        },
        ProcIdentity::Other => return Err(unsupported_support_proc("triConsistent", proc_oid)),
    })
}

fn resolve_compare_partial(proc_oid: ::types_core::Oid) -> PgResult<GinComparePartialFn> {
    Ok(match support_proc_identity(proc_oid)? {
        ProcIdentity::Builtin(oid) => match oid {
            opclass::F_GIN_CMP_PREFIX => GinComparePartialFn::TsPrefix,
            other => return Err(unsupported_support_proc("comparePartial", other)),
        },
        ProcIdentity::Symbol(name) => match name
            .strip_prefix("gin_compare_prefix_")
            .and_then(GinBtreeType::from_type_name)
        {
            Some(ty) => GinComparePartialFn::Btree(ty),
            None => return Err(unsupported_support_proc("comparePartial", proc_oid)),
        },
        ProcIdentity::Other => return Err(unsupported_support_proc("comparePartial", proc_oid)),
    })
}

fn init_gin_col(rel: &Relation<'_>, i: usize) -> PgResult<GinColState> {
    let opcintype = rel.rd_opcintype[i];
    let opfamily = rel.rd_opfamily[i];
    let proc_of = |procnum: u16| {
        lsyscache::get_opfamily_proc(opfamily, opcintype, opcintype, procnum as i16)
    };

    // ginutil.c:139-158: the compare proc, or the index key type's default
    // btree comparator from the typcache when the opclass omits it.
    let compare_proc = proc_of(GIN_COMPARE_PROC)?;
    let compare = if compare_proc != InvalidOid {
        resolve_compare(compare_proc)?
    } else {
        match rel.rd_att.attr(i).atttypid {
            // The typcache's default btree comparators of these key types
            // (btint2cmp / btint4cmp / btint8cmp / btoidcmp / bttextcmp).
            ::types_core::INT2OID => GinCompareFn::Int2,
            ::types_core::INT4OID => GinCompareFn::Int4,
            ::types_core::INT8OID => GinCompareFn::Int8,
            ::types_core::OIDOID => GinCompareFn::Oid,
            ::types_core::TEXTOID | ::types_core::VARCHAROID => GinCompareFn::Text,
            other => {
                // lookup_type_cache(atttypid, TYPECACHE_CMP_PROC_FINFO).
                let cmp_proc = ::typcache_seams::type_cache_cmp_proc::call(other)?;
                if cmp_proc == InvalidOid {
                    return Err(Box::new(
                        ::types_error::PgError::error(format!(
                            "could not identify a comparison function for type {}",
                            ::format_type::format_type_be(other)?
                        ))
                        .with_sqlstate(::types_error::ERRCODE_UNDEFINED_FUNCTION),
                    ));
                }
                resolve_compare(cmp_proc)?
            }
        }
    };

    // ginutil.c:160-165: the opclass must provide both extract procs, fetched
    // through index_getprocinfo (which errors here, not at planning, when one
    // is missing — CREATE OPERATOR CLASS ... USING gin does not require
    // FUNCTION 2/3). Proc 2 first, then proc 3, as in C.
    let extract_value_proc = proc_of(GIN_EXTRACTVALUE_PROC)?;
    if extract_value_proc == InvalidOid {
        return Err(missing_support_function(rel, GIN_EXTRACTVALUE_PROC, i)?);
    }
    let extract_value = resolve_extract_value(extract_value_proc)?;
    let extract_query_proc = proc_of(GIN_EXTRACTQUERY_PROC)?;
    if extract_query_proc == InvalidOid {
        return Err(missing_support_function(rel, GIN_EXTRACTQUERY_PROC, i)?);
    }
    let extract_query = resolve_extract_query(extract_query_proc)?;

    // ginutil.c:171-193: tri-state and/or binary consistent; at least one.
    let tri_consistent_proc = proc_of(GIN_TRICONSISTENT_PROC)?;
    let tri_consistent = if tri_consistent_proc != InvalidOid {
        Some(resolve_tri_consistent(tri_consistent_proc)?)
    } else {
        None
    };
    let consistent_proc = proc_of(GIN_CONSISTENT_PROC)?;
    let consistent = if consistent_proc != InvalidOid {
        Some(resolve_consistent(consistent_proc)?)
    } else {
        None
    };
    if consistent.is_none() && tri_consistent.is_none() {
        let cx = ::mcx::MemoryContext::new("gin consistent probe");
        let relname = lsyscache::get_rel_name(cx.mcx(), rel.rd_id)?
            .map_or_else(String::new, |n| n.as_str().to_string());
        return Err(Box::new(
            ::types_error::PgError::error(format!(
                "missing GIN support function ({GIN_CONSISTENT_PROC} or {GIN_TRICONSISTENT_PROC}) for attribute {} of index \"{relname}\"",
                i + 1
            ))
            .with_sqlstate(::types_error::ERRCODE_INTERNAL_ERROR),
        ));
    }

    // ginutil.c:196-206: partial match iff proc 5 is registered.
    let compare_partial_proc = proc_of(GIN_COMPARE_PARTIAL_PROC)?;
    let compare_partial = if compare_partial_proc != InvalidOid {
        Some(resolve_compare_partial(compare_partial_proc)?)
    } else {
        None
    };

    let attr = rel.rd_att.compact_attr(i);
    Ok(GinColState {
        compare,
        extract_value,
        extract_query,
        consistent,
        tri_consistent,
        compare_partial,
        // ginutil.c:208-214: the index collation, else the default
        // collation for collatable storage types of noncollatable keys.
        support_collation: if rel.rd_indcollation[i] != InvalidOid {
            rel.rd_indcollation[i]
        } else {
            ::types_core::catalog::DEFAULT_COLLATION_OID
        },
        can_partial_match: compare_partial.is_some(),
        key_byval: attr.attbyval,
        key_len: attr.attlen,
    })
}

// GinGetUseFastUpdate / GinGetPendingListCleanupSize
pub(crate) fn gin_use_fastupdate(rel: &Relation<'_>) -> bool {
    match rel.rd_options.as_ref().and_then(|o| o.gin()) {
        Some(o) => o.use_fast_update,
        None => GIN_DEFAULT_USE_FASTUPDATE,
    }
}

pub(crate) fn gin_pending_list_cleanup_size(rel: &Relation<'_>) -> i64 {
    match rel.rd_options.as_ref().and_then(|o| o.gin()) {
        Some(o) if o.pending_list_cleanup_size != -1 => o.pending_list_cleanup_size as i64,
        _ => guc_tables::vars::gin_pending_list_limit.read() as i64,
    }
}

/// GinPageIsRecyclable (ginvacuum.c).
pub(crate) fn gin_page_is_recyclable(buf: Buffer) -> PgResult<bool> {
    // SAFETY: caller holds at least a share lock (GinNewBuffer's conditional
    // lock, or ginvacuumcleanup's share lock).
    let page = unsafe { page_ref(buf) };
    if page.is_new() {
        return Ok(true);
    }
    let opaque = crate::page_opaque(&page);
    if crate::GinPageIsDeleted(&opaque) {
        // GinPageGetDeleteXid == pd_prune_xid; pending-list deletions leave
        // it invalid (always recyclable). A valid xid is a posting-tree page
        // deletion: recyclable once no scan can still see it. C passes
        // rel=NULL to GlobalVisCheckRemovableXid — the shared-rels horizon
        // (handle 1), the most conservative choice.
        let delete_xid = page.prune_xid();
        if delete_xid == 0 {
            return Ok(true);
        }
        return procarray_seams::global_vis_test_is_removable_xid::call(
            ::types_core::GlobalVisStateHandle::new(1),
            delete_xid,
        );
    }
    Ok(false)
}

/// GinNewBuffer: recycle via FSM or extend; returned pinned + exclusive.
pub fn GinNewBuffer(rel: &Relation<'_>) -> PgResult<Buffer> {
    loop {
        let blkno = freespace_seams::get_page_with_free_space::call(rel, (BLCKSZ / 2) as usize)?;
        if blkno == InvalidBlockNumber {
            break;
        }
        freespace_seams::record_page_with_free_space::call(rel, blkno, 0)?;

        let buffer = bm::read_buffer::call(rel, blkno)?;
        if bm::conditional_lock_buffer::call(buffer)? {
            if gin_page_is_recyclable(buffer)? {
                return Ok(buffer);
            }
            bm::lock_buffer::call(buffer, crate::GIN_UNLOCK)?;
        }
        bm::release_buffer::call(buffer)?;
    }

    let (buffer, extended_by) = bm::extend_buffered_rel_by::call(
        rel,
        ForkNumber::MAIN_FORKNUM,
        None,
        bm::EB_LOCK_FIRST,
        1,
    )?;
    debug_assert!(extended_by == 1);
    Ok(buffer)
}

/// GinInitPage over a raw BLCKSZ image.
pub(crate) fn gin_init_page_bytes(bytes: &mut [u8], flags: u16) {
    // PageInit(page, BLCKSZ, sizeof(GinPageOpaqueData)).
    // SAFETY: BLCKSZ image with exclusive access.
    let mut page = unsafe { PageMut::from_raw(core::ptr::NonNull::new(bytes.as_mut_ptr()).unwrap()) };
    page.init(core::mem::size_of::<GinPageOpaqueData>());
    write_opaque_to(
        bytes,
        &GinPageOpaqueData {
            rightlink: InvalidBlockNumber,
            maxoff: 0,
            flags,
        },
    );
}

/// GinInitBuffer.
pub fn GinInitBuffer(buf: Buffer, flags: u16) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    // SAFETY: borrow confined to this call.
    gin_init_page_bytes(unsafe { page_bytes_mut(&mut page) }, flags);
}

/// GinInitMetabuffer.
pub fn GinInitMetabuffer(buf: Buffer) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    // SAFETY: borrow confined to this call.
    let bytes = unsafe { page_bytes_mut(&mut page) };
    gin_init_metapage_bytes(bytes);
}

pub(crate) fn gin_init_metapage_bytes(bytes: &mut [u8]) {
    gin_init_page_bytes(bytes, GIN_META);
    write_meta_to(
        bytes,
        &GinMetaPageData {
            head: InvalidBlockNumber,
            tail: InvalidBlockNumber,
            tailFreeSize: 0,
            nPendingPages: 0,
            nPendingHeapTuples: 0,
            nTotalPages: 0,
            nEntryPages: 0,
            nDataPages: 0,
            nEntries: 0,
            ginVersion: GIN_CURRENT_VERSION,
        },
    );
    set_meta_pd_lower(bytes);
}

/// pd_lower just past the metadata: required so xlog page compression keeps it.
pub(crate) fn set_meta_pd_lower(bytes: &mut [u8]) {
    let lower = (crate::META_OFF + core::mem::size_of::<GinMetaPageData>()) as u16;
    bytes[12..14].copy_from_slice(&lower.to_ne_bytes());
}

/// ginCompareEntries.
pub fn ginCompareEntries(
    state: &GinState,
    attnum: OffsetNumber,
    a: Datum,
    category_a: GinNullCategory,
    b: Datum,
    category_b: GinNullCategory,
) -> i32 {
    if category_a != category_b {
        return if category_a < category_b { -1 } else { 1 };
    }
    if category_a != GIN_CAT_NORM_KEY {
        return 0;
    }
    opclass::compare(state.col(attnum), a, b)
}

/// ginCompareAttEntries: attribute number dominates.
pub fn ginCompareAttEntries(
    state: &GinState,
    attnum_a: OffsetNumber,
    a: Datum,
    category_a: GinNullCategory,
    attnum_b: OffsetNumber,
    b: Datum,
    category_b: GinNullCategory,
) -> i32 {
    if attnum_a != attnum_b {
        return if attnum_a < attnum_b { -1 } else { 1 };
    }
    ginCompareEntries(state, attnum_a, a, category_a, b, category_b)
}

/// ginExtractEntries: keys sorted + de-duplicated, with null categories.
/// Null keys (array_ops elements) sort after normal keys (cmpEntries) and
/// dedup into one GIN_CAT_NULL_KEY entry.
pub fn ginExtractEntries<'mcx>(
    mcx: Mcx<'mcx>,
    state: &GinState,
    attnum: OffsetNumber,
    value: Datum,
    is_null: bool,
) -> PgResult<(PgVec<'mcx, Datum>, PgVec<'mcx, GinNullCategory>)> {
    let mut categories: PgVec<'mcx, GinNullCategory>;
    if is_null {
        let mut entries = mcx::vec_with_capacity_in(mcx, 1)?;
        entries.push(Datum::null());
        categories = mcx::vec_with_capacity_in(mcx, 1)?;
        categories.push(GIN_CAT_NULL_ITEM);
        return Ok((entries, categories));
    }

    let (mut entries, null_flags) = opclass::extract_value(mcx, state.col(attnum), value)?;

    if entries.is_empty() {
        entries.try_reserve(1).map_err(|_| crate::oom(8))?;
        entries.push(Datum::null());
        categories = mcx::vec_with_capacity_in(mcx, 1)?;
        categories.push(GIN_CAT_EMPTY_ITEM);
        return Ok((entries, categories));
    }

    categories = mcx::vec_with_capacity_in(mcx, entries.len())?;
    if null_flags.is_empty() {
        for _ in 0..entries.len() {
            categories.push(GIN_CAT_NORM_KEY);
        }
    } else {
        debug_assert!(null_flags.len() == entries.len());
        for &f in null_flags.iter() {
            categories.push(if f { GIN_CAT_NULL_KEY } else { GIN_CAT_NORM_KEY });
        }
    }

    if entries.len() > 1 {
        // cmpEntries + qsort_arg + dedup over (datum, category) pairs.
        let mut keydata: PgVec<'mcx, (Datum, GinNullCategory)> =
            mcx::vec_with_capacity_in(mcx, entries.len())?;
        for i in 0..entries.len() {
            keydata.push((entries[i], categories[i]));
        }
        let mut have_dups = false;
        keydata.sort_by(|a, b| {
            let r = ginCompareEntries(state, attnum, a.0, a.1, b.0, b.1);
            if r == 0 {
                have_dups = true;
            }
            r.cmp(&0)
        });
        if have_dups {
            let mut j = 0usize;
            for i in 1..keydata.len() {
                if ginCompareEntries(state, attnum, keydata[j].0, keydata[j].1, keydata[i].0, keydata[i].1)
                    != 0
                {
                    j += 1;
                    keydata[j] = keydata[i];
                }
            }
            keydata.truncate(j + 1);
        }
        entries.truncate(keydata.len());
        categories.truncate(keydata.len());
        for (i, (d, c)) in keydata.iter().enumerate() {
            entries[i] = *d;
            categories[i] = *c;
        }
    }

    Ok((entries, categories))
}

/// ginGetStats.
pub fn ginGetStats(rel: &Relation<'_>) -> PgResult<GinStatsData> {
    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    bm::lock_buffer::call(metabuffer, crate::GIN_SHARE)?;
    // SAFETY: pin + share lock held.
    let metadata = meta_of(crate::page_bytes(&unsafe { page_ref(metabuffer) }));
    bm::lock_buffer::call(metabuffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;
    Ok(GinStatsData {
        nPendingPages: metadata.nPendingPages,
        nTotalPages: metadata.nTotalPages,
        nEntryPages: metadata.nEntryPages,
        nDataPages: metadata.nDataPages,
        nEntries: metadata.nEntries,
        ginVersion: metadata.ginVersion,
    })
}

/// ginUpdateStats.
pub fn ginUpdateStats(rel: &Relation<'_>, stats: &GinStatsData, is_build: bool) -> PgResult<()> {
    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    bm::lock_buffer::call(metabuffer, crate::GIN_EXCLUSIVE)?;

    // ginutil.c:666 START_CRIT_SECTION (an Err escaping with the count raised
    // is promoted to PANIC by the xact layer, as C's elog does).
    init_small::globals::StartCriticalSection();

    let metadata = {
        // SAFETY: pin + exclusive lock held.
        let mut page = unsafe { page_mut(metabuffer) };
        // SAFETY: borrow confined to this block.
        let bytes = unsafe { page_bytes_mut(&mut page) };
        let mut metadata = meta_of(bytes);
        metadata.nTotalPages = stats.nTotalPages;
        metadata.nEntryPages = stats.nEntryPages;
        metadata.nDataPages = stats.nDataPages;
        metadata.nEntries = stats.nEntries;
        write_meta_to(bytes, &metadata);
        set_meta_pd_lower(bytes);
        metadata
    };
    bm::mark_buffer_dirty::call(metabuffer)?;

    if relation_needs_wal(rel) && !is_build {
        let data = crate::wal::ginxlog_update_meta(
            rel,
            &metadata,
            InvalidBlockNumber,
            InvalidBlockNumber,
            0,
        );
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_UPDATE_META_PAGE,
            0,
            &[&data],
            &[XLogRegBuf {
                block_id: 0,
                buffer: metabuffer,
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(metabuffer) }.set_lsn(recptr);
    }

    bm::lock_buffer::call(metabuffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;

    // ginutil.c:705 END_CRIT_SECTION.
    init_small::globals::EndCriticalSection();
    Ok(())
}

/// gininsert.c:643-652 (ginbuild): initialize the metapage and the root
/// page of a fresh index inside one critical section; both buffers arrive
/// pinned + exclusively locked (GinNewBuffer) and leave unlocked/unpinned.
pub fn gin_build_init_pages(meta_buffer: Buffer, root_buffer: Buffer) -> PgResult<()> {
    init_small::globals::StartCriticalSection();
    GinInitMetabuffer(meta_buffer);
    bm::mark_buffer_dirty::call(meta_buffer)?;
    GinInitBuffer(root_buffer, GIN_LEAF);
    bm::mark_buffer_dirty::call(root_buffer)?;

    bm::lock_buffer::call(meta_buffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(meta_buffer)?;
    bm::lock_buffer::call(root_buffer, crate::GIN_UNLOCK)?;
    bm::release_buffer::call(root_buffer)?;
    init_small::globals::EndCriticalSection();
    Ok(())
}
