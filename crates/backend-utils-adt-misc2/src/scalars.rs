//! Family `scalars` — `src/backend/utils/adt/tid.c` + `windowfuncs.c`.
//!
//! Two cohesive scalar/SRF-helper clusters grouped together: the `tid` type
//! (ItemPointer) I/O and operators — `tidin` / `tidout` / `tidrecv` /
//! `tidsend`, the comparison/equality family (`tideq` … `bttidcmp`,
//! `tidlarger` / `tidsmaller`), `hashtid` / `hashtidextended`, and the
//! `currtid_*` lookups — and the SQL window support functions in
//! windowfuncs.c (rank / dense_rank / percent_rank / cume_dist / ntile /
//! row_number, the lead/lag/first/last/nth_value `leadlag_common` family, and
//! the matching `*_support` planner-support functions).
//!
//! TID ops are pure scalar transforms (no Mcx beyond text formatting); the
//! window functions drive the executor's WindowObject through the adt-infra
//! SRF/window boundary (seamed to its real owner). Values cross as `Datum`.
//! Independent of the keystone.
//!
//! ## Faithfulness / seam boundaries
//!
//! Every piece of *this unit's own* logic — the `tid` input-syntax parser, the
//! `ItemPointerCompare` ordering and the equality/larger/smaller/cmp wrappers
//! built on it, and the full control flow of the window functions (rank
//! advancement, ntile bucketing, lead/lag dispatch) — is ported field-for-field
//! from `tid.c` / `windowfuncs.c`.
//!
//! Three boundaries reach owners that are **not yet ported** and are therefore
//! reached through the named seam-and-panic helpers in [`unported`]
//! (mirror-pg-and-panic — never a silent stub):
//!
//! * The `WindowObject` runtime (`windowapi.h`: `WinGetCurrentPosition`,
//!   `WinSetMarkPosition`, `WinRowsArePeers`, `WinGetPartitionRowCount`,
//!   `WinGetPartitionLocalMemory`, `WinGetFuncArgCurrent`,
//!   `WinGetFuncArgInPartition`, `WinGetFuncArgInFrame`) — owned by the
//!   executor's `nodeWindowAgg.c`, not yet a crate on main.
//! * `get_fn_expr_arg_stable` (`utils/fmgr` flinfo introspection) — reached for
//!   the lead/lag/nth_value const-offset optimization.
//! * The `tid` type's pass-by-reference container plumbing: `palloc`'d
//!   `ItemPointerData`, the cstring/`bytea` result construction, and the
//!   `pq_getmsgint` / `pq_sendint*` wire codec (libpq/pqformat), plus the
//!   `currtid_internal` table-AM / snapshot / acl path. The pure arithmetic
//!   that *surrounds* these (syntax scan, range checks, comparison) is ported
//!   in full; only the container/runtime crossing panics.

use mcx::Mcx;
use alloc::string::ToString;
use types_tuple::backend_access_common_heaptuple::Datum;
use types_error::{ereturn, PgError, PgResult, SoftErrorContext};
use types_error::{
    ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE,
    ERRCODE_INVALID_ARGUMENT_FOR_NTILE, ERRCODE_INVALID_TEXT_REPRESENTATION,
};

// ---------------------------------------------------------------------------
// tid.c
// ---------------------------------------------------------------------------

// tid.c #defines
const LDELIM: u8 = b'(';
const RDELIM: u8 = b')';
const DELIM: u8 = b',';
const NTIDARGS: usize = 2;

/// `BlockNumber` (`storage/block.h`): `typedef uint32 BlockNumber`.
type BlockNumber = u32;
/// `OffsetNumber` (`storage/off.h`): `typedef uint16 OffsetNumber`.
type OffsetNumber = u16;

/// `ItemPointerData` (`storage/itemptr.h`) — `BlockIdData` (two `uint16`) plus a
/// `uint16` offset. Field-for-field with the C struct; the layered owner is
/// `types_tuple::ItemPointerData` (not a dependency of this carrier crate, so
/// the (block, offset) pair is modeled locally and crosses as a `Datum`).
#[derive(Clone, Copy, PartialEq, Eq)]
struct ItemPointer {
    block_number: BlockNumber,
    offset_number: OffsetNumber,
}

impl ItemPointer {
    /// C `ItemPointerSet(pointer, blockNumber, offNum)`.
    fn set(block_number: BlockNumber, offset_number: OffsetNumber) -> Self {
        ItemPointer {
            block_number,
            offset_number,
        }
    }

    /// C `ItemPointerGetBlockNumberNoCheck` (no validity assertion).
    fn block_number_no_check(&self) -> BlockNumber {
        self.block_number
    }

    /// C `ItemPointerGetOffsetNumberNoCheck`.
    fn offset_number_no_check(&self) -> OffsetNumber {
        self.offset_number
    }
}

/// C `ItemPointerCompare` (`storage/itemptr.c`): order by block number, then by
/// offset number; returns -1 / 0 / 1. Pure arithmetic, ported in full.
fn item_pointer_compare(arg1: &ItemPointer, arg2: &ItemPointer) -> i32 {
    // BlockIdGetBlockNumber comparison first.
    let b1 = arg1.block_number_no_check();
    let b2 = arg2.block_number_no_check();

    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else {
        // same block, compare offsets
        let o1 = arg1.offset_number_no_check();
        let o2 = arg2.offset_number_no_check();
        if o1 < o2 {
            -1
        } else if o1 > o2 {
            1
        } else {
            0
        }
    }
}

/// Build the `invalid input syntax for type tid` soft/hard error
/// (`ERRCODE_INVALID_TEXT_REPRESENTATION`) exactly as tid.c's repeated
/// `ereturn(... errmsg("invalid input syntax for type %s: \"%s\"", "tid", str))`.
fn tid_invalid_syntax(str: &str) -> PgError {
    PgError::error(alloc::format!(
        "invalid input syntax for type tid: \"{str}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// `tidin(str)` — input routine (tid.c, "largely stolen from boxin()").
///
/// Parses `(blocknum,offnum)`; the surrounding scan + `strtoul` range checks are
/// ported in full. NULL (`None`) cstring cannot reach a C cstring-in (the cstring
/// is never SQL-NULL); we treat it as the empty string, which fails the syntax
/// check exactly like a C empty input would.
pub fn tidin<'mcx>(
    mcx: Mcx<'mcx>,
    string: Option<&str>,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Datum<'mcx>> {
    let str = string.unwrap_or("");
    let bytes = str.as_bytes();

    // for (i = 0, p = str; *p && i < NTIDARGS && *p != RDELIM; p++)
    //     if (*p == DELIM || (*p == LDELIM && i == 0))
    //         coord[i++] = p + 1;
    // coord[k] records the byte offset *after* the matched delimiter.
    let mut coord: [usize; NTIDARGS] = [0; NTIDARGS];
    let mut i = 0usize;
    let mut p = 0usize;
    while p < bytes.len() && i < NTIDARGS && bytes[p] != RDELIM {
        let c = bytes[p];
        if c == DELIM || (c == LDELIM && i == 0) {
            coord[i] = p + 1;
            i += 1;
        }
        p += 1;
    }

    // C: `ereturn(escontext, (Datum) 0, ...)` — with a soft sink the syntax
    // error is saved and a discarded value is returned Ok; without one it
    // throws. `pg_input_is_valid('(0)','tid')` / `pg_input_error_info` rely on
    // the soft path.
    if i < NTIDARGS {
        return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str));
    }

    // errno = 0; cvt = strtoul(coord[0], &badp, 10);
    // if (errno || *badp != DELIM) ereturn(...)
    let (cvt0, badp0) = strtoul(bytes, coord[0]);
    let cvt0 = match cvt0 {
        Some(v) => v,
        None => {
            return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str))
        }
    };
    if byte_at(bytes, badp0) != DELIM {
        return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str));
    }
    let block_number = cvt0 as BlockNumber;

    // Cope with possibility that unsigned long is wider than BlockNumber
    // (SIZEOF_LONG > 4): reject values out of BlockNumber range (matching oidin).
    if cvt0 != block_number as u64 && cvt0 != (block_number as i32) as u64 {
        return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str));
    }

    // cvt = strtoul(coord[1], &badp, 10);
    // if (errno || *badp != RDELIM || cvt > USHRT_MAX) ereturn(...)
    let (cvt1, badp1) = strtoul(bytes, coord[1]);
    let cvt1 = match cvt1 {
        Some(v) => v,
        None => {
            return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str))
        }
    };
    if byte_at(bytes, badp1) != RDELIM || cvt1 > u16::MAX as u64 {
        return ereturn(escontext.as_deref_mut(), Datum::null(), tid_invalid_syntax(str));
    }
    let offset_number = cvt1 as OffsetNumber;

    // result = palloc(sizeof(ItemPointerData)); ItemPointerSet(result, ...)
    let result = ItemPointer::set(block_number, offset_number);

    // PG_RETURN_ITEMPOINTER(result): a palloc'd ItemPointerData crosses by
    // reference (unported container plumbing).
    unported::return_itempointer(mcx, result)
}

/// `tidout(itemPtr)` — `snprintf(buf, "(%u,%u)", block, offset)` then `pstrdup`.
pub fn tidout<'mcx>(mcx: Mcx<'mcx>, item_ptr: Datum) -> PgResult<Datum<'mcx>> {
    let item_ptr = unported::getarg_itempointer(&item_ptr);
    let block_number = item_ptr.block_number_no_check();
    let offset_number = item_ptr.offset_number_no_check();
    // "Perhaps someday we should output this as a record."
    let buf = alloc::format!("({block_number},{offset_number})");
    // PG_RETURN_CSTRING(pstrdup(buf)): the cstring result crosses by reference.
    unported::return_cstring(mcx, buf)
}

/// `tidrecv(buf)` — `pq_getmsgint(blocknum)` + `pq_getmsgint(offnum)`.
pub fn tidrecv<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<Datum<'mcx>> {
    // Wrap the wire bytes as a real StringInfo (cursor = 0) and read through the
    // libpq/pqformat owner, exactly mirroring tidrecv's two pq_getmsgint calls.
    let mut msg = types_stringinfo::StringInfo::from_vec(mcx::slice_in(mcx, buf)?);
    // blockNumber = pq_getmsgint(buf, sizeof(blockNumber)); // uint32
    let block_number = backend_libpq_pqformat::pq_getmsgint(&mut msg, 4)?;
    // offsetNumber = pq_getmsgint(buf, sizeof(offsetNumber)); // uint16
    let offset_number = backend_libpq_pqformat::pq_getmsgint(&mut msg, 2)? as OffsetNumber;
    let result = ItemPointer::set(block_number, offset_number);
    unported::return_itempointer(mcx, result)
}

/// `tidsend(itemPtr)` — `pq_sendint32(block)` + `pq_sendint16(offset)`.
pub fn tidsend<'mcx>(mcx: Mcx<'mcx>, item_ptr: Datum) -> PgResult<Datum<'mcx>> {
    let item_ptr = unported::getarg_itempointer(&item_ptr);
    // pq_begintypsend(&buf); pq_sendint32(&buf, block); pq_sendint16(&buf, off);
    let mut buf = backend_libpq_pqformat::pq_begintypsend(mcx)?;
    backend_libpq_pqformat::pq_sendint32(&mut buf, item_ptr.block_number_no_check())?;
    backend_libpq_pqformat::pq_sendint16(&mut buf, item_ptr.offset_number_no_check())?;
    // PG_RETURN_BYTEA_P(pq_endtypsend(&buf)): endtypsend stamps the varlena
    // header and yields the bytea result; the by-reference Datum construction is
    // the still-unported boundary.
    let bytea = backend_libpq_pqformat::pq_endtypsend(buf);
    unported::return_bytea(mcx, bytea.into_image().into_iter().collect())
}

/// `tideq(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) == 0`.
pub fn tideq(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) == 0)
}

/// `tidne(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) != 0`.
pub fn tidne(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) != 0)
}

/// `tidlt(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) < 0`.
pub fn tidlt(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) < 0)
}

/// `tidle(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) <= 0`.
pub fn tidle(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) <= 0)
}

/// `tidgt(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) > 0`.
pub fn tidgt(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) > 0)
}

/// `tidge(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) >= 0`.
pub fn tidge(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<bool> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2) >= 0)
}

/// `bttidcmp(arg1, arg2)` — three-way TID comparison shared by the operators.
pub fn bttidcmp(arg1: Datum<'_>, arg2: Datum<'_>) -> PgResult<i32> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(item_pointer_compare(&a1, &a2))
}

/// `tidlarger(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) >= 0 ? arg1 : arg2`.
pub fn tidlarger<'mcx>(arg1: Datum<'mcx>, arg2: Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(if item_pointer_compare(&a1, &a2) >= 0 {
        arg1
    } else {
        arg2
    })
}

/// `tidsmaller(arg1, arg2)` — `ItemPointerCompare(arg1, arg2) <= 0 ? arg1 : arg2`.
pub fn tidsmaller<'mcx>(arg1: Datum<'mcx>, arg2: Datum<'mcx>) -> PgResult<Datum<'mcx>> {
    let a1 = unported::getarg_itempointer(&arg1);
    let a2 = unported::getarg_itempointer(&arg2);
    Ok(if item_pointer_compare(&a1, &a2) <= 0 {
        arg1
    } else {
        arg2
    })
}

/// `hashtid(key)` — `hash_any(key, sizeof(BlockIdData) + sizeof(OffsetNumber))`.
///
/// The C deliberately hashes only `sizeof(BlockIdData) + sizeof(OffsetNumber)`
/// bytes (not `sizeof(ItemPointerData)`) to avoid struct trailing pad. The hash
/// itself (`hash_any`) lives in `common/hashfn`, not yet a dependency of this
/// carrier crate, so the byte image is built here and the hash crossing is the
/// seam-and-panic boundary.
pub fn hashtid(key: Datum<'_>) -> PgResult<u32> {
    let key = unported::getarg_itempointer(&key);
    let image = itempointer_hash_image(&key);
    Ok(unported::hash_any(&image))
}

/// `hashtidextended(key, seed)` —
/// `hash_any_extended(key, sizeof(BlockIdData) + sizeof(OffsetNumber), seed)`.
pub fn hashtidextended(key: Datum<'_>, seed: u64) -> PgResult<u64> {
    let key = unported::getarg_itempointer(&key);
    let image = itempointer_hash_image(&key);
    Ok(unported::hash_any_extended(&image, seed))
}

/// The exact `sizeof(BlockIdData) + sizeof(OffsetNumber)` little-significant
/// byte image the C hashes: `BlockIdData{bi_hi, bi_lo}` (two `uint16`, native
/// layout) followed by the `uint16` offset — 6 bytes total, no trailing pad.
fn itempointer_hash_image(ptr: &ItemPointer) -> [u8; 6] {
    itempointer_image(ptr)
}

/// The canonical 6-byte on-disk/in-memory image of an `ItemPointerData`:
/// `BlockIdData{bi_hi = high16, bi_lo = low16}` (two `uint16`, native layout)
/// followed by the `uint16` offset — exactly `sizeof(ItemPointerData)` with no
/// trailing pad. This is both the image the C struct occupies in memory (what a
/// pass-by-reference TID `Datum` points at) and the `sizeof(BlockIdData) +
/// sizeof(OffsetNumber)` window `hash_any` digests.
fn itempointer_image(ptr: &ItemPointer) -> [u8; 6] {
    // BlockIdData stores the block number as { bi_hi = high16, bi_lo = low16 }.
    let bn = ptr.block_number_no_check();
    let bi_hi = (bn >> 16) as u16;
    let bi_lo = (bn & 0xffff) as u16;
    let off = ptr.offset_number_no_check();
    let mut image = [0u8; 6];
    image[0..2].copy_from_slice(&bi_hi.to_ne_bytes());
    image[2..4].copy_from_slice(&bi_lo.to_ne_bytes());
    image[4..6].copy_from_slice(&off.to_ne_bytes());
    image
}

/// Convert the local 6-byte image `ItemPointer` (this unit's decoupled struct)
/// into the layered `types_tuple::ItemPointerData` the table-AM consumes.
fn to_item_pointer_data(p: &ItemPointer) -> types_tuple::ItemPointerData {
    types_tuple::ItemPointerData::new(p.block_number_no_check(), p.offset_number_no_check())
}

/// Convert back from the table-AM `ItemPointerData` into the local image struct.
fn from_item_pointer_data(p: &types_tuple::ItemPointerData) -> ItemPointer {
    ItemPointer::set(p.ip_blkid.block_number(), p.ip_posid)
}

/// `currtid_internal(rel, tid)` (tid.c) — return the latest tuple version
/// pointing at `tid` for the open relation `rel`. ACL-checks `ACL_SELECT`,
/// dispatches views to [`currtid_for_view`], rejects storage-less relkinds, and
/// otherwise runs a TID scan under a fresh registered snapshot.
fn currtid_internal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    tid: &ItemPointer,
) -> PgResult<ItemPointer> {
    use types_tuple::access::{
        RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_SEQUENCE, RELKIND_TOASTVALUE,
        RELKIND_VIEW,
    };

    let relid = rel.rd_id;
    let relkind = rel.rd_rel.relkind;

    // aclresult = pg_class_aclcheck(RelationGetRelid(rel), GetUserId(), ACL_SELECT);
    let aclresult = backend_catalog_aclchk_seams::pg_class_aclcheck::call(
        relid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        types_acl::acl::ACL_SELECT,
    )?;
    if aclresult != types_acl::acl::ACLCHECK_OK {
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            backend_catalog_objectaddress_seams::get_relkind_objtype::call(relkind),
            Some(rel.name().to_string()),
        )?;
    }

    if relkind == RELKIND_VIEW {
        return currtid_for_view(mcx, rel, tid);
    }

    // RELKIND_HAS_STORAGE(relkind) — pg_class.h.
    let has_storage = relkind == RELKIND_RELATION
        || relkind == RELKIND_INDEX
        || relkind == RELKIND_SEQUENCE
        || relkind == RELKIND_TOASTVALUE
        || relkind == RELKIND_MATVIEW;
    if !has_storage {
        let nspname = backend_utils_cache_lsyscache_seams::get_namespace_name::call(
            mcx,
            rel.rd_rel.relnamespace,
        )?;
        let nspname = nspname.as_ref().map(|s| s.as_str()).unwrap_or("");
        return Err(PgError::error(alloc::format!(
                "cannot look at latest visible tid for relation \"{}.{}\"",
                nspname,
                rel.name()
            ))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // ItemPointerCopy(tid, result); then the TID scan fills the latest version.
    let mut result = to_item_pointer_data(tid);

    let snapshot = backend_utils_time_snapmgr_seams::get_latest_snapshot::call()?;
    let snapshot = backend_utils_time_snapmgr_seams::register_snapshot::call(snapshot)?;
    let mut scan = backend_access_table_tableam::table_beginscan_tid(
        mcx,
        rel,
        Some(snapshot.clone()),
    )?;
    backend_access_table_tableam::table_tuple_get_latest_tid(mcx, &mut scan, &mut result)?;
    backend_access_table_tableam::table_endscan(scan)?;
    backend_utils_time_snapmgr_seams::unregister_snapshot::call(snapshot);

    Ok(from_item_pointer_data(&result))
}

/// `currtid_for_view(viewrel, tid)` (tid.c) — a view's `ctid` must be defined
/// and correspond to a base relation's `ctid`. Find the `ctid` column, walk the
/// view's SELECT rule to the `SelfItemPointerAttributeNumber` Var, then recurse
/// into the underlying base relation.
fn currtid_for_view<'mcx>(
    mcx: Mcx<'mcx>,
    viewrel: &types_rel::Relation<'mcx>,
    tid: &ItemPointer,
) -> PgResult<ItemPointer> {
    use types_tuple::heaptuple::{SelfItemPointerAttributeNumber, TIDOID};

    let att = &viewrel.rd_att;
    let natts = att.natts as usize;
    let mut tididx: i32 = -1;

    for i in 0..natts {
        let attr = att.attr(i);
        if attr.attname.name_str() == b"ctid" {
            if attr.atttypid != TIDOID {
                return Err(PgError::error("ctid isn't of type TID".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            tididx = i as i32;
            break;
        }
    }
    if tididx < 0 {
        return Err(PgError::error("currtid cannot handle views with no CTID".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // rulelock = viewrel->rd_rules; if (!rulelock) ereport "the view has no rules".
    let rulelock = backend_utils_cache_relcache_seams::relation_rules::call(mcx, viewrel.rd_id)?;
    let rulelock = match rulelock {
        Some(r) => r,
        None => {
            return Err(PgError::error("the view has no rules".to_string())
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
        }
    };

    for rewrite in rulelock.rules.iter() {
        if rewrite.event == types_nodes::nodes::CmdType::CMD_SELECT {
            // if (list_length(rewrite->actions) != 1) ereport "only one select rule".
            if rewrite.actions.len() != 1 {
                return Err(PgError::error("only one select rule is allowed in views".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            let query = &rewrite.actions[0];
            // tle = get_tle_by_resno(query->targetList, tididx + 1);
            let tle = backend_parser_relation::get_tle_by_resno(
                &query.targetList,
                (tididx + 1) as i16,
            );
            if let Some(tle) = tle {
                if let Some(expr) = tle.expr.as_deref() {
                    if let types_nodes::primnodes::Expr::Var(var) = expr {
                        // !IS_SPECIAL_VARNO(var->varno) -> varno >= 0 (C: < 0 special)
                        if var.varno >= 0
                            && var.varattno == SelfItemPointerAttributeNumber
                        {
                            // rte = rt_fetch(var->varno, query->rtable);
                            let idx = (var.varno - 1) as usize;
                            if let Some(rte) = query.rtable.get(idx) {
                                let rel = backend_access_table_table::table_open(
                                    mcx,
                                    rte.relid,
                                    types_storage::lock::AccessShareLock,
                                )?;
                                let result = currtid_internal(mcx, &rel, tid)?;
                                backend_access_table_table::table_close(
                                    rel,
                                    types_storage::lock::AccessShareLock,
                                )?;
                                return Ok(result);
                            }
                        }
                    }
                }
            }
            break;
        }
    }

    // elog(ERROR, "currtid cannot handle this view");
    Err(PgError::error("currtid cannot handle this view".to_string())
    )
}

/// `currtid_byrelname(relname, tid)` (tid.c) — open the named relation and
/// return the latest tuple version pointing at `tid`.
pub fn currtid_byrelname<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    tid: Datum<'_>,
) -> PgResult<Datum<'mcx>> {
    let tid = unported::getarg_itempointer(&tid);

    // relrv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
    let parts =
        backend_utils_adt_varlena_seams::text_to_qualified_name_list::call(mcx, relname.as_bytes())?;
    let parts: alloc::vec::Vec<&str> = parts.iter().map(|p| p.as_str()).collect();
    let relrv = backend_catalog_namespace_seams::make_range_var_from_name_list::call(&parts)?;

    // rel = table_openrv(relrv, AccessShareLock);
    let rel =
        backend_access_table_table::table_openrv(mcx, &relrv, types_storage::lock::AccessShareLock)?;

    // result = currtid_internal(rel, tid);
    let result = currtid_internal(mcx, &rel, &tid);

    // table_close(rel, AccessShareLock); — close before propagating any error,
    // mirroring C's cleanup (which happens via resowner on the error path).
    backend_access_table_table::table_close(rel, types_storage::lock::AccessShareLock)?;

    let result = result?;
    unported::return_itempointer(mcx, result)
}

// ---------------------------------------------------------------------------
// windowfuncs.c
// ---------------------------------------------------------------------------
//
// The WindowObject runtime (windowapi.h) is owned by the executor's
// nodeWindowAgg.c, not yet ported. The per-partition context structs
// (rank_context / ntile_context) and the entire control flow of each window
// function are ported field-for-field below; only the WinGet*/WinSet* accessors
// cross to the unported owner (via `unported::win`).

/// C `rank_context` — ranking process information stored in partition-local
/// memory. `WinGetPartitionLocalMemory` zero-initializes on first use, so
/// `rank == 0` means "first call".
#[derive(Clone, Copy, Default)]
struct RankContext {
    /// current rank
    rank: i64,
}

/// C `ntile_context` — ntile process information.
#[derive(Clone, Copy, Default)]
struct NtileContext {
    /// current result
    ntile: i32,
    /// row number of current bucket
    rows_per_bucket: i64,
    /// how many rows should be in the bucket
    boundary: i64,
    /// (total rows) % (bucket num)
    remainder: i64,
}

/// C `rank_up(winobj)` — utility routine for `*_rank` functions; returns whether
/// the rank should increase, and advances the mark.
fn rank_up(winobj: unported::WindowObject) -> bool {
    let mut up = false; // should rank increase?
    let curpos = unported::win::get_current_position(winobj);
    let context = unported::win::get_partition_local_memory_rank(winobj);

    if context.rank == 0 {
        // first call: rank of first row is always 1
        debug_assert!(curpos == 0);
        context.rank = 1;
    } else {
        debug_assert!(curpos > 0);
        // do current and prior tuples match by ORDER BY clause?
        if !unported::win::rows_are_peers(winobj, curpos - 1, curpos) {
            up = true;
        }
    }

    // We can advance the mark, but only *after* access to prior row
    unported::win::set_mark_position(winobj, curpos);

    up
}

/// `window_row_number(fcinfo)` — just increment up from 1 until current
/// partition finishes.
pub fn window_row_number<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let curpos = unported::win::get_current_position(winobj);

    unported::win::set_mark_position(winobj, curpos);
    Ok(Datum::from_i64(curpos + 1))
}

/// `window_rank(fcinfo)` — rank changes when key columns change; the new rank
/// number is the current row number.
pub fn window_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let up = rank_up(winobj);
    let context = unported::win::get_partition_local_memory_rank(winobj);
    if up {
        context.rank = unported::win::get_current_position(winobj) + 1;
    }
    Ok(Datum::from_i64(context.rank))
}

/// `window_dense_rank(fcinfo)` — rank increases by 1 when key columns change.
pub fn window_dense_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let up = rank_up(winobj);
    let context = unported::win::get_partition_local_memory_rank(winobj);
    if up {
        context.rank += 1;
    }
    Ok(Datum::from_i64(context.rank))
}

/// `window_percent_rank(fcinfo)` — `(RK - 1) / (NR - 1)`, per spec; returns 0 if
/// there is only one row.
pub fn window_percent_rank<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let totalrows = unported::win::get_partition_row_count(winobj);

    debug_assert!(totalrows > 0);

    let up = rank_up(winobj);
    let context = unported::win::get_partition_local_memory_rank(winobj);
    if up {
        context.rank = unported::win::get_current_position(winobj) + 1;
    }

    // return zero if there's only one row, per spec
    if totalrows <= 1 {
        return Ok(Datum::from_f64(0.0));
    }

    Ok(Datum::from_f64(
        (context.rank - 1) as f64 / (totalrows - 1) as f64,
    ))
}

/// `window_cume_dist(fcinfo)` — `NP / NR`, per spec, where NP is the number of
/// rows preceding or peer to the current row.
pub fn window_cume_dist<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let totalrows = unported::win::get_partition_row_count(winobj);

    debug_assert!(totalrows > 0);

    let up = rank_up(winobj);
    let context = unported::win::get_partition_local_memory_rank(winobj);
    if up || context.rank == 1 {
        // The current row is not peer to prior row or is just the first, so
        // count up the number of rows that are peer to the current.
        context.rank = unported::win::get_current_position(winobj) + 1;

        // start from current + 1
        let mut row = context.rank;
        while row < totalrows {
            if !unported::win::rows_are_peers(winobj, row - 1, row) {
                break;
            }
            context.rank += 1;
            row += 1;
        }
    }

    Ok(Datum::from_f64(context.rank as f64 / totalrows as f64))
}

/// `window_ntile(fcinfo)` — compute an exact numeric value with scale 0, ranging
/// from 1 to n, per spec.
pub fn window_ntile<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let context = unported::win::get_partition_local_memory_ntile(winobj);

    if context.ntile == 0 {
        // first call
        let total = unported::win::get_partition_row_count(winobj);
        let mut isnull = false;
        let nbuckets =
            unported::win::get_func_arg_current(winobj, 0, &mut isnull).as_i32();

        // per spec: If NT is the null value, then the result is the null value.
        if isnull {
            return Ok(Datum::null()); // PG_RETURN_NULL()
        }

        // per spec: If NT is <= 0, then an exception condition is raised.
        if nbuckets <= 0 {
            return Err(PgError::error(
                "argument of ntile must be greater than zero",
            )
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_NTILE));
        }

        context.ntile = 1;
        context.rows_per_bucket = 0;
        context.boundary = total / nbuckets as i64;
        if context.boundary <= 0 {
            context.boundary = 1;
        } else {
            // If the total number is not divisible, add 1 row to leading
            // buckets.
            context.remainder = total % nbuckets as i64;
            if context.remainder != 0 {
                context.boundary += 1;
            }
        }
    }

    context.rows_per_bucket += 1;
    if context.boundary < context.rows_per_bucket {
        // ntile up
        if context.remainder != 0 && context.ntile as i64 == context.remainder {
            context.remainder = 0;
            context.boundary -= 1;
        }
        context.ntile += 1;
        context.rows_per_bucket = 1;
    }

    Ok(Datum::from_i32(context.ntile))
}

/// C `WINDOW_SEEK_CURRENT` / `WINDOW_SEEK_HEAD` / `WINDOW_SEEK_TAIL`
/// (`windowapi.h`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum WindowSeek {
    /// `WINDOW_SEEK_CURRENT`
    Current,
    /// `WINDOW_SEEK_HEAD`
    Head,
    /// `WINDOW_SEEK_TAIL`
    Tail,
}

/// `leadlag_common(...)` — shared engine for lead/lag with/without
/// offset/default. For lead() `forward` is true; for lag() it is false.
/// `with_offset` indicates a second offset argument; `with_default` a third
/// default argument.
pub fn leadlag_common<'mcx>(
    _mcx: Mcx<'mcx>,
    forward: bool,
    with_offset: bool,
    with_default: bool,
) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let offset: i32;
    let const_offset: bool;
    let mut isnull = false;

    if with_offset {
        offset = unported::win::get_func_arg_current(winobj, 1, &mut isnull).as_i32();
        if isnull {
            return Ok(Datum::null()); // PG_RETURN_NULL()
        }
        const_offset = unported::get_fn_expr_arg_stable(1);
    } else {
        offset = 1;
        const_offset = true;
    }

    let mut isout = false;
    let mut result = unported::win::get_func_arg_in_partition(
        winobj,
        0,
        if forward { offset } else { -offset },
        WindowSeek::Current,
        const_offset,
        &mut isnull,
        &mut isout,
    );

    if isout {
        // target row is out of the partition; supply default value if provided.
        // otherwise it'll stay NULL
        if with_default {
            result = unported::win::get_func_arg_current(winobj, 2, &mut isnull);
        }
    }

    if isnull {
        return Ok(Datum::null()); // PG_RETURN_NULL()
    }

    Ok(result) // PG_RETURN_DATUM(result)
}

/// `window_lag(fcinfo)` — value 1 row before the current row.
pub fn window_lag<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, false, false, false)
}

/// `window_lag_with_offset(fcinfo)` — value OFFSET rows before the current row.
pub fn window_lag_with_offset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, false, true, false)
}

/// `window_lag_with_offset_and_default(fcinfo)` — as above with a default value.
pub fn window_lag_with_offset_and_default<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, false, true, true)
}

/// `window_lead(fcinfo)` — value 1 row after the current row.
pub fn window_lead<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, true, false, false)
}

/// `window_lead_with_offset(fcinfo)` — value OFFSET rows after the current row.
pub fn window_lead_with_offset<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, true, true, false)
}

/// `window_lead_with_offset_and_default(fcinfo)` — as above with a default value.
pub fn window_lead_with_offset_and_default<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    leadlag_common(mcx, true, true, true)
}

/// `window_first_value(fcinfo)` — value evaluated on the first row of the frame.
pub fn window_first_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let mut isnull = false;
    let result = unported::win::get_func_arg_in_frame(
        winobj,
        0,
        0,
        WindowSeek::Head,
        true,
        &mut isnull,
        unported::win::NO_ISOUT,
    );
    if isnull {
        return Ok(Datum::null());
    }
    Ok(result)
}

/// `window_last_value(fcinfo)` — value evaluated on the last row of the frame.
pub fn window_last_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let mut isnull = false;
    let result = unported::win::get_func_arg_in_frame(
        winobj,
        0,
        0,
        WindowSeek::Tail,
        true,
        &mut isnull,
        unported::win::NO_ISOUT,
    );
    if isnull {
        return Ok(Datum::null());
    }
    Ok(result)
}

/// `window_nth_value(fcinfo)` — value on the n-th row from the first row of the
/// frame, per spec.
pub fn window_nth_value<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Datum<'mcx>> {
    let winobj = unported::win::window_object();
    let mut isnull = false;

    let nth = unported::win::get_func_arg_current(winobj, 1, &mut isnull).as_i32();
    if isnull {
        return Ok(Datum::null());
    }
    let const_offset = unported::get_fn_expr_arg_stable(1);

    if nth <= 0 {
        return Err(PgError::error(
            "argument of nth_value must be greater than zero",
        )
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE));
    }

    let result = unported::win::get_func_arg_in_frame(
        winobj,
        0,
        nth - 1,
        WindowSeek::Head,
        const_offset,
        &mut isnull,
        unported::win::NO_ISOUT,
    );
    if isnull {
        return Ok(Datum::null());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// *_support planner-support functions (windowfuncs.c)
// ---------------------------------------------------------------------------
//
// Each prosupport function handles two request node kinds:
// SupportRequestWFuncMonotonic (set req->monotonic) and
// SupportRequestOptimizeWindowClause (set req->frameOptions). The request node
// (a Node*) and the WFunc-monotonicity / frame-option enums are owned by
// nodes/supportnodes.h + parsenodes.h, not ported. mirror-pg-and-panic at that
// named boundary; the *decision* each support fn encodes (which monotonicity,
// which frame options) is documented inline so the port is faithful once the
// node owners land.

/// `window_row_number_support(rawreq)` — row_number() is monotonically
/// increasing; optimizes the frame to ROWS BETWEEN UNBOUNDED PRECEDING AND
/// CURRENT ROW.
pub fn window_row_number_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

/// `window_rank_support(rawreq)` — rank() is monotonically increasing; frame set
/// to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
pub fn window_rank_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

/// `window_dense_rank_support(rawreq)` — dense_rank() is monotonically
/// increasing; frame set to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
pub fn window_dense_rank_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

/// `window_percent_rank_support(rawreq)` — percent_rank() is monotonically
/// increasing; frame set to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
pub fn window_percent_rank_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

/// `window_cume_dist_support(rawreq)` — cume_dist() is monotonically increasing;
/// frame set to ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
pub fn window_cume_dist_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

/// `window_ntile_support(rawreq)` — ntile() is monotonically increasing (the
/// bucket count cannot change after the first call); frame set to ROWS BETWEEN
/// UNBOUNDED PRECEDING AND CURRENT ROW.
pub fn window_ntile_support<'mcx>(_mcx: Mcx<'mcx>, rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
    unported::window_support(rawreq)
}

// ---------------------------------------------------------------------------
// Pure helpers (this unit's own logic).
// ---------------------------------------------------------------------------

/// Read `bytes[idx]`, or NUL if `idx` is at/after the end — mirrors C reading
/// `*p` of a NUL-terminated string (so a `badp` parked on the terminator
/// compares unequal to DELIM/RDELIM and yields the syntax error).
fn byte_at(bytes: &[u8], idx: usize) -> u8 {
    if idx < bytes.len() {
        bytes[idx]
    } else {
        0
    }
}

/// Faithful subset of C `strtoul(start, &badp, 10)` over a NUL-terminated
/// string, for the `tidin` parser: skips leading whitespace and an optional
/// sign, consumes decimal digits, and reports the byte offset of the first
/// unconsumed character (`badp`). C `strtoul` parses the magnitude as an
/// `unsigned long` and, for a leading `-`, returns the two's-complement
/// negation of that unsigned value (e.g. `strtoul("-1") == ULONG_MAX`); tid.c
/// RELIES on this so that `'(-1,0)'::tid` yields `BlockNumber` 4294967295 and
/// `'(0,-1)'::tid` overflows USHRT_MAX and is rejected. We model `unsigned long`
/// as 64-bit (LP64, matching darwin/linux64). Returns `None` on the C error
/// conditions tid.c checks via `errno` (no digits consumed → returns 0 with
/// `badp == start`, which the caller's delimiter check rejects; or overflow of
/// `unsigned long`). Returns `Some(value)` otherwise.
fn strtoul(bytes: &[u8], start: usize) -> (Option<u64>, usize) {
    let mut i = start;
    // skip leading whitespace (C isspace: space, \t, \n, \v, \f, \r)
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        i += 1;
    }
    // optional sign
    let mut negative = false;
    if i < bytes.len() && (bytes[i] == b'+' || bytes[i] == b'-') {
        negative = bytes[i] == b'-';
        i += 1;
    }

    let digits_start = i;
    let mut value: u64 = 0;
    let mut overflow = false;
    while i < bytes.len() && bytes[i].is_ascii_digit() {
        let d = (bytes[i] - b'0') as u64;
        match value.checked_mul(10).and_then(|v| v.checked_add(d)) {
            Some(v) => value = v,
            None => overflow = true,
        }
        i += 1;
    }

    if i == digits_start {
        // No digits: C strtoul returns 0 and sets badp = start (sign skipped).
        // errno is not set; the caller's `*badp != DELIM/RDELIM` check rejects.
        return (Some(0), i);
    }

    if overflow {
        // C: errno == ERANGE. tid.c checks `errno` and treats it as invalid.
        return (None, i);
    }

    // C strtoul negates the unsigned magnitude in place (modular two's
    // complement) for a leading '-'; e.g. strtoul("-1") == ULONG_MAX.
    if negative {
        value = value.wrapping_neg();
    }

    (Some(value), i)
}

// ---------------------------------------------------------------------------
// Genuinely-unported-owner boundaries (mirror-pg-and-panic).
// ---------------------------------------------------------------------------
//
// Each item here is a real owner that has NOT yet been ported onto main. None of
// this unit's own logic is stubbed: these are the cross-unit crossings the C
// reaches through other modules. They panic loudly with the owning C file named
// so a later port can wire them (or so this family can be re-homed onto seam
// slots once those owners land and expose seam crates).
mod unported {
    use super::{Datum, ItemPointer, Mcx, NtileContext, PgResult, WindowSeek};
    use super::{ERRCODE_FEATURE_NOT_SUPPORTED, PgError};

    /// Opaque `WindowObject` handle (`windowapi.h`) — owned by the executor's
    /// `nodeWindowAgg.c`, not yet ported. Inherited-opacity: this is a real
    /// pointer-shaped handle in C; here it is a never-constructed marker, so any
    /// path that would dereference it panics through the accessors below.
    #[derive(Clone, Copy)]
    pub struct WindowObject(());

    /// `PG_GETARG_ITEMPOINTER(n)` — the arg datum is a pass-by-reference
    /// `ItemPointerData`. The by-reference `Datum::ByRef` arm carries the verbatim
    /// 6-byte struct image (`BlockIdData{bi_hi, bi_lo}` + `ip_posid`, native
    /// layout, no trailing pad — exactly the image `return_itempointer` writes and
    /// `itempointer_hash_image` produces). Decode it back into the `(block,
    /// offset)` pair, mirroring a C deref of the `palloc`'d `ItemPointerData`.
    pub fn getarg_itempointer(datum: &Datum<'_>) -> ItemPointer {
        let image = datum.as_ref_bytes();
        // BlockIdData{bi_hi = high16, bi_lo = low16} then uint16 ip_posid.
        let bi_hi = u16::from_ne_bytes([image[0], image[1]]);
        let bi_lo = u16::from_ne_bytes([image[2], image[3]]);
        let off = u16::from_ne_bytes([image[4], image[5]]);
        let block_number = ((bi_hi as u32) << 16) | bi_lo as u32;
        ItemPointer::set(block_number, off)
    }

    /// `PG_RETURN_ITEMPOINTER(result)` — the `ItemPointerData` crosses by
    /// reference. Build the canonical 6-byte struct image (the same native layout
    /// the C struct occupies in memory) and carry it through the `Datum::ByRef`
    /// arm; `slice_in` copies it into the caller's context (C: the `palloc`'d
    /// pointer lives in the current memory context).
    pub fn return_itempointer<'mcx>(mcx: Mcx<'mcx>, ptr: ItemPointer) -> PgResult<Datum<'mcx>> {
        let image = super::itempointer_image(&ptr);
        Ok(Datum::ByRef(mcx::slice_in(mcx, &image)?))
    }

    /// `PG_RETURN_CSTRING(pstrdup(buf))` — the C `cstring` result. The
    /// `Datum::Cstring` arm owns the text (no varlena header, no terminating NUL
    /// stored), mirroring the `pstrdup`'d C string.
    pub fn return_cstring<'mcx>(_mcx: Mcx<'mcx>, buf: alloc::string::String) -> PgResult<Datum<'mcx>> {
        Ok(Datum::Cstring(buf))
    }

    /// `PG_RETURN_BYTEA_P(...)` — the bytea result. `pq_endtypsend` has already
    /// stamped the varlena header onto the byte image; the by-reference
    /// `Datum::ByRef` arm carries that verbatim varlena image, copied into the
    /// caller's context.
    pub fn return_bytea<'mcx>(mcx: Mcx<'mcx>, bytes: alloc::vec::Vec<u8>) -> PgResult<Datum<'mcx>> {
        Ok(Datum::ByRef(mcx::slice_in(mcx, &bytes)?))
    }

    /// `common/hashfn.c` `hash_any(k, keylen)` — the Bob Jenkins hash, mapped to
    /// the merged owner's `hash_bytes` (pure, no-alloc; no seam install needed).
    pub fn hash_any(image: &[u8]) -> u32 {
        common_hashfn::hash_bytes(image)
    }

    /// `common/hashfn.c` `hash_any_extended(k, keylen, seed)` -> `hash_bytes_extended`.
    pub fn hash_any_extended(image: &[u8], seed: u64) -> u64 {
        common_hashfn::hash_bytes_extended(image, seed)
    }


    /// `utils/fmgr` `get_fn_expr_arg_stable(flinfo, n)` — whether the n-th
    /// function argument is a stable (non-volatile) expression; flinfo
    /// introspection lives in fmgr, reached here for the lead/lag/nth_value
    /// const-offset fast path.
    pub fn get_fn_expr_arg_stable(_argno: i32) -> bool {
        panic!("unported owner: get_fn_expr_arg_stable (utils/fmgr flinfo introspection)")
    }

    /// Dispatch for the `*_support` prosupport functions: inspect the request
    /// `Node*` (`SupportRequestWFuncMonotonic` / `SupportRequestOptimizeWindowClause`)
    /// and fill its fields. The node kinds and `monotonic`/`frameOptions` enums
    /// are owned by nodes/supportnodes.h + parsenodes.h, not ported.
    pub fn window_support<'mcx>(_rawreq: Datum<'_>) -> PgResult<Datum<'mcx>> {
        panic!(
            "unported owner: window *_support prosupport (nodes/supportnodes.h \
             SupportRequestWFuncMonotonic / SupportRequestOptimizeWindowClause)"
        )
    }

    /// The `WindowObject` accessor surface (`windowapi.h`), owned by the
    /// executor's `nodeWindowAgg.c`. Every function the windowfuncs.c bodies
    /// call lives here; the bodies' own arithmetic/control-flow is ported in the
    /// parent module, these are only the runtime crossings.
    pub mod win {
        use super::super::RankContext;
        use super::{Datum, NtileContext, WindowObject, WindowSeek};

        /// Sentinel for the C `NULL` `isout` out-parameter that
        /// first_value/last_value/nth_value pass to `WinGetFuncArgInFrame`.
        pub const NO_ISOUT: Option<&mut bool> = None;

        /// `PG_WINDOW_OBJECT()`.
        pub fn window_object() -> WindowObject {
            panic!("unported owner: PG_WINDOW_OBJECT (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinGetCurrentPosition(winobj)`.
        pub fn get_current_position(_winobj: WindowObject) -> i64 {
            panic!("unported owner: WinGetCurrentPosition (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinSetMarkPosition(winobj, pos)`.
        pub fn set_mark_position(_winobj: WindowObject, _pos: i64) {
            panic!("unported owner: WinSetMarkPosition (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinRowsArePeers(winobj, pos1, pos2)`.
        pub fn rows_are_peers(_winobj: WindowObject, _pos1: i64, _pos2: i64) -> bool {
            panic!("unported owner: WinRowsArePeers (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinGetPartitionRowCount(winobj)`.
        pub fn get_partition_row_count(_winobj: WindowObject) -> i64 {
            panic!("unported owner: WinGetPartitionRowCount (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinGetPartitionLocalMemory(winobj, sizeof(rank_context))` — the
        /// runtime hands back zero-initialized partition-local storage that
        /// persists across calls within a partition; modeled as a typed `&mut`.
        pub fn get_partition_local_memory_rank(_winobj: WindowObject) -> &'static mut RankContext {
            panic!(
                "unported owner: WinGetPartitionLocalMemory (windowapi.h / \
                 nodeWindowAgg.c) — rank_context"
            )
        }

        /// `WinGetPartitionLocalMemory(winobj, sizeof(ntile_context))`.
        pub fn get_partition_local_memory_ntile(
            _winobj: WindowObject,
        ) -> &'static mut NtileContext {
            panic!(
                "unported owner: WinGetPartitionLocalMemory (windowapi.h / \
                 nodeWindowAgg.c) — ntile_context"
            )
        }

        /// `WinGetFuncArgCurrent(winobj, argno, &isnull)`.
        pub fn get_func_arg_current<'mcx>(
            _winobj: WindowObject,
            _argno: i32,
            _isnull: &mut bool,
        ) -> Datum<'mcx> {
            panic!("unported owner: WinGetFuncArgCurrent (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinGetFuncArgInPartition(winobj, argno, relpos, seektype,
        /// set_mark, &isnull, &isout)`.
        #[allow(clippy::too_many_arguments)]
        pub fn get_func_arg_in_partition<'mcx>(
            _winobj: WindowObject,
            _argno: i32,
            _relpos: i32,
            _seektype: WindowSeek,
            _set_mark: bool,
            _isnull: &mut bool,
            _isout: &mut bool,
        ) -> Datum<'mcx> {
            panic!("unported owner: WinGetFuncArgInPartition (windowapi.h / nodeWindowAgg.c)")
        }

        /// `WinGetFuncArgInFrame(winobj, argno, relpos, seektype, set_mark,
        /// &isnull, isout)`.
        #[allow(clippy::too_many_arguments)]
        pub fn get_func_arg_in_frame<'mcx>(
            _winobj: WindowObject,
            _argno: i32,
            _relpos: i32,
            _seektype: WindowSeek,
            _set_mark: bool,
            _isnull: &mut bool,
            _isout: Option<&mut bool>,
        ) -> Datum<'mcx> {
            panic!("unported owner: WinGetFuncArgInFrame (windowapi.h / nodeWindowAgg.c)")
        }
    }
}
