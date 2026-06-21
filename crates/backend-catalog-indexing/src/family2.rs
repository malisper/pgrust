//! F2 — the catalog-tuple seam bodies that are NOT a pure
//! `heap_form_tuple` + engine over a crossed `*InsertRow` (the F1 shape in
//! [`crate::family1`]).
//!
//! These fall into three shapes, all faithful ports of their C callers:
//!
//!  * **Form-and-insert / form-and-update** over a deformed row carrier that
//!    F1's helpers don't already cover (`pg_type`, `pg_constraint`,
//!    `pg_shdepend`, `pg_foreign_*`, `pg_user_mapping`, `pg_largeobject`,
//!    `pg_db_role_setting`, `pg_sequence`): build `values[]`/`nulls[]`
//!    (`/replaces[]`) and call the [`crate::keystone`] engine.
//!
//!  * **Syscache-copy single-field writes** (`set_pg_class_reltoastrelid`,
//!    `set_relation_rule_status`, `rename_namespace_tuple`,
//!    `update_namespace_owner_tuple`, `catalog_tuple_update_typname_pg_type`,
//!    the foreign owner/alter updates, `catalog_*_pg_sequence`): the C
//!    `SearchSysCacheCopy1` + `GETSTRUCT` field poke. The owned model re-fetches
//!    the full tuple by an OID-keyed `systable_beginscan` over the caller's open
//!    relation (or one this body opens by `table_open`), deforms it, replaces
//!    the touched column(s), and `CatalogTupleUpdate`s — behaviour-identical to
//!    scribbling the `GETSTRUCT` field and re-storing (`heap_modify_tuple` with
//!    `replaces[]` false everywhere except the touched columns re-forms the
//!    row losslessly from the old tuple).
//!
//!  * **Read-side decode** (`get_catalog_object_by_oid`, `deform_lo_page`,
//!    `decode_db_role_setting_setconfig`): the C `systable` scan / `heap_getattr`
//!    + detoast, returning the decoded value.
//!
//! MCX / RELATION BRIDGE. The F2 seams that the C calls hold an already-open
//! relation cross it as `&RelationData<'_>` (no `mcx`, the relcache-projection
//! `Deref` target). The engine + `heap_form_tuple` + the genam scan need an
//! owned `&Relation<'mcx>` and an `Mcx<'mcx>` (mcx/lib.rs: no ambient context).
//! Each body therefore opens a private [`MemoryContext`] and re-opens the
//! relation by `rel.rd_id` (`table_open(rd_id, RowExclusiveLock)`) — the caller
//! already holds the lock, so the re-open is the cheap idempotent relcache
//! lookup the C `SearchSysCacheCopy1` / `CatalogTupleUpdate` already perform
//! against the same open relation. Seams that get an owned `&Relation<'mcx>` +
//! `mcx` (the `<'mcx>`-signed declarations) use those directly.

#![allow(non_snake_case)]

use mcx::{Mcx, MemoryContext};
use types_catalog as cat;
use types_core::fmgr::F_OIDEQ;
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult};
use types_rel::{Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::RowExclusiveLock;
use types_tuple::access::{
    RELKIND_INDEX, RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};
use types_tuple::heaptuple::ItemPointerData;

use backend_access_common_heaptuple::{heap_deform_tuple, heap_form_tuple, heap_modify_tuple};
use backend_access_common_scankey::ScanKeyInit;
use backend_access_index_genam_seams as genam;
use backend_access_table_table::table_open;

use crate::keystone::{
    CatalogIndexState, CatalogOpenIndexes, CatalogTupleDelete, CatalogTupleInsert,
    CatalogTupleInsertWithInfo, CatalogTupleUpdate, CatalogTupleUpdateWithInfo,
};

/* ======================================================================== *
 * Type / Anum constants not exported by the type crates.
 * ======================================================================== */

const TEXTOID: Oid = 25;
const ACLITEMOID: Oid = 1033;

// pg_namespace (CATALOG(pg_namespace,2615): oid, nspname, nspowner, nspacl).
const ANUM_PG_NAMESPACE_OID: i16 = 1;
const ANUM_PG_NAMESPACE_NSPNAME: i16 = 2;
const ANUM_PG_NAMESPACE_NSPOWNER: i16 = 3;
const ANUM_PG_NAMESPACE_NSPACL: i16 = 4;
const NATTS_PG_NAMESPACE: usize = 4;

// pg_class field positions (catalog/pg_class.h).
const ANUM_PG_CLASS_OID: i16 = 1;
const ANUM_PG_CLASS_RELPAGES: i16 = 10;
const ANUM_PG_CLASS_RELTUPLES: i16 = 11;
const ANUM_PG_CLASS_RELALLVISIBLE: i16 = 12;
const ANUM_PG_CLASS_RELALLFROZEN: i16 = 13;
const ANUM_PG_CLASS_RELTOASTRELID: i16 = 14;
const ANUM_PG_CLASS_RELHASINDEX: i16 = 15;
const ANUM_PG_CLASS_RELCHECKS: i16 = 20;
const ANUM_PG_CLASS_RELHASRULES: i16 = 21;
const ANUM_PG_CLASS_RELHASTRIGGERS: i16 = 22;
const ANUM_PG_CLASS_RELHASSUBCLASS: i16 = 23;
const ANUM_PG_CLASS_RELROWSECURITY: i16 = 24;
const ANUM_PG_CLASS_RELFORCEROWSECURITY: i16 = 25;
const ANUM_PG_CLASS_RELISPOPULATED: i16 = 26;
const ANUM_PG_CLASS_RELREPLIDENT: i16 = 27;
const ANUM_PG_CLASS_RELISPARTITION: i16 = 28;
const ANUM_PG_CLASS_RELREWRITE: i16 = 29;
const ANUM_PG_INDEX_INDEXRELID: i16 = 1;
const ANUM_PG_INDEX_INDISVALID: i16 = 11;
const ANUM_PG_INDEX_INDISREPLIDENT: i16 = 15;

// pg_sequence (CATALOG(pg_sequence,2224)): 8 fixed columns.
const ANUM_PG_SEQUENCE_SEQRELID: i16 = 1;
const ANUM_PG_SEQUENCE_SEQTYPID: i16 = 2;
const ANUM_PG_SEQUENCE_SEQSTART: i16 = 3;
const ANUM_PG_SEQUENCE_SEQINCREMENT: i16 = 4;
const ANUM_PG_SEQUENCE_SEQMAX: i16 = 5;
const ANUM_PG_SEQUENCE_SEQMIN: i16 = 6;
const ANUM_PG_SEQUENCE_SEQCACHE: i16 = 7;
const ANUM_PG_SEQUENCE_SEQCYCLE: i16 = 8;
const NATTS_PG_SEQUENCE: usize = 8;
const SEQUENCE_RELATION_ID: Oid = 2224;

/* ======================================================================== *
 * Small shared helpers.
 * ======================================================================== */

/// `namestrcpy(&name, src)` — a zero-filled 64-byte `NameData` image, truncated
/// to `NAMEDATALEN`, force-terminated at the last slot.
fn namestrcpy_image(src: &str) -> [u8; 64] {
    let mut name = [0u8; 64];
    for (i, &b) in src.as_bytes().iter().take(64).enumerate() {
        name[i] = b;
    }
    name[64 - 1] = 0;
    name
}

/// `NameGetDatum(&name)` over a 64-byte `NameData` image (a by-reference Datum
/// over the on-disk `name` bytes).
fn name_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8; 64]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &image[..])?))
}

/// `CStringGetTextDatum(s)` — a `text` varlena image (4-byte header
/// `SET_VARSIZE(len + VARHDRSZ)` then the payload), carried as `Datum::ByRef`.
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let payload = s.as_bytes();
    let total = 4 + payload.len();
    let word = (total as u32) << 2;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.extend_from_slice(&word.to_ne_bytes());
    buf.extend_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// The on-disk varlena header bytes of an `ArrayType` (the repo's `ArrayType`
/// expresses only the 16-byte header; the element payload follows out of line
/// in C). Used for the `pg_type.typacl` column, whose carrier is the header.
fn arraytype_header_bytes(arr: &types_array::ArrayType) -> [u8; 16] {
    let mut b = [0u8; 16];
    b[0..4].copy_from_slice(&arr.vl_len_.to_ne_bytes());
    b[4..8].copy_from_slice(&arr.ndim.to_ne_bytes());
    b[8..12].copy_from_slice(&arr.dataoffset.to_ne_bytes());
    b[12..16].copy_from_slice(&arr.elemtype.to_ne_bytes());
    b
}

/// Build a 1-D, no-nulls array varlena image directly from per-element on-disk
/// byte slices (the faithful manual rendering of `construct_md_array`'s layout,
/// avoiding the repo's Datum-pointer-forge element lane, which routes by-ref
/// elements through the not-yet-ported detoast subsystem). `elmlen == -1` marks
/// a varlena element type (each `elem` is its full varlena image including
/// header); a positive `elmlen` is a fixed-width by-value/by-ref element (each
/// `elem` is exactly `elmlen` bytes). The header is laid out exactly as
/// `construct_md_array` for the `hasnulls == false` path. `align`/`elmtype` are
/// the element type's alignment char / OID.
fn build_array_image<'mcx>(
    mcx: Mcx<'mcx>,
    elems: &[&[u8]],
    elmtype: Oid,
    elmlen: i32,
    align: u8,
) -> PgResult<mcx::PgVec<'mcx, u8>> {
    use backend_utils_adt_arrayfuncs::foundation;

    let nelems = elems.len() as i32;
    // overhead (no nulls) = ARR_OVERHEAD_NONULLS(1).
    let overhead = foundation::arr_overhead_nonulls(1);

    // Compute total: overhead + sum of aligned element lengths (att_align_nominal
    // then att_addlength), exactly as construct_md_array.
    let mut nbytes: usize = 0;
    for e in elems {
        nbytes = foundation::att_align_nominal(nbytes, align);
        let add = if elmlen == -1 { e.len() } else { elmlen as usize };
        nbytes += add;
    }
    let total = overhead + nbytes;

    let mut result: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    result.resize(total, 0);
    // SET_VARSIZE + ndim=1, dataoffset=0, elemtype; dims[0]=nelems; lbs[0]=1.
    foundation::set_header(&mut result, total, 1, 0, elmtype);
    foundation::write_dims(&mut result, &[nelems]);
    foundation::write_lbounds(&mut result, 1, &[1]);

    // Copy elements into the data area at aligned offsets.
    let data_off = foundation::arr_data_ptr_off(&result);
    let mut off: usize = 0;
    for e in elems {
        off = foundation::att_align_nominal(off, align);
        let copy_len = if elmlen == -1 { e.len() } else { elmlen as usize };
        let dst = data_off + off;
        result[dst..dst + copy_len].copy_from_slice(&e[..copy_len]);
        off += copy_len;
    }
    Ok(result)
}

/// Build a `text[]` array varlena image from a `Vec<String>` (each element a
/// `text` varlena), or `None` for an empty list (the C `PointerGetDatum(NULL)` →
/// store SQL NULL). Mirrors `construct_array_builtin(elems, n, TEXTOID)`.
fn text_array_datum<'mcx>(
    mcx: Mcx<'mcx>,
    items: &[String],
) -> PgResult<Option<Datum<'mcx>>> {
    if items.is_empty() {
        return Ok(None);
    }
    // Each element is a `text` varlena: 4-byte header (len+VARHDRSZ) then bytes.
    let images: Vec<Vec<u8>> = items
        .iter()
        .map(|s| {
            let payload = s.as_bytes();
            let total = 4 + payload.len();
            let word = (total as u32) << 2;
            let mut v = Vec::with_capacity(total);
            v.extend_from_slice(&word.to_ne_bytes());
            v.extend_from_slice(payload);
            v
        })
        .collect();
    let refs: Vec<&[u8]> = images.iter().map(|v| v.as_slice()).collect();
    // text: elmlen=-1, elmalign='i'.
    let bytes = build_array_image(mcx, &refs, TEXTOID, -1, b'i')?;
    Ok(Some(Datum::ByRef(bytes)))
}

/// `optionListToArray`'s result for `Option<Vec<(name, value)>>` options:
/// render each pair to the `"name=value"` text element, build the `text[]`, or
/// `None` for the absent/empty list (store SQL NULL).
fn options_array_datum<'mcx>(
    mcx: Mcx<'mcx>,
    options: &Option<Vec<(String, String)>>,
) -> PgResult<Option<Datum<'mcx>>> {
    match options {
        None => Ok(None),
        Some(pairs) => {
            let strings: Vec<String> = pairs
                .iter()
                .map(|(n, v)| {
                    let mut s = String::with_capacity(n.len() + 1 + v.len());
                    s.push_str(n);
                    s.push('=');
                    s.push_str(v);
                    s
                })
                .collect();
            text_array_datum(mcx, &strings)
        }
    }
}

/// Re-open the catalog relation the C caller already holds open, under a fresh
/// `mcx` (the relcache lookup is idempotent; the RowExclusiveLock is already
/// held). The OID is read off the crossed projection.
fn reopen<'mcx>(mcx: Mcx<'mcx>, rel: &RelationData<'_>) -> PgResult<Relation<'mcx>> {
    table_open(mcx, rel.rd_id, RowExclusiveLock)
}

/// An OID equality scan key: `ScanKeyInit(&k, attno, BTEqualStrategyNumber,
/// F_OIDEQ, ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i16, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(&mut key, attno, BTEqualStrategyNumber, F_OIDEQ, Datum::from_oid(value))?;
    Ok(key)
}

/// Fetch the single catalog tuple whose `oidcol == oid` by an OID-keyed
/// `systable` *heap* scan over the open relation (`index_ok = false` forces the
/// heap scan — the genam fallback `table_beginscan_catalog` path, behaviour-
/// identical to the index probe `SearchSysCacheCopy1` performs, since the OID
/// column is unique). Returns the row copied into `mcx`, or `None` when absent.
fn fetch_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oidcol: i16,
    oid: Oid,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let keys = [oid_key(oidcol, oid)?];
    let mut scan = genam::systable_beginscan::call(rel, InvalidOid, false, None, &keys)?;
    let tup = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    scan.end()?;
    Ok(tup)
}

/// Deform every column of `tup` against `rel`'s descriptor into the
/// `(value, isnull)` arrays C's `GETSTRUCT`/`heap_deform_tuple` produce.
fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<(Vec<Datum<'mcx>>, Vec<bool>)> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tup.tuple, &tupdesc, &tup.data)?;
    let mut values = Vec::with_capacity(cols.len());
    let mut nulls = Vec::with_capacity(cols.len());
    for (v, n) in cols.iter() {
        values.push(v.clone());
        nulls.push(*n);
    }
    Ok((values, nulls))
}

/// Re-form `oldtup` with the supplied `values`/`nulls`/`replaces` and
/// `CatalogTupleUpdate` at `oldtup->t_self` (`heap_modify_tuple` +
/// `CatalogTupleUpdate`).
fn modify_and_update<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    replaces: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, values, nulls, replaces)?;
    CatalogTupleUpdate(mcx, rel, oldtup.tuple.t_self, &mut tup)
}

/// `heap_form_tuple(rel->rd_att, values, nulls)` + `CatalogTupleInsert`.
fn form_and_insert<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, values, nulls)?;
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/// Rewrite an `acl` varlena column for an ownership change: deconstruct the
/// `aclitem[]` image into `AclItem`s, `aclnewowner(acl, old, new)`, then
/// `construct_array` the result back into a varlena. `aclitem` is fixed-length
/// 16 bytes, by-reference, `'d'`-aligned (`ACLITEMOID`).
fn acl_new_owner_datum<'mcx>(
    mcx: Mcx<'mcx>,
    acl_bytes: &[u8],
    old_owner: Oid,
    new_owner: Oid,
) -> PgResult<Datum<'mcx>> {
    use types_acl::acl::AclItem;

    // The acl column is a 1-D, no-nulls `aclitem[]` (16-byte fixed-width,
    // 'd'-aligned, by-reference elements). Read it through the canonical array
    // value lane (`deconstruct_array_values`), which yields each element as a
    // real `Datum::ByRef` carrying its verbatim 16 stored bytes — no
    // pointer-word surrogate, no hand-rolled stride walk.
    let elems = backend_utils_adt_arrayfuncs::construct::deconstruct_array_values_bytes(
        mcx,
        acl_bytes,
        ACLITEMOID,
        16,
        false,
        b'd' as core::ffi::c_char,
    )?;
    let mut old_acl: Vec<AclItem> = Vec::with_capacity(elems.len());
    for (val, isnull) in elems.iter() {
        if *isnull {
            // A stored ACL is always no-nulls (check_acl); a null here means a
            // corrupt catalog image.
            return Err(PgError::error("null aclitem array element"));
        }
        let b = val.as_ref_bytes();
        if b.len() < 16 {
            return Err(PgError::error("short aclitem array"));
        }
        old_acl.push(AclItem {
            ai_grantee: u32::from_ne_bytes([b[0], b[1], b[2], b[3]]),
            ai_grantor: u32::from_ne_bytes([b[4], b[5], b[6], b[7]]),
            ai_privs: u64::from_ne_bytes([b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]]),
        });
    }

    // newAcl = aclnewowner(oldAcl, oldOwner, newOwner);
    let new_acl =
        backend_utils_adt_acl::acl_ops::aclnewowner(mcx, &old_acl, old_owner, new_owner)?;

    // Re-encode the aclitem[] image through the canonical array value lane:
    // each `AclItem` becomes a `Datum::ByRef` of its 16-byte image, and
    // `construct_array_values` lays out the 1-D, no-nulls `aclitem[]` varlena.
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(new_acl.len());
    for item in new_acl.iter() {
        let mut buf = mcx::vec_with_capacity_in::<u8>(mcx, 16)?;
        buf.extend_from_slice(&item.ai_grantee.to_ne_bytes());
        buf.extend_from_slice(&item.ai_grantor.to_ne_bytes());
        buf.extend_from_slice(&item.ai_privs.to_ne_bytes());
        values.push(Datum::ByRef(buf));
    }
    let bytes = backend_utils_adt_arrayfuncs::construct::construct_array_values(
        mcx, &values, ACLITEMOID, 16, false, b'd',
    )?;
    Ok(Datum::ByRef(bytes))
}

/* ======================================================================== *
 * Engine pass-through seams (cluster family: &Relation<'mcx> + mcx, or
 * &RelationData via re-open).
 * ======================================================================== */

/// `CatalogTupleDelete(rel, tid)` (indexing.c).
fn catalog_tuple_delete(rel: &RelationData<'_>, tid: ItemPointerData) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_tuple_delete");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    CatalogTupleDelete(mcx, &r, tid)
}

/// `CatalogOpenIndexes(rel)` (indexing.c).
///
/// Returns the real owned [`CatalogIndexState`] tied to the caller's `mcx`. The
/// cluster / large-object consumers open the catalog under one `mcx`, hold the
/// returned value live across their `*_with_info_*` calls (which borrow it
/// `&mut`), and close it with [`catalog_close_indexes`] — exactly C's
/// `CatalogOpenIndexes` → `CatalogTupleUpdateWithInfo`* → `CatalogCloseIndexes`
/// lifecycle. The seam passes the caller's `mcx` and an `'mcx`-tied relation, so
/// the embedded open `Relation<'mcx>`s live as long as the caller's context.
fn catalog_open_indexes<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
) -> PgResult<CatalogIndexState<'mcx>> {
    CatalogOpenIndexes(mcx, rel)
}

/// `CatalogCloseIndexes(indstate)` (indexing.c): close the open index relations
/// (the locks are held until end-of-transaction) and drop the owned state.
fn catalog_close_indexes<'mcx>(indstate: CatalogIndexState<'mcx>) -> PgResult<()> {
    crate::keystone::CatalogCloseIndexes(indstate)
}

/// `CatalogTupleUpdate(pg_class_rel, &tup->t_self, tup)` after reforming the
/// mutated `PgClassForm` (indexing.c). The cluster swap reads the full pg_class
/// row, mutates the carried fields, and re-stores; the owner re-fetches the
/// on-disk tuple at `tid`, overwrites the 15 carried columns, and updates.
fn catalog_tuple_update_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    tid: ItemPointerData,
    form: &types_cluster::PgClassForm,
) -> PgResult<()> {
    let r = table_open(mcx, rel.rd_id, RowExclusiveLock)?;
    update_pg_class_from_form(mcx, &r, tid, form, None)
}

/// `ATExecSetRelOptions`'s pg_class row write (tablecmds.c:16758-16772): update
/// only the variable `pg_class.reloptions` (`text[]`) column of `relid`.
///
/// `new_reloptions` is the constructed `text[]` varlena image
/// (`transformRelOptions`), or `None` for the C `(Datum) 0` (store SQL NULL).
/// Mirrors C: `table_open(RelationRelationId, RowExclusiveLock)` →
/// `SearchSysCacheLocked1(RELOID)` → set `repl_val[Anum_pg_class_reloptions]` (or
/// `repl_null`) with `repl_repl = true` → `heap_modify_tuple` →
/// `CatalogTupleUpdate`. All other columns ride from the old tuple
/// (`replaces[]` false), exactly as the C `repl_repl` memset-to-false leaves
/// them. `Err` "cache lookup failed for relation %u" when the tuple is missing.
fn update_pg_class_reloptions_seam<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    new_reloptions: Option<&[u8]>,
) -> PgResult<()> {
    let pg_class = table_open(mcx, cat::pg_class::RelationRelationId, RowExclusiveLock)?;
    let oldtup = fetch_by_oid(mcx, &pg_class, cat::pg_class::Anum_pg_class_oid, relid)?
        .ok_or_else(|| PgError::error(format!("cache lookup failed for relation {relid}")))?;
    let (mut values, mut nulls) = deform(mcx, &pg_class, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    let anum = cat::pg_class::Anum_pg_class_reloptions;
    match new_reloptions {
        // repl_val[Anum_pg_class_reloptions - 1] = newOptions: the text[] varlena
        // rides as a Datum::ByRef carrying its verbatim image.
        Some(bytes) => {
            let d = Datum::ByRef(mcx::slice_in(mcx, bytes)?);
            set_col(&mut values, &mut nulls, &mut replaces, anum, d);
        }
        // repl_null[Anum_pg_class_reloptions - 1] = true.
        None => {
            let i = (anum - 1) as usize;
            nulls[i] = true;
            replaces[i] = true;
        }
    }

    modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    pg_class.close(RowExclusiveLock)?;
    Ok(())
}

/// `CatalogTupleUpdateWithInfo(rel, &tup->t_self, tup, indstate)`. `rel` and
/// `indstate` are the caller's open pg_class relation and its open index state,
/// both tied to the caller's `mcx` (so the index state is opened once by the
/// caller and reused across both swap rows, exactly as C amortizes
/// `CatalogOpenIndexes`).
fn catalog_tuple_update_with_info_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
    form: &types_cluster::PgClassForm,
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    update_pg_class_from_form(mcx, rel, tid, form, Some(indstate))
}

/// Shared pg_class swap-row writer: read the on-disk tuple at `tid`, overwrite
/// the columns the cluster swap carries in [`types_cluster::PgClassForm`], and
/// `CatalogTupleUpdate{,WithInfo}`. The remaining pg_class columns are taken
/// from the old tuple (`replaces[]` false), matching the C
/// `GETSTRUCT`-mutate-then-update.
fn update_pg_class_from_form<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
    form: &types_cluster::PgClassForm,
    indstate: Option<&mut CatalogIndexState<'mcx>>,
) -> PgResult<()> {
    // Fetch the existing tuple at tid via an OID-keyed scan? The cluster swap
    // addresses by tid; re-read the full tuple by scanning for it. The OID is
    // not separately carried, so we read the tuple at tid through a heap scan
    // keyed on oid — but we have only the tid. Instead, fetch by tid directly:
    // C holds the SearchSysCacheCopy1 tuple. The owner re-derives it by reading
    // pg_class's oid column from the form is not possible (oid is not carried).
    // We therefore read the addressed row via the relation's oid index using
    // the relid the cluster swap already opened: rel.rd_id is pg_class itself,
    // not the target relation. The target relation OID lives in neither the tid
    // nor the form, so the only addressable handle is the tid. We re-fetch the
    // tuple by scanning the heap for the matching t_self.
    let oldtup = fetch_by_tid(mcx, rel, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_class tuple for update"))?;

    let (mut values, mut nulls) = deform(mcx, rel, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    // The Form_pg_class columns swap_relation_files / mark_index_clustered
    // mutate (catalog/pg_class.h field order). relname (2) through relminmxid
    // (31) carried by PgClassForm; oid (1) and the trailing acl/options are not
    // touched.
    set_col(&mut values, &mut nulls, &mut replaces, 2, name_datum(mcx, &namestrcpy_image(&form.relname))?);
    set_col(&mut values, &mut nulls, &mut replaces, 3, Datum::from_oid(form.relnamespace));
    set_col(&mut values, &mut nulls, &mut replaces, 8, Datum::from_oid(form.relfilenode));
    set_col(&mut values, &mut nulls, &mut replaces, 9, Datum::from_oid(form.reltablespace));
    set_col(&mut values, &mut nulls, &mut replaces, 7, Datum::from_oid(form.relam));
    set_col(&mut values, &mut nulls, &mut replaces, 14, Datum::from_oid(form.reltoastrelid));
    set_col(&mut values, &mut nulls, &mut replaces, 16, Datum::from_bool(form.relisshared));
    set_col(&mut values, &mut nulls, &mut replaces, 17, Datum::from_char(form.relpersistence as i8));
    set_col(&mut values, &mut nulls, &mut replaces, 18, Datum::from_char(form.relkind as i8));
    set_col(&mut values, &mut nulls, &mut replaces, 19, Datum::from_i16(form.relnatts));
    set_col(&mut values, &mut nulls, &mut replaces, 10, Datum::from_i32(form.relpages));
    set_col(&mut values, &mut nulls, &mut replaces, 11, Datum::from_f32(form.reltuples));
    set_col(&mut values, &mut nulls, &mut replaces, 12, Datum::from_i32(form.relallvisible));
    set_col(&mut values, &mut nulls, &mut replaces, 13, Datum::from_i32(form.relallfrozen));
    set_col(&mut values, &mut nulls, &mut replaces, 30, Datum::from_transaction_id(form.relfrozenxid));
    set_col(&mut values, &mut nulls, &mut replaces, 31, Datum::from_u32(form.relminmxid));

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, &oldtup, &tupdesc, &values, &nulls, &replaces)?;
    match indstate {
        // *_with_info: update using the caller's already-open index state.
        Some(indstate) => CatalogTupleUpdateWithInfo(mcx, rel, tid, &mut tup, indstate),
        None => CatalogTupleUpdate(mcx, rel, tid, &mut tup),
    }
}

/// `CatalogTupleUpdate(pg_index_rel, &tup->t_self, tup)` after reforming the
/// mutated `PgIndexForm` (indexing.c). `mark_index_clustered` toggles
/// `indisclustered` (and reads `indisvalid`).
fn catalog_tuple_update_pg_index<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'_>,
    tid: ItemPointerData,
    form: &types_cluster::PgIndexForm,
) -> PgResult<()> {
    let r = table_open(mcx, rel.rd_id, RowExclusiveLock)?;
    let oldtup = fetch_by_tid(mcx, &r, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_index tuple for update"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    // pg_index field order: indexrelid(1) indrelid(2) indnatts(3) indnkeyatts(4)
    // indisunique(5) indnullsnotdistinct(6) indisprimary(7) indisexclusion(8)
    // indimmediate(9) indisclustered(10) indisvalid(11) indcheckxmin(12)
    // indisready(13) indislive(14) indisreplident(15) ...
    //
    // The carrier is the writable `GETSTRUCT(Form_pg_index)` copy the callers
    // (`mark_index_clustered`, `index_set_state_flags`,
    // `index_constraint_create`) mutate; here we write every flag column it
    // carries back into the heap tuple (the C `CatalogTupleUpdate` of the whole
    // modified tuple — the unchanged flag columns simply get rewritten with
    // their re-read values, which is behaviour-identical to the C overwriting
    // the single `GETSTRUCT` view in place).
    set_col(&mut values, &mut nulls, &mut replaces, 7, Datum::from_bool(form.indisprimary));
    set_col(&mut values, &mut nulls, &mut replaces, 9, Datum::from_bool(form.indimmediate));
    set_col(&mut values, &mut nulls, &mut replaces, 10, Datum::from_bool(form.indisclustered));
    set_col(&mut values, &mut nulls, &mut replaces, 11, Datum::from_bool(form.indisvalid));
    set_col(&mut values, &mut nulls, &mut replaces, 12, Datum::from_bool(form.indcheckxmin));
    set_col(&mut values, &mut nulls, &mut replaces, 13, Datum::from_bool(form.indisready));
    set_col(&mut values, &mut nulls, &mut replaces, 14, Datum::from_bool(form.indislive));
    set_col(&mut values, &mut nulls, &mut replaces, 15, Datum::from_bool(form.indisreplident));
    modify_and_update_tid(mcx, &r, &oldtup, &values, &nulls, &replaces, tid)
}

/// `update_relispartition(relationId, newval)` (commands/indexcmds.c:4574) —
/// subroutine of `IndexSetParentIndex`: open pg_class RowExclusiveLock,
/// `SearchSysCacheLockedCopy1(RELOID, relationId)`, set `relispartition` to
/// `newval`, `CatalogTupleUpdate`. The C asserts `cur != newval`; we set
/// unconditionally (the only callers always flip the value). Returns
/// `HeapTupleIsValid(tuple)`.
fn update_relispartition_catalog(relation_id: Oid, newval: bool) -> PgResult<bool> {
    let ctx = MemoryContext::new("update_relispartition_catalog");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relation_id)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &pg_class, &oldtup)?;
    let mut values = values;
    let mut nulls = nulls;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_CLASS_RELISPARTITION,
        Datum::from_bool(newval),
    );
    modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `pg_partitioned_table.partdefid` column (`pg_partitioned_table.h`):
/// partrelid=1, partstrat=2, partnatts=3, partdefid=4.
const ANUM_PG_PARTITIONED_TABLE_PARTRELID: i16 = 1;
const ANUM_PG_PARTITIONED_TABLE_PARTDEFID: i16 = 4;

/// `update_default_partition_oid(parentId, defaultPartId)`
/// (catalog/partition.c:340): set `pg_partitioned_table.partdefid` of the
/// partitioned table `parentId` to `defaultPartId`. Mirrors C exactly:
/// `table_open(PartitionedRelationId, RowExclusiveLock)` →
/// `SearchSysCacheCopy1(PARTRELID, parentId)` → in-place `partdefid` write →
/// `CatalogTupleUpdate` → `heap_freetuple` → `table_close`. On a cache miss the
/// C `elog(ERROR, "cache lookup failed for partition key of relation %u")`.
fn update_default_partition_oid_catalog(parent_id: Oid, default_part_id: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("update_default_partition_oid_catalog");
    let mcx = ctx.mcx();
    let pg_partitioned_table =
        table_open(mcx, cat::catalog::PARTITIONED_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(
        mcx,
        &pg_partitioned_table,
        ANUM_PG_PARTITIONED_TABLE_PARTRELID,
        parent_id,
    )?
    else {
        pg_partitioned_table.close(RowExclusiveLock)?;
        return Err(PgError::error(format!(
            "cache lookup failed for partition key of relation {parent_id}"
        )));
    };
    let (values, nulls) = deform(mcx, &pg_partitioned_table, &oldtup)?;
    let mut values = values;
    let mut nulls = nulls;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_PARTITIONED_TABLE_PARTDEFID,
        Datum::from_oid(default_part_id),
    );
    modify_and_update(mcx, &pg_partitioned_table, &oldtup, &values, &nulls, &replaces)?;
    pg_partitioned_table.close(RowExclusiveLock)?;
    Ok(())
}

/// `DefineIndex`'s partitioned-recursion `invalidate_parent` update
/// (indexcmds.c:1573): open pg_index RowExclusiveLock,
/// `SearchSysCache1(INDEXRELID)`, `heap_copytuple`, set `indisvalid = false`,
/// `CatalogTupleUpdate`. Returns `HeapTupleIsValid(tup)`.
fn index_mark_invalid_catalog(index_relation_id: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("index_mark_invalid_catalog");
    let mcx = ctx.mcx();
    let pg_index = table_open(mcx, types_core::catalog::INDEX_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_index, ANUM_PG_INDEX_INDEXRELID, index_relation_id)?
    else {
        pg_index.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &pg_index, &oldtup)?;
    let mut values = values;
    let mut nulls = nulls;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_INDEX_INDISVALID,
        Datum::from_bool(false),
    );
    modify_and_update(mcx, &pg_index, &oldtup, &values, &nulls, &replaces)?;
    pg_index.close(RowExclusiveLock)?;
    Ok(true)
}

/// `validatePartitionedIndex`'s pg_index update (tablecmds.c:21877): set
/// `indisvalid = true` transactionally for the partitioned index. Returns
/// `false` if the pg_index row is gone (the `cache lookup failed` ereport is
/// raised by the caller seam, mirroring `index_mark_invalid_catalog`).
fn index_mark_valid_catalog(index_relation_id: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("index_mark_valid_catalog");
    let mcx = ctx.mcx();
    let pg_index = table_open(mcx, types_core::catalog::INDEX_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_index, ANUM_PG_INDEX_INDEXRELID, index_relation_id)?
    else {
        pg_index.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &pg_index, &oldtup)?;
    let mut values = values;
    let mut nulls = nulls;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_INDEX_INDISVALID,
        Datum::from_bool(true),
    );
    modify_and_update(mcx, &pg_index, &oldtup, &values, &nulls, &replaces)?;
    pg_index.close(RowExclusiveLock)?;
    Ok(true)
}

/// `set_col(values, nulls, replaces, anum, datum)` — write a 1-based column.
fn set_col<'mcx>(
    values: &mut [Datum<'mcx>],
    nulls: &mut [bool],
    replaces: &mut [bool],
    anum: i16,
    datum: Datum<'mcx>,
) {
    let i = (anum - 1) as usize;
    values[i] = datum;
    nulls[i] = false;
    replaces[i] = true;
}

/// `heap_modify_tuple` + `CatalogTupleUpdate` at an explicit `tid` (used where
/// the addressed TID differs from the re-read tuple's `t_self`, which it does
/// not here, but keeps the call site explicit).
fn modify_and_update_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    oldtup: &FormedTuple<'mcx>,
    values: &[Datum<'mcx>],
    nulls: &[bool],
    replaces: &[bool],
    tid: ItemPointerData,
) -> PgResult<()> {
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, oldtup, &tupdesc, values, nulls, replaces)?;
    CatalogTupleUpdate(mcx, rel, tid, &mut tup)
}

/// Fetch the tuple at `tid` by a full heap scan matching `t_self` (the cluster
/// swap addresses by TID; the owner re-reads the row to supply
/// `heap_modify_tuple`'s base). This is the genam heap scan with no key,
/// stopping at the matching item pointer.
fn fetch_by_tid<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tid: ItemPointerData,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    let keys: [ScanKeyData<'mcx>; 0] = [];
    let mut scan = genam::systable_beginscan::call(rel, InvalidOid, false, None, &keys)?;
    loop {
        let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
            scan.end()?;
            return Ok(None);
        };
        if tup.tuple.t_self == tid {
            scan.end()?;
            return Ok(Some(tup));
        }
    }
}

/* ======================================================================== *
 * pg_class single-field writes (toasting.c / rewriteSupport.c).
 * ======================================================================== */

/// `set_pg_class_reltoastrelid` (toasting.c, normal path): re-fetch the
/// pg_class row for `rel_oid`, write `reltoastrelid = toast_relid`, and
/// `CatalogTupleUpdate`. Returns `HeapTupleIsValid(reltup)`.
fn set_pg_class_reltoastrelid(
    class_rel: &RelationData<'_>,
    rel_oid: Oid,
    toast_relid: Oid,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_reltoastrelid");
    let mcx = ctx.mcx();
    let r = reopen(mcx, class_rel)?;
    let Some(oldtup) = fetch_by_oid(mcx, &r, ANUM_PG_CLASS_OID, rel_oid)? else {
        return Ok(false);
    };
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_CLASS_RELTOASTRELID,
        Datum::from_oid(toast_relid),
    );
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)?;
    Ok(true)
}

/// `set_pg_class_reltoastrelid_inplace` (toasting.c, bootstrap path): the C uses
/// `systable_inplace_update_*` because a transactional UPDATE is not possible in
/// bootstrap. The genam owner exposes the begin→mutate→finish flow as
/// `systable_inplace_update`; the mutation overwrites the fixed-size
/// `reltoastrelid` Oid field in the tuple's user-data area in place.
fn set_pg_class_reltoastrelid_inplace(
    class_rel: &RelationData<'_>,
    rel_oid: Oid,
    toast_relid: Oid,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_reltoastrelid_inplace");
    let mcx = ctx.mcx();
    let r = reopen(mcx, class_rel)?;

    // The fixed offset of reltoastrelid within the tuple's user-data area:
    // pg_class's leading columns are all fixed-width and non-null in a
    // bootstrap-created relation, so the field sits at a constant byte offset.
    // Rather than hardcode the offset, deform the tuple's descriptor to find
    // the column's start, then poke the 4 Oid bytes in `mutate`.
    let tupdesc = r.rd_att_clone_in(mcx)?;
    // Compute the byte offset of column reltoastrelid in the data area.
    let off = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELTOASTRELID)
        .ok_or_else(|| PgError::error("reltoastrelid not at a fixed offset"))?;

    let keys = [oid_key(ANUM_PG_CLASS_OID, rel_oid)?];
    let new_oid = toast_relid;
    let mut mutate = |data: &mut [u8]| -> PgResult<bool> {
        if off + 4 > data.len() {
            return Err(PgError::error("reltoastrelid offset out of range"));
        }
        data[off..off + 4].copy_from_slice(&new_oid.to_ne_bytes());
        // bootstrap path always overwrites reltoastrelid → always dirty.
        Ok(true)
    };
    let res = genam::systable_inplace_update::call(
        mcx,
        &r,
        cat::catalog::CLASS_OID_INDEX_ID,
        true,
        &keys,
        &mut mutate,
    )?;
    Ok(res.is_some())
}

/// `index_update_stats(rel, hasindex, reltuples)` (catalog/index.c) — the
/// non-transactional (`systable_inplace_update`) write of `rel`'s `pg_class`
/// row that finishes `index_build` / `index_create` / `reindex`. Sets
/// `relhasindex`, and (when `reltuples >= 0 && !IsBinaryUpgrade`, subject to the
/// autovacuum/relkind relstats rules) `relpages` / `reltuples` /
/// `relallvisible` / `relallfrozen`, then commits via `_finish` (WAL + cache
/// inval) when any column changed, or `_cancel` +
/// `CacheInvalidateRelcacheByTuple` when nothing changed.
///
/// The C edits `(Form_pg_class) GETSTRUCT(tuple)` in place under the locked
/// pg_class buffer. The combined `systable_inplace_update` seam supplies that
/// user-data byte area to the `mutate` callback, which reads the live column
/// bytes (to decide `dirty`), pokes the new fixed-width values at their
/// descriptor offsets, and returns the `dirty` flag the owner uses to choose
/// `_finish` vs `_cancel`. pg_class columns 10..=15 are all fixed-width and
/// precede the variable-length tail (`relacl`@32 …), so each sits at a constant
/// data-area offset computable from the descriptor.
fn index_update_stats(rel: &Relation<'_>, hasindex: bool, reltuples: f64) -> PgResult<()> {
    let ctx = MemoryContext::new("index_update_stats");
    let mcx = ctx.mcx();

    let relid = rel.rd_id;

    let mut reltuples = reltuples;

    /*
     * As a special hack, if we are dealing with an empty table and the
     * existing reltuples is -1, we leave that alone. This ensures that
     * creating an index as part of CREATE TABLE doesn't cause the table to
     * prematurely look like it's been vacuumed.
     */
    if reltuples == 0.0 && (rel.rd_rel.reltuples as f64) < 0.0 {
        reltuples = -1.0;
    }

    /*
     * Don't update statistics during binary upgrade, because the indexes are
     * created before the data is moved into place.
     */
    let mut update_stats = reltuples >= 0.0 && !backend_utils_init_small_seams::is_binary_upgrade::call();

    /*
     * If autovacuum is off, user may not be expecting table relstats to
     * change. Preserve any restored table statistics in that case.
     */
    if rel.rd_rel.relkind == RELKIND_RELATION
        || rel.rd_rel.relkind == RELKIND_TOASTVALUE
        || rel.rd_rel.relkind == RELKIND_MATVIEW
    {
        if backend_postmaster_autovacuum_seams::auto_vacuuming_active::call() {
            // StdRdOptions *options = (StdRdOptions *) rel->rd_options;
            if let Some(options) = rel.rd_options.as_ref() {
                if !options.autovacuum.enabled {
                    update_stats = false;
                }
            }
        } else {
            update_stats = false;
        }
    }

    /*
     * Finish I/O and visibility map buffer locks before the inplace update
     * locks the pg_class buffer.
     */
    let mut relpages: u32 = 0;
    let mut relallvisible: u32 = 0;
    let mut relallfrozen: u32 = 0;
    if update_stats {
        relpages = backend_utils_cache_relcache_seams::relation_get_number_of_blocks::call(
            &reopen_self(mcx, rel)?,
        )?;

        if rel.rd_rel.relkind != RELKIND_INDEX {
            let (av, af) =
                backend_access_heap_vacuumlazy_seams::visibilitymap_count::call(&reopen_self(mcx, rel)?)?;
            relallvisible = av;
            relallfrozen = af;
        }
    }

    /*
     * Always update via a non-transactional, overwrite-in-place update (see
     * the three reasons in catalog/index.c: bootstrap, reindexing pg_class
     * itself, and the share-lock concurrent-CREATE-INDEX race).
     *
     * C: `pg_class = table_open(RelationRelationId, RowExclusiveLock)`. The row
     * being updated belongs to `rel`, but the relation we open and poke in
     * place is `pg_class` itself — so the column byte offsets must be computed
     * from pg_class's descriptor, not from `rel`'s.
     */
    let r = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;

    // Compute the fixed data-area byte offsets of every column we touch.
    let tupdesc = r.rd_att_clone_in(mcx)?;
    let off_relpages = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELPAGES)
        .ok_or_else(|| PgError::error("pg_class relpages not at a fixed offset"))?;
    let off_reltuples = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELTUPLES)
        .ok_or_else(|| PgError::error("pg_class reltuples not at a fixed offset"))?;
    let off_relallvisible = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELALLVISIBLE)
        .ok_or_else(|| PgError::error("pg_class relallvisible not at a fixed offset"))?;
    let off_relallfrozen = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELALLFROZEN)
        .ok_or_else(|| PgError::error("pg_class relallfrozen not at a fixed offset"))?;
    let off_relhasindex = fixed_attr_offset(&tupdesc, ANUM_PG_CLASS_RELHASINDEX)
        .ok_or_else(|| PgError::error("pg_class relhasindex not at a fixed offset"))?;

    let keys = [oid_key(ANUM_PG_CLASS_OID, relid)?];

    // The mutate callback mirrors C's `rd_rel = GETSTRUCT(tuple)` read-poke:
    // compute `dirty` by comparing the live column bytes against the new
    // values, write the changed ones in place, and return `dirty`. We also keep
    // the flag in `was_dirty` so the cancel branch below can issue the extra
    // `CacheInvalidateRelcacheByTuple`.
    let mut was_dirty = false;
    let was_dirty_ref = &mut was_dirty;
    let mut mutate = |data: &mut [u8]| -> PgResult<bool> {
        // relhasindex is a bool (1 byte).
        if off_relhasindex >= data.len() {
            return Err(PgError::error("relhasindex offset out of range"));
        }
        let mut dirty = false;
        if (data[off_relhasindex] != 0) != hasindex {
            data[off_relhasindex] = hasindex as u8;
            dirty = true;
        }

        if update_stats {
            // relpages, relallvisible, relallfrozen are int4; reltuples is float4.
            if off_reltuples + 4 > data.len()
                || off_relallvisible + 4 > data.len()
                || off_relallfrozen + 4 > data.len()
            {
                return Err(PgError::error("pg_class stats column offset out of range"));
            }
            let new_relpages = relpages as i32;
            let new_reltuples = reltuples as f32;
            let new_relallvisible = relallvisible as i32;
            let new_relallfrozen = relallfrozen as i32;

            let cur_relpages = i32::from_ne_bytes(
                data[off_relpages..off_relpages + 4].try_into().unwrap(),
            );
            if cur_relpages != new_relpages {
                data[off_relpages..off_relpages + 4].copy_from_slice(&new_relpages.to_ne_bytes());
                dirty = true;
            }
            let cur_reltuples = f32::from_ne_bytes(
                data[off_reltuples..off_reltuples + 4].try_into().unwrap(),
            );
            if cur_reltuples != new_reltuples {
                data[off_reltuples..off_reltuples + 4].copy_from_slice(&new_reltuples.to_ne_bytes());
                dirty = true;
            }
            let cur_relallvisible = i32::from_ne_bytes(
                data[off_relallvisible..off_relallvisible + 4].try_into().unwrap(),
            );
            if cur_relallvisible != new_relallvisible {
                data[off_relallvisible..off_relallvisible + 4]
                    .copy_from_slice(&new_relallvisible.to_ne_bytes());
                dirty = true;
            }
            let cur_relallfrozen = i32::from_ne_bytes(
                data[off_relallfrozen..off_relallfrozen + 4].try_into().unwrap(),
            );
            if cur_relallfrozen != new_relallfrozen {
                data[off_relallfrozen..off_relallfrozen + 4]
                    .copy_from_slice(&new_relallfrozen.to_ne_bytes());
                dirty = true;
            }
        }
        *was_dirty_ref = dirty;
        Ok(dirty)
    };

    let tid = genam::systable_inplace_update::call(
        mcx,
        &r,
        cat::catalog::CLASS_OID_INDEX_ID,
        true,
        &keys,
        &mut mutate,
    )?;

    // !HeapTupleIsValid(tuple) → elog(ERROR, "could not find tuple ...").
    if tid.is_none() {
        return Err(PgError::error(format!(
            "could not find tuple for relation {relid}"
        )));
    }

    /*
     * When nothing changed, the seam ran `systable_inplace_update_cancel` (no
     * WAL). C then still issues `CacheInvalidateRelcacheByTuple(tuple)` so the
     * new index's catalog rows force a relcache rebuild when they become
     * visible. That inval reads only the row's `oid` (= relid) and `relisshared`
     * — both carried by the relation projection — so build the trimmed
     * `PgClassForm` from `rel` and invalidate. (When dirty, the seam's `_finish`
     * already sent transactional + immediate cache invals.)
     */
    if !was_dirty {
        let form = types_cluster::PgClassForm {
            relisshared: rel.rd_rel.relisshared,
            ..types_cluster::PgClassForm::default()
        };
        backend_utils_cache_inval_seams::cache_invalidate_relcache_by_pg_class::call(relid, &form)?;
    }

    Ok(())
}

/// Re-open `rel` for the `RelationGetNumberOfBlocks` smgr probe. The C reads the
/// block count off the same passed `rel`; the owned model re-acquires the
/// cache-carrying `Relation` (idempotent relcache lookup, lock already held).
///
/// `index_update_stats` runs for both heap relations AND indexes (C's
/// `RelationGetNumberOfBlocks(rel)` is relkind-agnostic), so the re-open must go
/// through `relation_open` — NOT `table_open`, which rejects an index relkind
/// with "cannot open relation ... not supported for indexes". The toast-index
/// `index_build` -> `index_update_stats` path is exactly such an index caller.
fn reopen_self<'mcx>(mcx: Mcx<'mcx>, rel: &RelationData<'_>) -> PgResult<Relation<'mcx>> {
    backend_access_common_relation_seams::relation_open::call(
        mcx,
        rel.rd_id,
        types_storage::lock::NoLock,
    )
}

/// The fixed byte offset, within a heap tuple's user-data area, of the 1-based
/// fixed-width column `anum`, assuming every preceding column is fixed-width and
/// non-null (true for pg_class's leading columns in a bootstrap row). Returns
/// `None` if a preceding column is variable-length.
fn fixed_attr_offset(
    tupdesc: &types_tuple::heaptuple::TupleDescData<'_>,
    anum: i16,
) -> Option<usize> {
    use backend_utils_adt_arrayfuncs::foundation;
    let mut off: usize = 0;
    for i in 0..(anum as usize - 1) {
        let att = tupdesc.attr(i);
        if att.attlen < 0 {
            return None;
        }
        off = foundation::att_align_nominal(off, att.attalign as u8);
        off += att.attlen as usize;
    }
    // Align the target column itself.
    let att = tupdesc.attr(anum as usize - 1);
    off = foundation::att_align_nominal(off, att.attalign as u8);
    Some(off)
}

/// `SetRelationNumChecks`'s disk-store branch (heap.c): open pg_class
/// RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`, set
/// `relStruct->relchecks = numchecks`, and `CatalogTupleUpdate`. The caller
/// (`backend-catalog-heap`) has already confirmed the stored value differs from
/// `numchecks` (and owns the equal-value `CacheInvalidateRelcache` branch), so
/// this is the unconditional store. `Ok(())` maps the C `cache lookup failed for
/// relation %u` elog(ERROR) to an error when no tuple is found.
fn set_relation_num_checks(relid: Oid, numchecks: i32) -> PgResult<()> {
    let ctx = MemoryContext::new("set_relation_num_checks");
    let mcx = ctx.mcx();
    let r = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let oldtup = fetch_by_oid(mcx, &r, ANUM_PG_CLASS_OID, relid)?.ok_or_else(|| {
        PgError::error(format!("cache lookup failed for relation {relid}"))
    })?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    // relStruct->relchecks = numchecks (int2 in pg_class).
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_CLASS_RELCHECKS,
        Datum::from_i16(numchecks as i16),
    );
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)?;
    r.close(RowExclusiveLock)?;
    Ok(())
}

/// `SetRelationRuleStatus` (rewriteSupport.c): re-fetch the pg_class row for
/// `relation_id`; if `relhasrules != rel_has_rules`, set it and
/// `CatalogTupleUpdate`; otherwise `CacheInvalidateRelcacheByTuple` to force a
/// relcache rebuild anyway. Returns `HeapTupleIsValid(tuple)`.
fn set_relation_rule_status(
    class_rel: &RelationData<'_>,
    relation_id: Oid,
    rel_has_rules: bool,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_relation_rule_status");
    let mcx = ctx.mcx();
    let r = reopen(mcx, class_rel)?;
    let Some(oldtup) = fetch_by_oid(mcx, &r, ANUM_PG_CLASS_OID, relation_id)? else {
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &r, &oldtup)?;
    let cur = values[(ANUM_PG_CLASS_RELHASRULES - 1) as usize].as_bool();
    if cur != rel_has_rules {
        let mut values = values;
        let mut nulls = nulls;
        let mut replaces = vec![false; values.len()];
        set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            ANUM_PG_CLASS_RELHASRULES,
            Datum::from_bool(rel_has_rules),
        );
        modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)?;
    } else {
        // CacheInvalidateRelcacheByTuple(tuple): the owner reads oid + relisshared
        // from the reformed pg_class row. relisshared is column 16.
        let relisshared = values[(16 - 1) as usize].as_bool();
        let form = pg_class_form_for_inval(relation_id, relisshared);
        backend_utils_cache_inval_seams::cache_invalidate_relcache_by_pg_class::call(
            relation_id,
            &form,
        )?;
    }
    Ok(true)
}

/// `SetRelationHasSubclass` catalog body (tablecmds.c:3647): open pg_class
/// RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`; if
/// `relhassubclass != relhassubclass`, set the field and `CatalogTupleUpdate`,
/// otherwise `CacheInvalidateRelcacheByTuple` to force a relcache rebuild
/// anyway. Returns `HeapTupleIsValid(tuple)`. Same update-or-invalidate shape
/// as `set_relation_rule_status`; the field-write must run against the owner's
/// full pg_class copy here, not the trimmed command-layer `PgClassForm`.
fn set_relation_has_subclass_catalog(relid: Oid, relhassubclass: bool) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_relation_has_subclass_catalog");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &pg_class, &oldtup)?;
    let cur = values[(ANUM_PG_CLASS_RELHASSUBCLASS - 1) as usize].as_bool();
    if cur != relhassubclass {
        let mut values = values;
        let mut nulls = nulls;
        let mut replaces = vec![false; values.len()];
        set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            ANUM_PG_CLASS_RELHASSUBCLASS,
            Datum::from_bool(relhassubclass),
        );
        modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    } else {
        // CacheInvalidateRelcacheByTuple(tuple): relisshared is column 16.
        let relisshared = values[(16 - 1) as usize].as_bool();
        let form = pg_class_form_for_inval(relid, relisshared);
        backend_utils_cache_inval_seams::cache_invalidate_relcache_by_pg_class::call(relid, &form)?;
    }
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `CreateTrigger`'s pg_class `relhastriggers` poke (commands/trigger.c:1016):
/// open pg_class RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`; if
/// `relhastriggers` is not already set, set it, `CatalogTupleUpdate`, and
/// `CommandCounterIncrement`; otherwise `CacheInvalidateRelcacheByTuple` to
/// force a relcache rebuild anyway. Returns `HeapTupleIsValid(tuple)`. Same
/// update-or-invalidate shape as `set_relation_has_subclass_catalog`, but the
/// changed branch also bumps the command counter (C does so under the `if
/// (!relhastriggers)` block).
fn set_pg_class_relhastriggers(relid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_relhastriggers");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (values, nulls) = deform(mcx, &pg_class, &oldtup)?;
    let cur = values[(ANUM_PG_CLASS_RELHASTRIGGERS - 1) as usize].as_bool();
    if !cur {
        let mut values = values;
        let mut nulls = nulls;
        let mut replaces = vec![false; values.len()];
        set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            ANUM_PG_CLASS_RELHASTRIGGERS,
            Datum::from_bool(true),
        );
        modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
        // CommandCounterIncrement();
        backend_access_transam_xact_seams::command_counter_increment::call()?;
    } else {
        // CacheInvalidateRelcacheByTuple(tuple): relisshared is column 16.
        let relisshared = values[(16 - 1) as usize].as_bool();
        let form = pg_class_form_for_inval(relid, relisshared);
        backend_utils_cache_inval_seams::cache_invalidate_relcache_by_pg_class::call(relid, &form)?;
    }
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `ATExecSetRowSecurity` / `ATExecForceNoForceRowSecurity` (tablecmds.c:18604,
/// 18634): open pg_class RowExclusiveLock, `SearchSysCacheCopy1(RELOID, relid)`,
/// poke `relrowsecurity` (or `relforcerowsecurity`) on the `GETSTRUCT` copy, and
/// `CatalogTupleUpdate`. Exactly one of `relrowsecurity` / `relforcerowsecurity`
/// is `Some`. The C `InvokeObjectPostAlterHook` is a no-op without an installed
/// object-access hook. The boolean result is `HeapTupleIsValid(tuple)`; the
/// caller raises the `cache lookup failed for relation %u` `elog(ERROR)` when it
/// is `false`.
fn set_pg_class_row_security(
    relid: Oid,
    relrowsecurity: Option<bool>,
    relforcerowsecurity: Option<bool>,
) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_row_security");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (mut values, mut nulls) = deform(mcx, &pg_class, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    if let Some(rls) = relrowsecurity {
        set_col(&mut values, &mut nulls, &mut replaces, ANUM_PG_CLASS_RELROWSECURITY, Datum::from_bool(rls));
    }
    if let Some(force) = relforcerowsecurity {
        set_col(&mut values, &mut nulls, &mut replaces, ANUM_PG_CLASS_RELFORCEROWSECURITY, Datum::from_bool(force));
    }
    modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `SetMatViewPopulatedState`'s pg_class write (matview.c:78-110): `pg_class =
/// table_open(RelationRelationId, RowExclusiveLock)` → `tuple =
/// SearchSysCacheCopy1(RELOID, relid)` → `((Form_pg_class)
/// GETSTRUCT(tuple))->relispopulated = newstate` → `CatalogTupleUpdate(pg_class,
/// &tuple->t_self, tuple)` → `heap_freetuple` → `table_close`. Returns `false`
/// when the syscache lookup failed (`!HeapTupleIsValid`), so the matview caller
/// raises `cache lookup failed for relation %u`. Installed as the
/// `update_pg_class_populated` matview seam: matview.c's body crosses into the
/// pg_class-write owner exactly as the row-security setters above do.
fn set_pg_class_relispopulated(relid: Oid, newstate: bool) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_relispopulated");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (mut values, mut nulls) = deform(mcx, &pg_class, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_CLASS_RELISPOPULATED,
        Datum::from_bool(newstate),
    );
    modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `relation_mark_replica_identity`'s pg_class leg (tablecmds.c:18411-18429):
/// `pg_class = table_open(RelationRelationId, RowExclusiveLock)` → `tuple =
/// SearchSysCacheCopy1(RELOID, relid)` → if `relreplident != ri_type`, poke
/// `relreplident = ri_type` and `CatalogTupleUpdate` → `table_close` →
/// `heap_freetuple`. Returns `false` when the syscache lookup failed
/// (`!HeapTupleIsValid`), so the caller raises `cache lookup failed for relation
/// %s`. The C unconditionally writes `relreplident` then guards the
/// `CatalogTupleUpdate` on a change; the owned re-fetch model only writes (and
/// re-forms) when the value differs, which is the same on-disk outcome.
fn set_pg_class_relreplident(relid: Oid, ri_type: i8) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_relreplident");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (mut values, mut nulls) = deform(mcx, &pg_class, &oldtup)?;
    // `if (pg_class_form->relreplident != ri_type)` — only update if it changed.
    if values[(ANUM_PG_CLASS_RELREPLIDENT - 1) as usize].as_char() != ri_type {
        let mut replaces = vec![false; values.len()];
        set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            ANUM_PG_CLASS_RELREPLIDENT,
            Datum::from_char(ri_type),
        );
        modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    }
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `ResetRelRewrite`'s pg_class write (tablecmds.c:4363): `pg_class =
/// table_open(RelationRelationId, RowExclusiveLock)` → `tuple =
/// SearchSysCacheCopy1(RELOID, relid)` → `((Form_pg_class)
/// GETSTRUCT(tuple))->relrewrite = InvalidOid` → `CatalogTupleUpdate` →
/// `heap_freetuple` → `table_close`. Returns `false` when the syscache lookup
/// failed (`!HeapTupleIsValid`), so the caller raises `cache lookup failed for
/// relation %u`. Installed as the `reset_rel_rewrite` tablecmds seam (the pg_class
/// write the tablecmds body delegates to the indexing pg_class-write owner, like
/// the setters above).
fn set_pg_class_relrewrite(relid: Oid, relrewrite: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("set_pg_class_relrewrite");
    let mcx = ctx.mcx();
    let pg_class = table_open(mcx, cat::catalog::RELATION_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_class, ANUM_PG_CLASS_OID, relid)? else {
        pg_class.close(RowExclusiveLock)?;
        return Ok(false);
    };
    let (mut values, mut nulls) = deform(mcx, &pg_class, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_CLASS_RELREWRITE,
        Datum::from_oid(relrewrite),
    );
    modify_and_update(mcx, &pg_class, &oldtup, &values, &nulls, &replaces)?;
    pg_class.close(RowExclusiveLock)?;
    Ok(true)
}

/// `relation_mark_replica_identity`'s per-index pg_index leg
/// (tablecmds.c:18435-18481): `pg_index = table_open(IndexRelationId,
/// RowExclusiveLock)` (the caller loops over `RelationGetIndexList(rel)`) →
/// `tuple = SearchSysCacheCopy1(INDEXRELID, thisIndexOid)` → if `indisreplident
/// != want`, poke `indisreplident = want` and `CatalogTupleUpdate`. Returns
/// `(found, dirty)`: `found` is `HeapTupleIsValid(tuple)` (the caller raises
/// `cache lookup failed for index %u` when `false`), `dirty` is whether the flag
/// actually changed (the caller owns the per-dirty-index
/// `InvokeObjectPostAlterHookArg` + `CacheInvalidateRelcache(rel)`). The
/// `table_open`/`table_close` of pg_index per call mirrors C re-opening it once
/// for the whole loop; the on-disk effect is identical.
fn set_index_isreplident(index_oid: Oid, want: bool) -> PgResult<(bool, bool)> {
    let ctx = MemoryContext::new("set_index_isreplident");
    let mcx = ctx.mcx();
    let pg_index = table_open(mcx, types_core::catalog::INDEX_RELATION_ID, RowExclusiveLock)?;
    let Some(oldtup) = fetch_by_oid(mcx, &pg_index, ANUM_PG_INDEX_INDEXRELID, index_oid)? else {
        pg_index.close(RowExclusiveLock)?;
        return Ok((false, false));
    };
    let (mut values, mut nulls) = deform(mcx, &pg_index, &oldtup)?;
    let mut dirty = false;
    if values[(ANUM_PG_INDEX_INDISREPLIDENT - 1) as usize].as_bool() != want {
        dirty = true;
        let mut replaces = vec![false; values.len()];
        set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            ANUM_PG_INDEX_INDISREPLIDENT,
            Datum::from_bool(want),
        );
        modify_and_update(mcx, &pg_index, &oldtup, &values, &nulls, &replaces)?;
    }
    pg_index.close(RowExclusiveLock)?;
    Ok((true, dirty))
}

/// The minimal `PgClassForm` the relcache-invalidation seam reads (`oid` is
/// passed separately; only `relisshared` is consulted by the C invalidation).
fn pg_class_form_for_inval(_oid: Oid, relisshared: bool) -> types_cluster::PgClassForm {
    types_cluster::PgClassForm {
        relisshared,
        ..Default::default()
    }
}

/* ======================================================================== *
 * pg_sequence (sequence.c).
 * ======================================================================== */

/// `DefineSequence`'s pg_sequence INSERT: open pg_sequence, form the 8-column
/// row, insert it, close.
fn catalog_insert_pg_sequence(form: &cat::pg_sequence::FormData_pg_sequence) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_insert_pg_sequence");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, SEQUENCE_RELATION_ID, RowExclusiveLock)?;
    let values = sequence_values(form);
    let nulls = [false; NATTS_PG_SEQUENCE];
    form_and_insert(mcx, &rel, &values, &nulls)?;
    rel.close(RowExclusiveLock)
}

/// The pg_sequence `values[]` for one row (all 8 columns non-null).
fn sequence_values<'mcx>(form: &cat::pg_sequence::FormData_pg_sequence) -> [Datum<'mcx>; NATTS_PG_SEQUENCE] {
    let mut v: [Datum<'mcx>; NATTS_PG_SEQUENCE] = core::array::from_fn(|_| Datum::null());
    v[(ANUM_PG_SEQUENCE_SEQRELID - 1) as usize] = Datum::from_oid(form.seqrelid);
    v[(ANUM_PG_SEQUENCE_SEQTYPID - 1) as usize] = Datum::from_oid(form.seqtypid);
    v[(ANUM_PG_SEQUENCE_SEQSTART - 1) as usize] = Datum::from_i64(form.seqstart);
    v[(ANUM_PG_SEQUENCE_SEQINCREMENT - 1) as usize] = Datum::from_i64(form.seqincrement);
    v[(ANUM_PG_SEQUENCE_SEQMAX - 1) as usize] = Datum::from_i64(form.seqmax);
    v[(ANUM_PG_SEQUENCE_SEQMIN - 1) as usize] = Datum::from_i64(form.seqmin);
    v[(ANUM_PG_SEQUENCE_SEQCACHE - 1) as usize] = Datum::from_i64(form.seqcache);
    v[(ANUM_PG_SEQUENCE_SEQCYCLE - 1) as usize] = Datum::from_bool(form.seqcycle);
    v
}

/// `AlterSequence`'s pg_sequence UPDATE: re-fetch the row keyed on
/// `form.seqrelid`, overwrite all `Form_pg_sequence` columns from `form`,
/// `CatalogTupleUpdate`, `InvokeObjectPostAlterHook(RelationRelationId,
/// seqrelid, 0)`. Returns `HeapTupleIsValid(seqtuple)`.
fn catalog_update_pg_sequence(form: &cat::pg_sequence::FormData_pg_sequence) -> PgResult<bool> {
    let ctx = MemoryContext::new("catalog_update_pg_sequence");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, SEQUENCE_RELATION_ID, RowExclusiveLock)?;
    let found = fetch_by_oid(mcx, &rel, ANUM_PG_SEQUENCE_SEQRELID, form.seqrelid)?;
    let Some(oldtup) = found else {
        rel.close(RowExclusiveLock)?;
        return Ok(false);
    };
    // The whole fixed-part row is overwritten (init_params scribbled every
    // field on the GETSTRUCT copy). Re-form from the new values; all 8 columns
    // replaced.
    let values = sequence_values(form);
    let nulls = [false; NATTS_PG_SEQUENCE];
    let replaces = [true; NATTS_PG_SEQUENCE];
    modify_and_update(mcx, &rel, &oldtup, &values, &nulls, &replaces)?;
    // InvokeObjectPostAlterHook(RelationRelationId, seqrelid, 0);
    if backend_catalog_objectaccess_seams::object_access_hook_present::call() {
        backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
            cat::catalog::RELATION_RELATION_ID,
            form.seqrelid,
            0,
        )?;
    }
    rel.close(RowExclusiveLock)?;
    Ok(true)
}

/// `DeleteSequenceTuple` (sequence.c): re-fetch the pg_sequence row keyed on
/// `relid`, `CatalogTupleDelete`, close. Returns `HeapTupleIsValid(tuple)`.
fn catalog_delete_pg_sequence(relid: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("catalog_delete_pg_sequence");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, SEQUENCE_RELATION_ID, RowExclusiveLock)?;
    let found = fetch_by_oid(mcx, &rel, ANUM_PG_SEQUENCE_SEQRELID, relid)?;
    let Some(tup) = found else {
        rel.close(RowExclusiveLock)?;
        return Ok(false);
    };
    CatalogTupleDelete(mcx, &rel, tup.tuple.t_self)?;
    rel.close(RowExclusiveLock)?;
    Ok(true)
}

/// `RemoveFunctionById(funcOid)` (functioncmds.c:1311): delete the `pg_proc`
/// row (reading its `prokind` first), `pgstat_drop_function(funcOid)`, and — if
/// it was an aggregate (`prokind == PROKIND_AGGREGATE`) — also delete the
/// `pg_aggregate` row keyed on `aggfnoid == funcOid`. This is the catalog-write
/// leg of the `backend-commands-functioncmds` seam (functioncmds.c can't do raw
/// catalog tuple I/O directly), installed cross-crate from the catalog-indexing
/// owner that holds `CatalogTupleDelete` + the heap scan substrate.
fn remove_function_tuple(func_oid: Oid) -> PgResult<()> {
    use cat::pg_aggregate as pa;
    use cat::pg_proc as pp;

    let ctx = MemoryContext::new("remove_function_tuple");
    let mcx = ctx.mcx();

    // relation = table_open(ProcedureRelationId, RowExclusiveLock);
    let relation = table_open(mcx, cat::catalog::PROCEDURE_RELATION_ID, RowExclusiveLock)?;
    // tup = SearchSysCache1(PROCOID, funcOid);
    let tup = fetch_by_oid(mcx, &relation, pp::Anum_pg_proc_oid, func_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for function (remove)"))?;
    // prokind = ((Form_pg_proc) GETSTRUCT(tup))->prokind;
    let (values, _nulls) = deform(mcx, &relation, &tup)?;
    let prokind = values[(pp::Anum_pg_proc_prokind - 1) as usize].as_char();
    // CatalogTupleDelete(relation, &tup->t_self);
    CatalogTupleDelete(mcx, &relation, tup.tuple.t_self)?;
    relation.close(RowExclusiveLock)?;

    // pgstat_drop_function(funcOid);
    backend_utils_activity_pgstat_function::pgstat_drop_function(func_oid)?;

    // If there's a pg_aggregate tuple, delete that too.
    if prokind == pp::PROKIND_AGGREGATE {
        let aggrel = table_open(mcx, pa::AggregateRelationId, RowExclusiveLock)?;
        let aggtup = fetch_by_oid(mcx, &aggrel, pa::Anum_pg_aggregate_aggfnoid, func_oid)?
            .ok_or_else(|| {
                PgError::error("cache lookup failed for pg_aggregate tuple for function")
            })?;
        CatalogTupleDelete(mcx, &aggrel, aggtup.tuple.t_self)?;
        aggrel.close(RowExclusiveLock)?;
    }
    Ok(())
}

/* ======================================================================== *
 * pg_depend / pg_shdepend single-row update + insert.
 * ======================================================================== */

fn catalog_tuple_update_pg_depend(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    form: &cat::catalog_dependency::FormData_pg_depend,
) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_tuple_update_pg_depend");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let values = [
        Datum::from_oid(form.classid),
        Datum::from_oid(form.objid),
        Datum::from_i32(form.objsubid),
        Datum::from_oid(form.refclassid),
        Datum::from_oid(form.refobjid),
        Datum::from_i32(form.refobjsubid),
        Datum::from_char(form.deptype),
    ];
    let nulls = [false; cat::catalog_dependency::Natts_pg_depend];
    let tupdesc = r.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleUpdate(mcx, &r, tid, &mut tup)
}

fn catalog_tuple_insert_pg_shdepend(
    rel: &RelationData<'_>,
    form: &cat::catalog_shdepend::FormData_pg_shdepend,
) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_shdepend");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let values = shdepend_values(form);
    let nulls = [false; cat::catalog_shdepend::Natts_pg_shdepend];
    form_and_insert(mcx, &r, &values, &nulls)
}

fn catalog_tuple_update_pg_shdepend(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    form: &cat::catalog_shdepend::FormData_pg_shdepend,
) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_tuple_update_pg_shdepend");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let values = shdepend_values(form);
    let nulls = [false; cat::catalog_shdepend::Natts_pg_shdepend];
    let tupdesc = r.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleUpdate(mcx, &r, tid, &mut tup)
}

fn shdepend_values<'mcx>(
    form: &cat::catalog_shdepend::FormData_pg_shdepend,
) -> [Datum<'mcx>; cat::catalog_shdepend::Natts_pg_shdepend] {
    [
        Datum::from_oid(form.dbid),
        Datum::from_oid(form.classid),
        Datum::from_oid(form.objid),
        Datum::from_i32(form.objsubid),
        Datum::from_oid(form.refclassid),
        Datum::from_oid(form.refobjid),
        Datum::from_char(form.deptype),
    ]
}

/* ======================================================================== *
 * pg_constraint (CreateConstraintEntry + in-place mutators).
 * ======================================================================== */

/// Build an `int2[]` (smallint) array varlena Datum, or `None` for an empty/
/// absent column. `construct_array(elems, INT2OID, 2, true, 's')`.
fn int2_array_datum<'mcx>(mcx: Mcx<'mcx>, vals: &Option<Vec<i16>>) -> PgResult<Option<Datum<'mcx>>> {
    const INT2OID: Oid = 21;
    let Some(vals) = vals else { return Ok(None) };
    let images: Vec<[u8; 2]> = vals.iter().map(|&v| v.to_ne_bytes()).collect();
    let refs: Vec<&[u8]> = images.iter().map(|a| &a[..]).collect();
    let bytes = build_array_image(mcx, &refs, INT2OID, 2, b's')?;
    Ok(Some(Datum::ByRef(bytes)))
}

/// Build an `oid[]` array varlena Datum, or `None`.
/// `construct_array(elems, OIDOID, 4, true, 'i')`.
fn oid_array_datum<'mcx>(mcx: Mcx<'mcx>, vals: &Option<Vec<Oid>>) -> PgResult<Option<Datum<'mcx>>> {
    const OIDOID: Oid = 26;
    let Some(vals) = vals else { return Ok(None) };
    let images: Vec<[u8; 4]> = vals.iter().map(|&v| v.to_ne_bytes()).collect();
    let refs: Vec<&[u8]> = images.iter().map(|a| &a[..]).collect();
    let bytes = build_array_image(mcx, &refs, OIDOID, 4, b'i')?;
    Ok(Some(Datum::ByRef(bytes)))
}

/// `CreateConstraintEntry`'s tuple build + insert (pg_constraint.c): assign the
/// row OID, build the 28-column `values[]`/`nulls[]` (fixed columns + the four
/// array columns + conbin text), and `CatalogTupleInsert`. Returns the OID.
fn catalog_tuple_insert_pg_constraint(
    rel: &RelationData<'_>,
    row: &cat::pg_constraint::PgConstraintInsertRow,
) -> PgResult<Oid> {
    use cat::pg_constraint as pc;
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_constraint");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;

    // conOid = GetNewOidWithIndex(rel, ConstraintOidIndexId, Anum_pg_constraint_oid);
    let con_oid = backend_catalog_catalog::GetNewOidWithIndex(
        &r,
        pc::ConstraintOidIndexId,
        pc::Anum_pg_constraint_oid,
    )?;

    let mut values: Vec<Datum<'_>> = Vec::with_capacity(pc::Natts_pg_constraint);
    let mut nulls = vec![false; pc::Natts_pg_constraint];

    // Fixed columns 1..=20.
    values.push(Datum::from_oid(con_oid)); // oid
    values.push(name_datum(mcx, &row.conname)?); // conname
    values.push(Datum::from_oid(row.connamespace));
    values.push(Datum::from_char(row.contype));
    values.push(Datum::from_bool(row.condeferrable));
    values.push(Datum::from_bool(row.condeferred));
    values.push(Datum::from_bool(row.conenforced));
    values.push(Datum::from_bool(row.convalidated));
    values.push(Datum::from_oid(row.conrelid));
    values.push(Datum::from_oid(row.contypid));
    values.push(Datum::from_oid(row.conindid));
    values.push(Datum::from_oid(row.conparentid));
    values.push(Datum::from_oid(row.confrelid));
    values.push(Datum::from_char(row.confupdtype));
    values.push(Datum::from_char(row.confdeltype));
    values.push(Datum::from_char(row.confmatchtype));
    values.push(Datum::from_bool(row.conislocal));
    values.push(Datum::from_i16(row.coninhcount));
    values.push(Datum::from_bool(row.connoinherit));
    values.push(Datum::from_bool(row.conperiod));

    // Array columns 21..=27 (NULL when absent).
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_conkey, int2_array_datum(mcx, &row.conkey)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_confkey, int2_array_datum(mcx, &row.confkey)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_conpfeqop, oid_array_datum(mcx, &row.conpfeqop)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_conppeqop, oid_array_datum(mcx, &row.conppeqop)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_conffeqop, oid_array_datum(mcx, &row.conffeqop)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_confdelsetcols, int2_array_datum(mcx, &row.confdelsetcols)?);
    push_arr(&mut values, &mut nulls, pc::Anum_pg_constraint_conexclop, oid_array_datum(mcx, &row.conexclop)?);

    // conbin (column 28): pg_node_tree text, NULL when no CHECK expression.
    match &row.conbin {
        Some(s) => values.push(cstring_to_text_datum(mcx, s)?),
        None => {
            values.push(Datum::null());
            nulls[(pc::Anum_pg_constraint_conbin - 1) as usize] = true;
        }
    }

    debug_assert_eq!(values.len(), pc::Natts_pg_constraint);
    form_and_insert(mcx, &r, &values, &nulls)?;
    Ok(con_oid)
}

/// Push an array column at its 1-based `anum`: `Some(d)` is the array Datum,
/// `None` is a SQL NULL placeholder + `nulls[anum-1] = true`. The columns are
/// pushed in ascending order, so the push index equals `anum-1`.
fn push_arr<'mcx>(
    values: &mut Vec<Datum<'mcx>>,
    nulls: &mut [bool],
    anum: i16,
    d: Option<Datum<'mcx>>,
) {
    debug_assert_eq!(values.len(), (anum - 1) as usize);
    match d {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(anum - 1) as usize] = true;
        }
    }
}

/// `CatalogTupleUpdate` for the in-place pg_constraint mutators: re-fetch the
/// row at `tid`, overwrite the `ConstraintFieldUpdate` columns (conname,
/// connamespace, conislocal, coninhcount, conparentid), re-form, and store.
fn catalog_tuple_update_pg_constraint(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    fields: &cat::pg_constraint::ConstraintFieldUpdate,
) -> PgResult<()> {
    use cat::pg_constraint as pc;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_constraint");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_tid(mcx, &r, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_constraint tuple for update"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_conname, name_datum(mcx, &fields.conname)?);
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_connamespace, Datum::from_oid(fields.connamespace));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_conislocal, Datum::from_bool(fields.conislocal));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_coninhcount, Datum::from_i16(fields.coninhcount));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_conparentid, Datum::from_oid(fields.conparentid));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_convalidated, Datum::from_bool(fields.convalidated));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_connoinherit, Datum::from_bool(fields.connoinherit));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_conenforced, Datum::from_bool(fields.conenforced));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_condeferrable, Datum::from_bool(fields.condeferrable));
    set_col(&mut values, &mut nulls, &mut replaces, pc::Anum_pg_constraint_condeferred, Datum::from_bool(fields.condeferred));
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `CatalogTupleUpdate` for `renametrig_internal` (commands/trigger.c): re-read
/// the `pg_trigger` tuple at `tid`, overwrite `tgname`, re-form, store.
fn catalog_tuple_update_pg_trigger(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    fields: &cat::pg_trigger::TriggerFieldUpdate,
) -> PgResult<()> {
    use cat::pg_trigger as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_trigger");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_tid(mcx, &r, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_trigger tuple for update"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    if let Some(tgname) = &fields.tgname {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_trigger_tgname, name_datum(mcx, tgname)?);
    }
    if let Some(d) = fields.tgdeferrable {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_trigger_tgdeferrable, Datum::from_bool(d));
    }
    if let Some(d) = fields.tginitdeferred {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_trigger_tginitdeferred, Datum::from_bool(d));
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/* ======================================================================== *
 * pg_type (TypeShellMake / TypeCreate / RenameTypeInternal).
 * ======================================================================== */

/// The 32-column `values[]`/`nulls[]` for one pg_type row (pg_type.c:352-410).
fn type_values<'mcx>(
    mcx: Mcx<'mcx>,
    row: &cat::pg_type::PgTypeInsertRow,
) -> PgResult<(Vec<Datum<'mcx>>, Vec<bool>)> {
    use cat::pg_type as pt;
    let f = &row.fields;
    let mut nulls = vec![false; pt::Natts_pg_type];
    let mut values: Vec<Datum<'mcx>> = Vec::with_capacity(pt::Natts_pg_type);
    values.push(Datum::from_oid(f.oid)); // 1 oid
    values.push(name_datum(mcx, &namestrcpy_image(&f.typname))?); // 2 typname
    values.push(Datum::from_oid(f.typnamespace)); // 3
    values.push(Datum::from_oid(f.typowner)); // 4
    values.push(Datum::from_i16(f.typlen)); // 5
    values.push(Datum::from_bool(f.typbyval)); // 6
    values.push(Datum::from_char(f.typtype)); // 7
    values.push(Datum::from_char(f.typcategory)); // 8
    values.push(Datum::from_bool(f.typispreferred)); // 9
    values.push(Datum::from_bool(f.typisdefined)); // 10
    values.push(Datum::from_char(f.typdelim)); // 11
    values.push(Datum::from_oid(f.typrelid)); // 12
    values.push(Datum::from_oid(f.typsubscript)); // 13
    values.push(Datum::from_oid(f.typelem)); // 14
    values.push(Datum::from_oid(f.typarray)); // 15
    values.push(Datum::from_oid(f.typinput)); // 16
    values.push(Datum::from_oid(f.typoutput)); // 17
    values.push(Datum::from_oid(f.typreceive)); // 18
    values.push(Datum::from_oid(f.typsend)); // 19
    values.push(Datum::from_oid(f.typmodin)); // 20
    values.push(Datum::from_oid(f.typmodout)); // 21
    values.push(Datum::from_oid(f.typanalyze)); // 22
    values.push(Datum::from_char(f.typalign)); // 23
    values.push(Datum::from_char(f.typstorage)); // 24
    values.push(Datum::from_bool(f.typnotnull)); // 25
    values.push(Datum::from_oid(f.typbasetype)); // 26
    values.push(Datum::from_i32(f.typtypmod)); // 27
    values.push(Datum::from_i32(f.typndims)); // 28
    values.push(Datum::from_oid(f.typcollation)); // 29
    // 30 typdefaultbin (pg_node_tree text), 31 typdefault (text), 32 typacl.
    match &row.typdefaultbin {
        Some(s) => values.push(cstring_to_text_datum(mcx, s)?),
        None => {
            values.push(Datum::null());
            nulls[(pt::Anum_pg_type_typdefaultbin - 1) as usize] = true;
        }
    }
    match &row.typdefault {
        Some(s) => values.push(cstring_to_text_datum(mcx, s)?),
        None => {
            values.push(Datum::null());
            nulls[(pt::Anum_pg_type_typdefault - 1) as usize] = true;
        }
    }
    match &row.typacl {
        // `typacl` (`Acl *` = `ArrayType`) crosses as its on-disk array header
        // (`types_array::ArrayType` is the 16-byte varlena header; the element
        // payload follows out of line as in C). On the `TypeCreate` path this is
        // either NULL (the common case — `isDependentType`, or
        // `get_user_default_acl()` returned NULL) or a default ACL; serialize the
        // carried header bytes verbatim.
        Some(arr) => values.push(Datum::ByRef(mcx::slice_in(mcx, &arraytype_header_bytes(arr))?)),
        None => {
            values.push(Datum::null());
            nulls[(pt::Anum_pg_type_typacl - 1) as usize] = true;
        }
    }
    debug_assert_eq!(values.len(), pt::Natts_pg_type);
    Ok((values, nulls))
}

fn catalog_tuple_insert_pg_type(
    rel: &RelationData<'_>,
    row: &cat::pg_type::PgTypeInsertRow,
) -> PgResult<()> {
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let (values, nulls) = type_values(mcx, row)?;
    form_and_insert(mcx, &r, &values, &nulls)
}

/// `TypeCreate`'s shell-replacement path: re-fetch the shell row by `row.fields.oid`,
/// `heap_modify_tuple` with every column replaced *except* oid, `CatalogTupleUpdate`.
fn catalog_tuple_update_pg_type(
    rel: &RelationData<'_>,
    row: &cat::pg_type::PgTypeInsertRow,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, row.fields.oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type (shell replace)"))?;
    let (values, nulls) = type_values(mcx, row)?;
    let mut replaces = vec![true; pt::Natts_pg_type];
    replaces[(pt::Anum_pg_type_oid - 1) as usize] = false;
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `RenameTypeInternal`: re-fetch the row by `type_oid`, write only typname.
fn catalog_tuple_update_typname_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    new_type_name: &str,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_typname_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        pt::Anum_pg_type_typname,
        name_datum(mcx, &namestrcpy_image(new_type_name))?,
    );
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `AlterTypeOwnerInternal`'s single-row write (typecmds.c:3986): re-fetch the
/// row by `type_oid`, set `typowner = new_owner_id`, and — when the held row's
/// `typacl` is non-NULL — `typacl = aclnewowner(old_acl, old_owner, new_owner)`
/// (the old owner is the row's pre-write `typowner`), then `CatalogTupleUpdate`.
fn catalog_tuple_update_typowner_typacl_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    new_owner_id: Oid,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_typowner_typacl_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    // typTup->typowner (the OLD owner) is read before we overwrite it.
    let old_owner = values[(pt::Anum_pg_type_typowner - 1) as usize].as_oid();
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        pt::Anum_pg_type_typowner,
        Datum::from_oid(new_owner_id),
    );
    // /* Null ACLs do not require changes */
    let acl_idx = (pt::Anum_pg_type_typacl - 1) as usize;
    if !nulls[acl_idx] {
        if let Datum::ByRef(bytes) = &values[acl_idx] {
            let new_acl = acl_new_owner_datum(mcx, &bytes.clone(), old_owner, new_owner_id)?;
            set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typacl, new_acl);
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `AlterTypeNamespaceInternal`'s single-row write (typecmds.c:4231): re-fetch
/// the row by `type_oid`, set `typnamespace = nsp_oid`, `CatalogTupleUpdate`.
/// (The `oldNspOid != nspOid` guard + dup-name checks stay on the pg_type
/// owner side; the seam is the unconditional row write.)
fn catalog_tuple_update_typnamespace_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    nsp_oid: Oid,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_typnamespace_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        pt::Anum_pg_type_typnamespace,
        Datum::from_oid(nsp_oid),
    );
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `AlterDomainNotNull`/`AlterDomainAddConstraint`/`AlterDomainDropConstraint`
/// single-row write (typecmds.c:2806/3014/2885): re-fetch by `type_oid`, set
/// `typnotnull = not_null`, `CatalogTupleUpdate`.
fn catalog_tuple_update_typnotnull_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    not_null: bool,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_typnotnull_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        pt::Anum_pg_type_typnotnull,
        Datum::from_bool(not_null),
    );
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `AlterDomainDefault`'s pg_type write (typecmds.c:2691-2705): replace
/// `typdefaultbin` (text) and `typdefault` (text). `None` for either marks the
/// column SQL NULL (`nulls[..] = true; replaces[..] = true` — the DROP DEFAULT
/// and NULL-constant arms); a value is `CStringGetTextDatum`. Both columns are
/// always in `replaces`.
fn catalog_tuple_update_typdefault_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    default_value: Option<String>,
    default_bin: Option<String>,
) -> PgResult<()> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_typdefault_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    match default_bin {
        Some(s) => set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            pt::Anum_pg_type_typdefaultbin,
            cstring_to_text_datum(mcx, &s)?,
        ),
        None => {
            let i = (pt::Anum_pg_type_typdefaultbin - 1) as usize;
            values[i] = Datum::null();
            nulls[i] = true;
            replaces[i] = true;
        }
    }
    match default_value {
        Some(s) => set_col(
            &mut values,
            &mut nulls,
            &mut replaces,
            pt::Anum_pg_type_typdefault,
            cstring_to_text_datum(mcx, &s)?,
        ),
        None => {
            let i = (pt::Anum_pg_type_typdefault - 1) as usize;
            values[i] = Datum::null();
            nulls[i] = true;
            replaces[i] = true;
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `AlterTypeRecurse`'s per-row update (typecmds.c:4576-4621): build `replaces[]`
/// from the [`TypeAttrUpdate`] gates (`typstorage`/`typreceive`/`typsend`/
/// `typmodin`/`typmodout`/`typanalyze`/`typsubscript`), `heap_modify_tuple`,
/// `CatalogTupleUpdate`. Returns the row's `typarray` OID so the caller can
/// recurse to the array type.
fn catalog_tuple_update_attrs_pg_type(
    rel: &RelationData<'_>,
    type_oid: Oid,
    attr: cat::pg_type::TypeAttrUpdate,
) -> PgResult<Oid> {
    use cat::pg_type as pt;
    let ctx = MemoryContext::new("catalog_tuple_update_attrs_pg_type");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, pt::Anum_pg_type_oid, type_oid)?
        .ok_or_else(|| PgError::error("cache lookup failed for type"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let typarray = values[(pt::Anum_pg_type_typarray - 1) as usize].as_oid();
    let mut replaces = vec![false; values.len()];

    if attr.update_storage {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typstorage,
                Datum::from_char(attr.storage));
    }
    if attr.update_receive {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typreceive,
                Datum::from_oid(attr.receive_oid));
    }
    if attr.update_send {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typsend,
                Datum::from_oid(attr.send_oid));
    }
    if attr.update_typmodin {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typmodin,
                Datum::from_oid(attr.typmodin_oid));
    }
    if attr.update_typmodout {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typmodout,
                Datum::from_oid(attr.typmodout_oid));
    }
    if attr.update_analyze {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typanalyze,
                Datum::from_oid(attr.analyze_oid));
    }
    if attr.update_subscript {
        set_col(&mut values, &mut nulls, &mut replaces, pt::Anum_pg_type_typsubscript,
                Datum::from_oid(attr.subscript_oid));
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)?;
    Ok(typarray)
}

/// `GetNewOidWithIndex(pg_type, TypeOidIndexId, Anum_pg_type_oid)`.
fn get_new_oid_with_index_pg_type<'mcx>(rel: &Relation<'mcx>) -> PgResult<Oid> {
    backend_catalog_catalog::GetNewOidWithIndex(
        rel,
        cat::pg_type::TypeOidIndexId,
        cat::pg_type::Anum_pg_type_oid,
    )
}

/* ======================================================================== *
 * pg_namespace (schemacmds.c RenameSchema / AlterSchemaOwner_internal).
 *
 * The C keys RenameSchema on NAMESPACEOID-by-the-oid the caller resolved
 * (the seam takes `nspoid` directly). Both bodies open pg_namespace themselves
 * (the C `table_open(NamespaceRelationId, ...)`).
 * ======================================================================== */

fn rename_namespace_tuple(nspoid: Oid, newname: &str) -> PgResult<()> {
    let ctx = MemoryContext::new("rename_namespace_tuple");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, cat::catalog::NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let oldtup = fetch_by_oid(mcx, &rel, 1 /* oid */, nspoid)?
        .ok_or_else(|| PgError::error("cache lookup failed for namespace"))?;
    let (mut values, mut nulls) = deform(mcx, &rel, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_NAMESPACE_NSPNAME,
        name_datum(mcx, &namestrcpy_image(newname))?,
    );
    modify_and_update(mcx, &rel, &oldtup, &values, &nulls, &replaces)?;
    rel.close(RowExclusiveLock)
}

fn update_namespace_owner_tuple(nspoid: Oid, old_owner: Oid, new_owner: Oid) -> PgResult<()> {
    let ctx = MemoryContext::new("update_namespace_owner_tuple");
    let mcx = ctx.mcx();
    let rel = table_open(mcx, cat::catalog::NAMESPACE_RELATION_ID, RowExclusiveLock)?;
    let oldtup = fetch_by_oid(mcx, &rel, 1 /* oid */, nspoid)?
        .ok_or_else(|| PgError::error("cache lookup failed for namespace"))?;
    let (values, nulls) = deform(mcx, &rel, &oldtup)?;
    debug_assert_eq!(values.len(), NATTS_PG_NAMESPACE);

    let mut values = values;
    let mut nulls = nulls;
    let mut replaces = vec![false; values.len()];
    set_col(
        &mut values,
        &mut nulls,
        &mut replaces,
        ANUM_PG_NAMESPACE_NSPOWNER,
        Datum::from_oid(new_owner),
    );
    // if (!isNull) aclnewowner(nspacl, oldOwner, newOwner).
    let acl_idx = (ANUM_PG_NAMESPACE_NSPACL - 1) as usize;
    if !nulls[acl_idx] {
        if let Datum::ByRef(bytes) = &values[acl_idx] {
            let new_acl = acl_new_owner_datum(mcx, &bytes.clone(), old_owner, new_owner)?;
            set_col(&mut values, &mut nulls, &mut replaces, ANUM_PG_NAMESPACE_NSPACL, new_acl);
        }
    }
    modify_and_update(mcx, &rel, &oldtup, &values, &nulls, &replaces)?;
    rel.close(RowExclusiveLock)
}

/// `NamespaceCreate`'s pg_namespace INSERT (pg_namespace.c:76-96): zero the
/// `values[]`/`nulls[]`, allocate the OID via `GetNewOidWithIndex(rel,
/// NamespaceOidIndexId, Anum_pg_namespace_oid)`, `namestrcpy(&nname, nspName)`
/// into the `nspname` column, set `nspowner`, and either install the supplied
/// default ACL into `nspacl` or mark it NULL (`nspacl == None`). The caller has
/// already opened pg_namespace RowExclusiveLock and crossed it as
/// `&RelationData`; `reopen` re-derives an owned `Relation<'mcx>` over the same
/// OID under this seam's `mcx` (the family-wide convention). Returns the new
/// namespace OID.
fn insert_pg_namespace(
    rel: &RelationData<'_>,
    nspname: &str,
    nspowner: Oid,
    nspacl: Option<types_array::ArrayType>,
) -> PgResult<Oid> {
    let ctx = MemoryContext::new("insert_pg_namespace");
    let mcx = ctx.mcx();
    let rel = reopen(mcx, rel)?;

    // for (i = 0; i < Natts_pg_namespace; i++) nulls[i] = false;
    let mut values: [Datum; NATTS_PG_NAMESPACE] = core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; NATTS_PG_NAMESPACE];

    // nspoid = GetNewOidWithIndex(nspdesc, NamespaceOidIndexId,
    //                             Anum_pg_namespace_oid);
    let nspoid = backend_catalog_catalog::GetNewOidWithIndex(
        &rel,
        cat::catalog::NAMESPACE_OID_INDEX_ID,
        ANUM_PG_NAMESPACE_OID,
    )?;
    // values[Anum_pg_namespace_oid - 1] = ObjectIdGetDatum(nspoid);
    values[(ANUM_PG_NAMESPACE_OID - 1) as usize] = Datum::from_oid(nspoid);
    // namestrcpy(&nname, nspName); values[..nspname] = NameGetDatum(&nname);
    values[(ANUM_PG_NAMESPACE_NSPNAME - 1) as usize] =
        name_datum(mcx, &namestrcpy_image(nspname))?;
    // values[Anum_pg_namespace_nspowner - 1] = ObjectIdGetDatum(ownerId);
    values[(ANUM_PG_NAMESPACE_NSPOWNER - 1) as usize] = Datum::from_oid(nspowner);
    // if (nspacl != NULL) values[..nspacl] = PointerGetDatum(nspacl);
    // else nulls[Anum_pg_namespace_nspacl - 1] = true;
    match &nspacl {
        Some(acl) => {
            values[(ANUM_PG_NAMESPACE_NSPACL - 1) as usize] =
                Datum::ByRef(mcx::slice_in(mcx, &arraytype_header_bytes(acl))?)
        }
        None => nulls[(ANUM_PG_NAMESPACE_NSPACL - 1) as usize] = true,
    }

    // tup = heap_form_tuple(...); CatalogTupleInsert(nspdesc, tup);
    form_and_insert(mcx, &rel, &values, &nulls)?;
    rel.close(RowExclusiveLock)?;
    Ok(nspoid)
}

/// `AlterObjectOwner_internal`'s modified-tuple write (alter.c 1012-1046) for an
/// arbitrary simple catalog. The caller (alter.c) opened `rel` (catalog
/// `catalog_id`, RowExclusiveLock) and locked the row via
/// `get_catalog_object_by_oid_extended(.., locktuple=true)`. We re-fetch the row
/// over a re-open of the same relation, set `owner` (and, when `anum_acl !=
/// InvalidAttrNumber` and the ACL is non-null, `aclnewowner(acl, old, new)`),
/// `heap_modify_tuple` + `CatalogTupleUpdate`, then `UnlockTuple(rel,
/// &oldtup->t_self, InplaceUpdateTupleLock)` — releasing the lock the caller's
/// `get_catalog_object_by_oid_extended` took. The generic `aclitem[]`
/// re-serialization is [`acl_new_owner_datum`], shared with the per-catalog
/// typed owner-tuple writers.
fn update_object_owner_tuple(
    rel: &RelationData<'_>,
    anum_oid: i16,
    object_id: Oid,
    anum_owner: i16,
    anum_acl: i16,
    old_owner: Oid,
    new_owner: Oid,
) -> PgResult<()> {
    const INVALID_ATTR_NUMBER: i16 = 0;
    let ctx = MemoryContext::new("update_object_owner_tuple");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, anum_oid, object_id)?.ok_or_else(|| {
        PgError::error(format!(
            "cache lookup failed for object {object_id} of catalog {}",
            rel.rd_id
        ))
    })?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];

    // values[Anum_owner - 1] = ObjectIdGetDatum(new_ownerId).
    set_col(&mut values, &mut nulls, &mut replaces, anum_owner, Datum::from_oid(new_owner));

    // if (Anum_acl != InvalidAttrNumber) { datum = heap_getattr(Anum_acl);
    //   if (!isnull) values[Anum_acl-1] = aclnewowner(acl, old, new); }
    if anum_acl != INVALID_ATTR_NUMBER {
        let acl_idx = (anum_acl - 1) as usize;
        if !nulls[acl_idx] {
            if let Datum::ByRef(bytes) = &values[acl_idx] {
                let new_acl = acl_new_owner_datum(mcx, &bytes.clone(), old_owner, new_owner)?;
                set_col(&mut values, &mut nulls, &mut replaces, anum_acl, new_acl);
            }
        }
    }

    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)?;

    // UnlockTuple(rel, &oldtup->t_self, InplaceUpdateTupleLock).
    backend_storage_lmgr_lmgr_seams::unlock_tuple::call(
        rel.rd_id,
        oldtup.tuple.t_self,
        types_storage::lock::InplaceUpdateTupleLock,
    )?;
    r.close(RowExclusiveLock)
}

/* ======================================================================== *
 * pg_foreign_data_wrapper / pg_foreign_server / pg_user_mapping /
 * pg_foreign_table (foreigncmds.c).
 * ======================================================================== */

fn catalog_tuple_insert_pg_foreign_data_wrapper(
    rel: &RelationData<'_>,
    row: &types_foreigncmds::PgForeignDataWrapperInsertRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_foreign_data_wrapper");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let mut nulls = vec![false; fc::Natts_pg_foreign_data_wrapper];
    let mut values: Vec<Datum<'_>> = Vec::with_capacity(fc::Natts_pg_foreign_data_wrapper);
    values.push(Datum::from_oid(row.oid)); // oid
    values.push(name_datum(mcx, &namestrcpy_image(&row.fdwname))?); // fdwname (namein)
    values.push(Datum::from_oid(row.fdwowner));
    values.push(Datum::from_oid(row.fdwhandler));
    values.push(Datum::from_oid(row.fdwvalidator));
    // fdwacl: always NULL on create.
    values.push(Datum::null());
    nulls[(fc::Anum_pg_foreign_data_wrapper_fdwacl - 1) as usize] = true;
    // fdwoptions: text[] or NULL.
    match options_array_datum(mcx, &row.options)? {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(fc::Anum_pg_foreign_data_wrapper_fdwoptions - 1) as usize] = true;
        }
    }
    form_and_insert(mcx, &r, &values, &nulls)
}

fn catalog_tuple_update_pg_foreign_data_wrapper(
    rel: &RelationData<'_>,
    fdwid: Oid,
    row: &types_foreigncmds::PgForeignDataWrapperUpdateRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_foreign_data_wrapper");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, fc::Anum_pg_foreign_data_wrapper_oid, fdwid)?
        .ok_or_else(|| PgError::error("cache lookup failed for foreign-data wrapper"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    if let Some(h) = row.handler {
        set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_data_wrapper_fdwhandler, Datum::from_oid(h));
    }
    if let Some(v) = row.validator {
        set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_data_wrapper_fdwvalidator, Datum::from_oid(v));
    }
    if let Some(opts) = &row.options {
        let col = fc::Anum_pg_foreign_data_wrapper_fdwoptions;
        match options_array_datum(mcx, opts)? {
            Some(d) => set_col(&mut values, &mut nulls, &mut replaces, col, d),
            None => set_null_col(&mut values, &mut nulls, &mut replaces, col),
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn catalog_tuple_update_owner_pg_foreign_data_wrapper(
    rel: &RelationData<'_>,
    fdwid: Oid,
    old_owner: Oid,
    new_owner: Oid,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_update_owner_pg_foreign_data_wrapper");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, fc::Anum_pg_foreign_data_wrapper_oid, fdwid)?
        .ok_or_else(|| PgError::error("cache lookup failed for foreign-data wrapper"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_data_wrapper_fdwowner, Datum::from_oid(new_owner));
    let acl_idx = (fc::Anum_pg_foreign_data_wrapper_fdwacl - 1) as usize;
    if !nulls[acl_idx] {
        if let Datum::ByRef(bytes) = &values[acl_idx] {
            let new_acl = acl_new_owner_datum(mcx, &bytes.clone(), old_owner, new_owner)?;
            set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_data_wrapper_fdwacl, new_acl);
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn catalog_tuple_insert_pg_foreign_server(
    rel: &RelationData<'_>,
    row: &types_foreigncmds::PgForeignServerInsertRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_foreign_server");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let mut nulls = vec![false; fc::Natts_pg_foreign_server];
    let mut values: Vec<Datum<'_>> = Vec::with_capacity(fc::Natts_pg_foreign_server);
    values.push(Datum::from_oid(row.oid));
    values.push(name_datum(mcx, &namestrcpy_image(&row.srvname))?);
    values.push(Datum::from_oid(row.srvowner));
    values.push(Datum::from_oid(row.srvfdw));
    // srvtype (text or NULL).
    push_text_or_null(mcx, &mut values, &mut nulls, fc::Anum_pg_foreign_server_srvtype, &row.srvtype)?;
    // srvversion (text or NULL).
    push_text_or_null(mcx, &mut values, &mut nulls, fc::Anum_pg_foreign_server_srvversion, &row.srvversion)?;
    // srvacl: always NULL on create.
    values.push(Datum::null());
    nulls[(fc::Anum_pg_foreign_server_srvacl - 1) as usize] = true;
    // srvoptions (text[] or NULL).
    match options_array_datum(mcx, &row.options)? {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(fc::Anum_pg_foreign_server_srvoptions - 1) as usize] = true;
        }
    }
    form_and_insert(mcx, &r, &values, &nulls)
}

fn catalog_tuple_update_pg_foreign_server(
    rel: &RelationData<'_>,
    serverid: Oid,
    row: &types_foreigncmds::PgForeignServerUpdateRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_foreign_server");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, fc::Anum_pg_foreign_server_oid, serverid)?
        .ok_or_else(|| PgError::error("cache lookup failed for foreign server"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    if let Some(ver) = &row.version {
        let col = fc::Anum_pg_foreign_server_srvversion;
        match ver {
            Some(s) => set_col(&mut values, &mut nulls, &mut replaces, col, cstring_to_text_datum(mcx, s)?),
            None => set_null_col(&mut values, &mut nulls, &mut replaces, col),
        }
    }
    if let Some(opts) = &row.options {
        let col = fc::Anum_pg_foreign_server_srvoptions;
        match options_array_datum(mcx, opts)? {
            Some(d) => set_col(&mut values, &mut nulls, &mut replaces, col, d),
            None => set_null_col(&mut values, &mut nulls, &mut replaces, col),
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn catalog_tuple_update_owner_pg_foreign_server(
    rel: &RelationData<'_>,
    serverid: Oid,
    old_owner: Oid,
    new_owner: Oid,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_update_owner_pg_foreign_server");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, fc::Anum_pg_foreign_server_oid, serverid)?
        .ok_or_else(|| PgError::error("cache lookup failed for foreign server"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_server_srvowner, Datum::from_oid(new_owner));
    let acl_idx = (fc::Anum_pg_foreign_server_srvacl - 1) as usize;
    if !nulls[acl_idx] {
        if let Datum::ByRef(bytes) = &values[acl_idx] {
            let new_acl = acl_new_owner_datum(mcx, &bytes.clone(), old_owner, new_owner)?;
            set_col(&mut values, &mut nulls, &mut replaces, fc::Anum_pg_foreign_server_srvacl, new_acl);
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn catalog_tuple_insert_pg_user_mapping(
    rel: &RelationData<'_>,
    row: &types_foreigncmds::PgUserMappingInsertRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_user_mapping");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let mut nulls = vec![false; fc::Natts_pg_user_mapping];
    let mut values: Vec<Datum<'_>> = Vec::with_capacity(fc::Natts_pg_user_mapping);
    values.push(Datum::from_oid(row.oid));
    values.push(Datum::from_oid(row.umuser));
    values.push(Datum::from_oid(row.umserver));
    match options_array_datum(mcx, &row.options)? {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(fc::Anum_pg_user_mapping_umoptions - 1) as usize] = true;
        }
    }
    form_and_insert(mcx, &r, &values, &nulls)
}

fn catalog_tuple_update_pg_user_mapping(
    rel: &RelationData<'_>,
    umid: Oid,
    row: &types_foreigncmds::PgUserMappingUpdateRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_user_mapping");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_oid(mcx, &r, fc::Anum_pg_user_mapping_oid, umid)?
        .ok_or_else(|| PgError::error("cache lookup failed for user mapping"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    if let Some(opts) = &row.options {
        let col = fc::Anum_pg_user_mapping_umoptions;
        match options_array_datum(mcx, opts)? {
            Some(d) => set_col(&mut values, &mut nulls, &mut replaces, col, d),
            None => set_null_col(&mut values, &mut nulls, &mut replaces, col),
        }
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

fn catalog_tuple_insert_pg_foreign_table(
    rel: &RelationData<'_>,
    row: &types_foreigncmds::PgForeignTableInsertRow,
) -> PgResult<()> {
    use types_foreigncmds as fc;
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_foreign_table");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let mut nulls = vec![false; fc::Natts_pg_foreign_table];
    let mut values: Vec<Datum<'_>> = Vec::with_capacity(fc::Natts_pg_foreign_table);
    values.push(Datum::from_oid(row.ftrelid));
    values.push(Datum::from_oid(row.ftserver));
    match options_array_datum(mcx, &row.options)? {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(fc::Anum_pg_foreign_table_ftoptions - 1) as usize] = true;
        }
    }
    form_and_insert(mcx, &r, &values, &nulls)
}

/// Push a `text`-or-NULL column at the next position (ascending order).
fn push_text_or_null<'mcx>(
    mcx: Mcx<'mcx>,
    values: &mut Vec<Datum<'mcx>>,
    nulls: &mut [bool],
    anum: i16,
    val: &Option<String>,
) -> PgResult<()> {
    debug_assert_eq!(values.len(), (anum - 1) as usize);
    match val {
        Some(s) => values.push(cstring_to_text_datum(mcx, s)?),
        None => {
            values.push(Datum::null());
            nulls[(anum - 1) as usize] = true;
        }
    }
    Ok(())
}

/// Set a column to SQL NULL with `replaces[anum-1] = true` (the C
/// `repl_null[col] = true; repl_repl[col] = true`).
fn set_null_col<'mcx>(
    values: &mut [Datum<'mcx>],
    nulls: &mut [bool],
    replaces: &mut [bool],
    anum: i16,
) {
    let i = (anum - 1) as usize;
    values[i] = Datum::null();
    nulls[i] = true;
    replaces[i] = true;
}

/* ======================================================================== *
 * pg_db_role_setting (pg_db_role_setting.c).
 * ======================================================================== */

/// `decode_db_role_setting_setconfig`: `heap_getattr(setconfig)` +
/// `DatumGetArrayTypeP` decode into the `Vec<String>` of `"name=value"`
/// entries. `None` is the C `isnull` (SQL NULL setconfig).
fn decode_db_role_setting_setconfig<'mcx>(
    rel: &RelationData<'mcx>,
    tuple: &FormedTuple<'mcx>,
) -> PgResult<Option<Vec<String>>> {
    use cat::pg_db_role_setting::Anum_pg_db_role_setting_setconfig as SETCONFIG;
    let ctx = MemoryContext::new("decode_db_role_setting_setconfig");
    let mcx = ctx.mcx();
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tuple.tuple, &tupdesc, &tuple.data)?;
    let idx = (SETCONFIG - 1) as usize;
    let (value, isnull) = &cols[idx];
    if *isnull {
        return Ok(None);
    }
    let bytes = match value {
        Datum::ByRef(v) => v.clone(),
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => return Err(PgError::error("setconfig is by-value")),
    };
    Ok(Some(decode_text_array(mcx, &bytes)?))
}

/// `deconstruct_array(arr, TEXTOID, -1, false, 'i')` + `TextDatumGetCString`
/// each non-null element, into a `Vec<String>`. The text elements are read
/// directly from the array image (varlena elements at 'i'-aligned offsets);
/// each is decoded by [`text_datum_to_string`]. Null elements are skipped (the
/// C `TransformGUCArray` skip-null behaviour).
fn decode_text_array(mcx: Mcx<'_>, bytes: &[u8]) -> PgResult<Vec<String>> {
    use backend_utils_adt_arrayfuncs::foundation;

    let ndim = foundation::arr_ndim(bytes);
    let nelems = if ndim == 0 {
        0
    } else {
        let dims = foundation::arr_dims(mcx, bytes)?;
        dims.first().copied().unwrap_or(0).max(0) as usize
    };
    // No-nulls catalog text[]: there is no null bitmap (dataoffset == 0). Read
    // each varlena element at its aligned offset.
    let has_nulls = foundation::arr_hasnull(bytes);
    let data_off = foundation::arr_data_ptr_off(bytes);
    let mut out = Vec::with_capacity(nelems);
    let mut off = 0usize;
    for _ in 0..nelems {
        // For a no-nulls array, every element is present. (A setconfig text[]
        // built by GUCArrayAdd is always a no-nulls array.) If a null bitmap is
        // present we conservatively stop — the C path skips nulls, but catalog
        // setconfig arrays never carry them.
        if has_nulls {
            break;
        }
        off = foundation::att_align_nominal(off, b'i');
        let base = data_off + off;
        if base >= bytes.len() {
            break;
        }
        // Varlena element: decode the length from its header.
        let (payload_off, len) = varlena_header(&bytes[base..])?;
        let total_hdr_len = payload_off + len;
        let s = String::from_utf8_lossy(&bytes[base + payload_off..base + total_hdr_len]).into_owned();
        out.push(s);
        off += total_hdr_len;
    }
    Ok(out)
}

/// Decode a varlena header at the start of `bytes`, returning
/// `(payload_offset, payload_len)`. Handles the 1-byte short header and the
/// 4-byte full header (native byte order, uncompressed).
fn varlena_header(bytes: &[u8]) -> PgResult<(usize, usize)> {
    if bytes.is_empty() {
        return Err(PgError::error("empty varlena"));
    }
    if (bytes[0] & 0x01) != 0 {
        // 1-byte short header: VARSIZE_1B = byte >> 1 (includes the 1-byte hdr).
        let total = (bytes[0] >> 1) as usize;
        Ok((1, total.saturating_sub(1)))
    } else {
        if bytes.len() < 4 {
            return Err(PgError::error("short 4-byte varlena header"));
        }
        let word = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let total = (word >> 2) as usize;
        Ok((4, total.saturating_sub(4)))
    }
}

/// `catalog_tuple_update_pg_db_role_setting`: re-form the row at `tid` replacing
/// only `setconfig` with the `Vec<String>` text[], `CatalogTupleUpdate`.
fn catalog_tuple_update_pg_db_role_setting(
    rel: &RelationData<'_>,
    tid: ItemPointerData,
    new_setconfig: Vec<String>,
) -> PgResult<()> {
    use cat::pg_db_role_setting::Anum_pg_db_role_setting_setconfig as SETCONFIG;
    let ctx = MemoryContext::new("catalog_tuple_update_pg_db_role_setting");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let oldtup = fetch_by_tid(mcx, &r, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_db_role_setting tuple"))?;
    let (mut values, mut nulls) = deform(mcx, &r, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    // setconfig is never empty on the update path (the caller passes a non-empty
    // array; an empty result deletes the row instead), but build defensively.
    match text_array_datum(mcx, &new_setconfig)? {
        Some(d) => set_col(&mut values, &mut nulls, &mut replaces, SETCONFIG, d),
        None => set_null_col(&mut values, &mut nulls, &mut replaces, SETCONFIG),
    }
    modify_and_update(mcx, &r, &oldtup, &values, &nulls, &replaces)
}

/// `catalog_tuple_insert_pg_db_role_setting`: form a new row with
/// `setdatabase = databaseid`, `setrole = roleid`, `setconfig = <text[]>`.
fn catalog_tuple_insert_pg_db_role_setting(
    rel: &RelationData<'_>,
    databaseid: Oid,
    roleid: Oid,
    setconfig: Vec<String>,
) -> PgResult<()> {
    use cat::pg_db_role_setting::{
        Anum_pg_db_role_setting_setconfig as SETCONFIG, Natts_pg_db_role_setting as NATTS,
    };
    let ctx = MemoryContext::new("catalog_tuple_insert_pg_db_role_setting");
    let mcx = ctx.mcx();
    let r = reopen(mcx, rel)?;
    let mut values: Vec<Datum<'_>> = Vec::with_capacity(NATTS);
    let mut nulls = vec![false; NATTS];
    values.push(Datum::from_oid(databaseid)); // setdatabase
    values.push(Datum::from_oid(roleid)); // setrole
    match text_array_datum(mcx, &setconfig)? {
        Some(d) => values.push(d),
        None => {
            values.push(Datum::null());
            nulls[(SETCONFIG - 1) as usize] = true;
        }
    }
    form_and_insert(mcx, &r, &values, &nulls)
}

/* ======================================================================== *
 * pg_largeobject (inv_api.c).
 * ======================================================================== */

/// `deform_lo_page`: the per-page deform of a scanned pg_largeobject tuple —
/// the `HeapTupleHasNulls` paranoia, `GETSTRUCT->pageno`, and `getdatafield`
/// detoast + the `VARSIZE - VARHDRSZ` length sanity (0..=LOBLKSIZE).
fn deform_lo_page<'mcx>(
    rel: &RelationData<'mcx>,
    tuple: &FormedTuple<'mcx>,
) -> PgResult<backend_catalog_indexing_seams::LoPageRow> {
    use cat::catalog::{ANUM_PG_LARGEOBJECT_DATA, ANUM_PG_LARGEOBJECT_PAGENO};
    use types_storage::large_object::LOBLKSIZE;

    let ctx = MemoryContext::new("deform_lo_page");
    let mcx = ctx.mcx();
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let cols = heap_deform_tuple(mcx, &tuple.tuple, &tupdesc, &tuple.data)?;

    // if (HeapTupleHasNulls(tuple)) elog(ERROR, "null field found in pg_largeobject");
    for (_, isnull) in cols.iter() {
        if *isnull {
            return Err(PgError::error("null field found in pg_largeobject"));
        }
    }

    // pageno = ((Form_pg_largeobject) GETSTRUCT(tuple))->pageno;
    let pageno = cols[(ANUM_PG_LARGEOBJECT_PAGENO - 1) as usize].0.as_i32();

    // getdatafield: the data bytea, already detoasted by heap_deform_tuple's
    // fetchatt (the field bytes are copied out verbatim; a stored short/4-byte
    // header is decoded below). len = VARSIZE - VARHDRSZ.
    let data_datum = &cols[(ANUM_PG_LARGEOBJECT_DATA - 1) as usize].0;
    let raw = match data_datum {
        Datum::ByRef(v) => &v[..],
        Datum::ByVal(_)
        | Datum::Cstring(_)
        | Datum::Composite(_)
        | Datum::Expanded(_)
        | Datum::Internal(_) => return Err(PgError::error("pg_largeobject data is by-value")),
    };
    let (payload_off, len) = if !raw.is_empty() && (raw[0] & 0x01) != 0 {
        let hdr = (raw[0] >> 1) as usize;
        (1usize, hdr.saturating_sub(1))
    } else if raw.len() >= 4 {
        let word = u32::from_ne_bytes([raw[0], raw[1], raw[2], raw[3]]);
        ((4usize), (word >> 2) as usize - 4)
    } else {
        return Err(PgError::error("pg_largeobject data field too short"));
    };
    // if (len < 0 || len > LOBLKSIZE) ereport(ERRCODE_DATA_CORRUPTED, ...);
    if len > LOBLKSIZE as usize {
        return Err(PgError::error(
            "pg_largeobject entry has invalid data field size",
        ));
    }
    let end = (payload_off + len).min(raw.len());
    let data = raw[payload_off..end].to_vec();

    Ok(backend_catalog_indexing_seams::LoPageRow {
        pageno,
        data,
        tid: tuple.tuple.t_self,
    })
}

/// A `bytea` varlena Datum framing `data` with `SET_VARSIZE(len + VARHDRSZ)`.
fn bytea_datum<'mcx>(mcx: Mcx<'mcx>, data: &[u8]) -> PgResult<Datum<'mcx>> {
    let total = 4 + data.len();
    let word = (total as u32) << 2;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.extend_from_slice(&word.to_ne_bytes());
    buf.extend_from_slice(data);
    Ok(Datum::ByRef(buf))
}

/// `catalog_tuple_insert_with_info_pg_largeobject`: form a new pg_largeobject
/// page row (loid, pageno, data bytea) and `CatalogTupleInsertWithInfo`. `rel`
/// and `indstate` are the caller's open `lo_heap_r` and its open index state
/// (`CatalogOpenIndexes` done once in `inv_write`/`inv_truncate`), reused across
/// every page write — exactly C's amortized-`CatalogOpenIndexes` lifecycle.
fn catalog_tuple_insert_with_info_pg_largeobject<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'mcx>,
    loid: Oid,
    pageno: i32,
    data: &[u8],
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    use cat::catalog::{
        ANUM_PG_LARGEOBJECT_DATA, ANUM_PG_LARGEOBJECT_LOID, ANUM_PG_LARGEOBJECT_PAGENO,
    };

    let _ = rel;
    let mut values: [Datum<'_>; 3] = core::array::from_fn(|_| Datum::null());
    values[(ANUM_PG_LARGEOBJECT_LOID - 1) as usize] = Datum::from_oid(loid);
    values[(ANUM_PG_LARGEOBJECT_PAGENO - 1) as usize] = Datum::from_i32(pageno);
    values[(ANUM_PG_LARGEOBJECT_DATA - 1) as usize] = bytea_datum(mcx, data)?;
    let nulls = [false; 3];

    // indstate->ri_RelationDesc is the open lo_heap_r the caller passed to
    // CatalogOpenIndexes; use it (an alias) for the heap mutation.
    let heap_rel = indstate.heap_relation.alias();
    let tupdesc = heap_rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    CatalogTupleInsertWithInfo(mcx, &heap_rel, &mut tup, indstate)
}

/// `catalog_tuple_update_with_info_pg_largeobject`: re-read the old page tuple
/// at `tid`, replace only the `data` column, `CatalogTupleUpdateWithInfo`. `rel`
/// and `indstate` are the caller's open relation and index state.
fn catalog_tuple_update_with_info_pg_largeobject<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'mcx>,
    tid: ItemPointerData,
    data: &[u8],
    indstate: &mut CatalogIndexState<'mcx>,
) -> PgResult<()> {
    use cat::catalog::ANUM_PG_LARGEOBJECT_DATA;
    let _ = rel;
    // indstate->ri_RelationDesc is the open lo_heap_r the caller passed to
    // CatalogOpenIndexes; use it (an alias) for the heap read/modify/update.
    let heap_rel = indstate.heap_relation.alias();
    let oldtup = fetch_by_tid(mcx, &heap_rel, tid)?
        .ok_or_else(|| PgError::error("could not re-read pg_largeobject page tuple"))?;
    let (mut values, mut nulls) = deform(mcx, &heap_rel, &oldtup)?;
    let mut replaces = vec![false; values.len()];
    set_col(&mut values, &mut nulls, &mut replaces, ANUM_PG_LARGEOBJECT_DATA, bytea_datum(mcx, data)?);

    let tupdesc = heap_rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_modify_tuple(mcx, &oldtup, &tupdesc, &values, &nulls, &replaces)?;
    CatalogTupleUpdateWithInfo(mcx, &heap_rel, tid, &mut tup, indstate)
}

/* ======================================================================== *
 * get_catalog_object_by_oid (objectaddress.c).
 * ======================================================================== */

/// `get_catalog_object_by_oid(catalog, oidcol, objectId, locktuple)`: an
/// OID-keyed scan over the open `catalog` relation, returning the located tuple
/// copied into `mcx`, or `None`. The seam doc specifies the index/sequential
/// choice; the owned model runs the heap scan keyed on `oidcol` (behaviour-
/// identical — the OID column is unique). `locktuple` mirrors the `_extended`
/// variant's `LockTuple` (taken on the located row before return).
fn get_catalog_object_by_oid<'mcx>(
    mcx: Mcx<'mcx>,
    catalog: &RelationData<'mcx>,
    oidcol: i16,
    object_id: Oid,
    locktuple: bool,
) -> PgResult<Option<FormedTuple<'mcx>>> {
    // Re-open the caller's catalog under `mcx` for the scan (the caller already
    // holds it open; the lock is whatever it took — re-open AccessShare-safe via
    // the relid). We use the relation's own lock posture by re-opening with the
    // same row-exclusive default; objectaddress callers hold at least
    // AccessShareLock, and re-opening is idempotent.
    let r = table_open(mcx, catalog.rd_id, RowExclusiveLock)?;
    let keys = [oid_key(oidcol, object_id)?];
    let mut scan = genam::systable_beginscan::call(&r, InvalidOid, false, None, &keys)?;
    let tuple = genam::systable_getnext::call(mcx, scan.desc_mut())?;
    // if (locktup) LockTuple(catalog, &tuple->t_self, InplaceUpdateTupleLock);
    // The heavyweight tuple-tag lock is held until transaction end (released by
    // the transaction resource owner), so it is taken imperatively — mirroring
    // the C `LockTuple` in `get_catalog_object_by_oid_extended`.
    if locktuple {
        if let Some(t) = &tuple {
            backend_storage_lmgr_lmgr_seams::lock_tuple::call(
                catalog.rd_id,
                t.tuple.t_self,
                types_storage::lock::InplaceUpdateTupleLock,
            )?;
        }
    }
    scan.end()?;
    Ok(tuple)
}

/// `pg_get_acl`'s catalog read (objectaddress.c 4426). The caller
/// (objectaddress) has already resolved the catalog substitution
/// (`pg_largeobject` -> `pg_largeobject_metadata`) and the `aclitem[]` column
/// attnum (`anum_acl`) / OID column attnum (`anum_oid`) via
/// `get_object_attnum_acl` / `get_object_attnum_oid`, and decided whether this
/// is a relation-attribute ACL (`classId == RelationRelationId && objsubid !=
/// 0`). We read the `aclitem[]` column verbatim and return it as the raw
/// varlena `Datum` (the C `PG_RETURN_DATUM`), or `None` for `PG_RETURN_NULL`.
fn get_acl_datum<'mcx>(
    mcx: Mcx<'mcx>,
    catalog_id: Oid,
    anum_oid: i16,
    anum_acl: i16,
    object_id: Oid,
    objsubid: i32,
    is_relation_attr: bool,
) -> PgResult<Option<Datum<'mcx>>> {
    if is_relation_attr {
        // The ACL is retrieved from pg_attribute.attacl via
        // SearchSysCacheCopyAttNum(objectId, objsubid) (objectaddress.c).
        use backend_utils_cache_syscache as syscache;
        use types_cache::syscache::SysCacheKey;
        let attnum = objsubid as i16;
        let tup = syscache::SearchSysCache2(
            mcx,
            syscache::ATTNUM,
            SysCacheKey::Value(types_datum::Datum::from_oid(object_id)),
            SysCacheKey::Value(types_datum::Datum::from_i16(attnum)),
        )?;
        let Some(tup) = tup else {
            return Ok(None);
        };
        let (datum, isnull) = syscache::SysCacheGetAttr(
            mcx,
            syscache::ATTNUM,
            &tup,
            types_catalog::pg_attribute::Anum_pg_attribute_attacl as i32,
        )?;
        syscache::ReleaseSysCache(tup);
        if isnull {
            return Ok(None);
        }
        return Ok(Some(datum));
    }

    // rel = table_open(catalogId, AccessShareLock); the OID-keyed scan; then
    // heap_getattr(tup, Anum_acl, RelationGetDescr(rel), &isnull).
    let _ = objsubid;
    let rel = table_open(mcx, catalog_id, types_storage::lock::AccessShareLock)?;
    let tup = fetch_by_oid(mcx, &rel, anum_oid, object_id)?;
    let Some(tup) = tup else {
        rel.close(types_storage::lock::AccessShareLock)?;
        return Ok(None);
    };
    let (values, nulls) = deform(mcx, &rel, &tup)?;
    rel.close(types_storage::lock::AccessShareLock)?;
    let i = (anum_acl - 1) as usize;
    if nulls[i] {
        return Ok(None);
    }
    Ok(Some(values[i].clone()))
}

/// Install the F2 seam bodies. Wired from [`crate::init_seams`] via
/// [`crate::family1::install`]'s sibling [`install`].
pub fn install() {
    use backend_catalog_indexing_seams as s;

    // Engine pass-through + cluster family.
    s::catalog_tuple_delete::set(catalog_tuple_delete);
    s::catalog_open_indexes::set(catalog_open_indexes);
    s::catalog_close_indexes::set(catalog_close_indexes);
    s::catalog_tuple_update_pg_class::set(catalog_tuple_update_pg_class);
    s::update_pg_class_reloptions::set(update_pg_class_reloptions_seam);
    s::catalog_tuple_update_with_info_pg_class::set(catalog_tuple_update_with_info_pg_class);
    s::catalog_tuple_update_pg_index::set(catalog_tuple_update_pg_index);

    // pg_class single-field writes.
    s::set_pg_class_reltoastrelid::set(set_pg_class_reltoastrelid);
    s::set_pg_class_reltoastrelid_inplace::set(set_pg_class_reltoastrelid_inplace);
    s::set_relation_rule_status::set(set_relation_rule_status);
    s::set_pg_class_row_security::set(set_pg_class_row_security);
    s::set_pg_class_relhastriggers::set(set_pg_class_relhastriggers);
    s::set_pg_class_relreplident::set(set_pg_class_relreplident);
    s::set_pg_class_relrewrite::set(set_pg_class_relrewrite);
    s::set_index_isreplident::set(set_index_isreplident);

    // matview.c's SetMatViewPopulatedState pg_class write (cross-crate install:
    // the matview-deps seam's body is this pg_class single-field writer).
    backend_commands_matview_deps_seams::update_pg_class_populated::set(
        set_pg_class_relispopulated,
    );

    // pg_sequence.
    s::catalog_insert_pg_sequence::set(catalog_insert_pg_sequence);
    s::catalog_update_pg_sequence::set(catalog_update_pg_sequence);
    s::catalog_delete_pg_sequence::set(catalog_delete_pg_sequence);

    // functioncmds.c RemoveFunctionById catalog-delete leg (cross-crate install:
    // the seam is declared on backend-commands-functioncmds-seams but the body
    // needs this crate's CatalogTupleDelete + heap scan substrate).
    backend_commands_functioncmds_seams::remove_function_tuple::set(remove_function_tuple);

    // catalog/partition.c update_default_partition_oid pg_partitioned_table
    // single-field write (cross-crate install: the seam is declared on
    // backend-utils-cache-syscache-seams but the catalog-write substrate
    // (table_open + CatalogTupleUpdate) lives in this crate).
    backend_utils_cache_syscache_seams::update_default_partition_oid::set(
        update_default_partition_oid_catalog,
    );

    // pg_depend / pg_shdepend.
    s::catalog_tuple_update_pg_depend::set(catalog_tuple_update_pg_depend);
    s::catalog_tuple_insert_pg_shdepend::set(catalog_tuple_insert_pg_shdepend);
    s::catalog_tuple_update_pg_shdepend::set(catalog_tuple_update_pg_shdepend);

    // pg_constraint.
    s::catalog_tuple_insert_pg_constraint::set(catalog_tuple_insert_pg_constraint);
    s::catalog_tuple_update_pg_constraint::set(catalog_tuple_update_pg_constraint);

    // pg_trigger.
    s::catalog_tuple_update_pg_trigger::set(catalog_tuple_update_pg_trigger);

    // pg_type.
    s::catalog_tuple_insert_pg_type::set(catalog_tuple_insert_pg_type);
    s::catalog_tuple_update_pg_type::set(catalog_tuple_update_pg_type);
    s::catalog_tuple_update_typname_pg_type::set(catalog_tuple_update_typname_pg_type);
    // typecmds.c F3/F4 narrow single-column pg_type mutators.
    s::catalog_tuple_update_typowner_typacl_pg_type::set(catalog_tuple_update_typowner_typacl_pg_type);
    s::catalog_tuple_update_typnamespace_pg_type::set(catalog_tuple_update_typnamespace_pg_type);
    s::catalog_tuple_update_typnotnull_pg_type::set(catalog_tuple_update_typnotnull_pg_type);
    s::catalog_tuple_update_typdefault_pg_type::set(catalog_tuple_update_typdefault_pg_type);
    s::catalog_tuple_update_attrs_pg_type::set(catalog_tuple_update_attrs_pg_type);
    s::get_new_oid_with_index_pg_type::set(get_new_oid_with_index_pg_type);

    // pg_namespace.
    s::catalog_tuple_insert_pg_namespace::set(insert_pg_namespace);
    s::rename_namespace_tuple::set(rename_namespace_tuple);
    s::update_namespace_owner_tuple::set(update_namespace_owner_tuple);

    // Generic owner-tuple write (alter.c AlterObjectOwner_internal) + pg_get_acl
    // catalog read (objectaddress.c).
    s::update_object_owner_tuple::set(update_object_owner_tuple);
    s::get_acl_datum::set(get_acl_datum);

    // pg_foreign_* / pg_user_mapping.
    s::catalog_tuple_insert_pg_foreign_data_wrapper::set(catalog_tuple_insert_pg_foreign_data_wrapper);
    s::catalog_tuple_update_pg_foreign_data_wrapper::set(catalog_tuple_update_pg_foreign_data_wrapper);
    s::catalog_tuple_update_owner_pg_foreign_data_wrapper::set(catalog_tuple_update_owner_pg_foreign_data_wrapper);
    s::catalog_tuple_insert_pg_foreign_server::set(catalog_tuple_insert_pg_foreign_server);
    s::catalog_tuple_update_pg_foreign_server::set(catalog_tuple_update_pg_foreign_server);
    s::catalog_tuple_update_owner_pg_foreign_server::set(catalog_tuple_update_owner_pg_foreign_server);
    s::catalog_tuple_insert_pg_user_mapping::set(catalog_tuple_insert_pg_user_mapping);
    s::catalog_tuple_update_pg_user_mapping::set(catalog_tuple_update_pg_user_mapping);
    s::catalog_tuple_insert_pg_foreign_table::set(catalog_tuple_insert_pg_foreign_table);

    // pg_db_role_setting.
    s::decode_db_role_setting_setconfig::set(decode_db_role_setting_setconfig);
    s::catalog_tuple_update_pg_db_role_setting::set(catalog_tuple_update_pg_db_role_setting);
    s::catalog_tuple_insert_pg_db_role_setting::set(catalog_tuple_insert_pg_db_role_setting);

    // pg_largeobject.
    s::deform_lo_page::set(deform_lo_page);
    s::catalog_tuple_insert_with_info_pg_largeobject::set(catalog_tuple_insert_with_info_pg_largeobject);
    s::catalog_tuple_update_with_info_pg_largeobject::set(catalog_tuple_update_with_info_pg_largeobject);

    // objectaddress.
    s::get_catalog_object_by_oid::set(get_catalog_object_by_oid);

    // catalog/index.c pg_class in-place stats producer (sub-keystone #348): the
    // `index_update_stats` seam is declared on `backend-catalog-index-seams`
    // (index.c's owner), installed here because this is the pg_class-write layer.
    backend_catalog_index_seams::index_update_stats::set(index_update_stats);

    // SetRelationNumChecks disk-store branch (heap.c): the pg_class `relchecks`
    // write, declared on heap-seams (its owner) but installed here, the
    // pg_class-write layer.
    backend_catalog_heap_seams::set_relation_num_checks::set(set_relation_num_checks);

    // SetRelationHasSubclass catalog body (tablecmds.c): the pg_class
    // `relhassubclass` write, declared on tablecmds-seams (its owner) but
    // installed here, the pg_class-write layer. `Ok(false)` (no tuple) maps to
    // the C `cache lookup failed for relation %u` elog(ERROR).
    backend_commands_tablecmds_seams::set_relation_has_subclass_catalog::set(
        |relid, relhassubclass| {
            if !set_relation_has_subclass_catalog(relid, relhassubclass)? {
                return Err(PgError::error(format!(
                    "cache lookup failed for relation {relid}"
                )));
            }
            Ok(())
        },
    );

    // update_relispartition catalog body (indexcmds.c): the pg_class
    // `relispartition` write of IndexSetParentIndex.
    backend_commands_tablecmds_seams::update_relispartition_catalog::set(|relid, newval| {
        if !update_relispartition_catalog(relid, newval)? {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relid}"
            )));
        }
        Ok(())
    });

    // index_mark_invalid catalog body (indexcmds.c): the pg_index `indisvalid`
    // clear of DefineIndex's partitioned-recursion invalidate_parent path.
    backend_catalog_index_seams::index_mark_invalid::set(|relid| {
        if !index_mark_invalid_catalog(relid)? {
            return Err(PgError::error(format!(
                "cache lookup failed for index {relid}"
            )));
        }
        Ok(())
    });

    // index_mark_valid catalog body (tablecmds.c validatePartitionedIndex): set
    // pg_index `indisvalid = true` once all leaf partitions have a valid index.
    backend_catalog_index_seams::index_mark_valid::set(|relid| {
        if !index_mark_valid_catalog(relid)? {
            return Err(PgError::error(format!(
                "cache lookup failed for index {relid}"
            )));
        }
        Ok(())
    });
}
