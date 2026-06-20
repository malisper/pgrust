//! F3 — the DDL-cluster catalog-write producers (`catalog/heap.c`):
//! `InsertPgClassTuple`, `InsertPgAttributeTuples`, and `StoreAttrDefault`'s
//! pg_attrdef insert.
//!
//! Each producer forms the catalog heap tuple from a typed `*InsertRow` against
//! the open catalog relation's descriptor (`heap_form_tuple(RelationGetDescr(
//! rel), values, nulls)`) and calls the F0 engine ([`crate::keystone`]). These
//! are the form-and-insert leaves the unported `catalog/heap.c`
//! (`heap_create_with_catalog`) drives; they install over the DDL-cluster seams
//! in `backend-catalog-indexing-seams`.
//!
//! The variable-length / array columns (`relacl` / `reloptions` /
//! `attoptions`) arrive as already-built on-disk varlena byte images (the C
//! caller built them: `relacl` from `aclitem[]`, `reloptions` /`attoptions`
//! from `text[]`); the producer wraps each image as a by-reference Datum
//! unchanged. The `pg_node_tree` text columns (`adbin`) are rendered to a
//! `text` varlena here (the C `CStringGetTextDatum(nodeToString(expr))`).

#![allow(non_snake_case)]

use mcx::Mcx;
use types_catalog as cat;
use types_error::PgResult;
use types_rel::Relation;
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_access_common_heaptuple::heap_form_tuple;
use backend_access_table_table::table_open;
use backend_catalog_catalog::GetNewOidWithIndex;
use types_storage::lock::RowExclusiveLock;

use crate::keystone::{CatalogCloseIndexes, CatalogOpenIndexes, CatalogTupleInsert};

/// `NameGetDatum(&name)` over a 64-byte `NameData` image (a by-reference Datum
/// over the column's on-disk bytes; the `name` type is fixed-length 64 stored
/// inline). The `InsertRow` carriers already hold the NUL-padded image
/// (`namestrcpy` ran in the caller), so this wraps the bytes unchanged.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, image: &[u8; 64]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, &image[..])?))
}

/// Wrap an already-built on-disk varlena image (an `aclitem[]` / `text[]`
/// array, with its 4-byte length header) as a by-reference Datum, unchanged.
fn bytes_datum<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<Datum<'mcx>> {
    Ok(Datum::ByRef(mcx::slice_in(mcx, bytes)?))
}

/// `CStringGetTextDatum(s)` — a `text` varlena image (4-byte header
/// `SET_VARSIZE(len + VARHDRSZ)` then the payload), carried as `Datum::ByRef`.
/// Used for the `pg_node_tree` text columns (`adbin`).
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    let payload = s.as_bytes();
    let total = 4 + payload.len();
    let word = (total as u32) << 2;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.extend_from_slice(&word.to_ne_bytes());
    buf.extend_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/* ======================================================================== *
 * pg_class — InsertPgClassTuple (catalog/heap.c).
 * ======================================================================== */

/// `InsertPgClassTuple(pg_class_desc, new_rel_desc, new_rel_oid, relacl,
/// reloptions)` (catalog/heap.c): build the full 34-column `pg_class` row from
/// the new relation's `rd_rel` (carried in [`cat::pg_class::PgClassInsertRow`]),
/// `heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls)`, and
/// `CatalogTupleInsert(pg_class_desc, tup)`.
fn catalog_tuple_insert_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_class::PgClassInsertRow,
) -> PgResult<()> {
    use cat::pg_class as pc;

    // memset(values, 0, ...); memset(nulls, false, ...);
    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    for _ in 0..pc::Natts_pg_class {
        values.push(Datum::null());
        nulls.push(false);
    }

    // The fixed columns, in pg_class.h field order (oid=1 .. relminmxid=31).
    values[pc::Anum_pg_class_oid as usize - 1] = Datum::from_oid(row.oid);
    values[pc::Anum_pg_class_relname as usize - 1] = name_datum(mcx, &row.relname)?;
    values[pc::Anum_pg_class_relnamespace as usize - 1] = Datum::from_oid(row.relnamespace);
    values[pc::Anum_pg_class_reltype as usize - 1] = Datum::from_oid(row.reltype);
    values[pc::Anum_pg_class_reloftype as usize - 1] = Datum::from_oid(row.reloftype);
    values[pc::Anum_pg_class_relowner as usize - 1] = Datum::from_oid(row.relowner);
    values[pc::Anum_pg_class_relam as usize - 1] = Datum::from_oid(row.relam);
    values[pc::Anum_pg_class_relfilenode as usize - 1] = Datum::from_oid(row.relfilenode);
    values[pc::Anum_pg_class_reltablespace as usize - 1] = Datum::from_oid(row.reltablespace);
    values[pc::Anum_pg_class_relpages as usize - 1] = Datum::from_i32(row.relpages);
    values[pc::Anum_pg_class_reltuples as usize - 1] = Datum::from_f32(row.reltuples);
    values[pc::Anum_pg_class_relallvisible as usize - 1] = Datum::from_i32(row.relallvisible);
    values[pc::Anum_pg_class_relallfrozen as usize - 1] = Datum::from_i32(row.relallfrozen);
    values[pc::Anum_pg_class_reltoastrelid as usize - 1] = Datum::from_oid(row.reltoastrelid);
    values[pc::Anum_pg_class_relhasindex as usize - 1] = Datum::from_bool(row.relhasindex);
    values[pc::Anum_pg_class_relisshared as usize - 1] = Datum::from_bool(row.relisshared);
    values[pc::Anum_pg_class_relpersistence as usize - 1] = Datum::from_char(row.relpersistence);
    values[pc::Anum_pg_class_relkind as usize - 1] = Datum::from_char(row.relkind);
    values[pc::Anum_pg_class_relnatts as usize - 1] = Datum::from_i16(row.relnatts);
    values[pc::Anum_pg_class_relchecks as usize - 1] = Datum::from_i16(row.relchecks);
    values[pc::Anum_pg_class_relhasrules as usize - 1] = Datum::from_bool(row.relhasrules);
    values[pc::Anum_pg_class_relhastriggers as usize - 1] = Datum::from_bool(row.relhastriggers);
    values[pc::Anum_pg_class_relrowsecurity as usize - 1] = Datum::from_bool(row.relrowsecurity);
    values[pc::Anum_pg_class_relforcerowsecurity as usize - 1] =
        Datum::from_bool(row.relforcerowsecurity);
    values[pc::Anum_pg_class_relhassubclass as usize - 1] = Datum::from_bool(row.relhassubclass);
    values[pc::Anum_pg_class_relispopulated as usize - 1] = Datum::from_bool(row.relispopulated);
    values[pc::Anum_pg_class_relreplident as usize - 1] = Datum::from_char(row.relreplident);
    values[pc::Anum_pg_class_relispartition as usize - 1] = Datum::from_bool(row.relispartition);
    values[pc::Anum_pg_class_relrewrite as usize - 1] = Datum::from_oid(row.relrewrite);
    values[pc::Anum_pg_class_relfrozenxid as usize - 1] =
        Datum::from_transaction_id(row.relfrozenxid);
    values[pc::Anum_pg_class_relminmxid as usize - 1] = Datum::from_u32(row.relminmxid);

    // if (relacl != (Datum) 0) values[..relacl] = relacl; else nulls[..] = true;
    match &row.relacl {
        Some(image) => {
            values[pc::Anum_pg_class_relacl as usize - 1] = bytes_datum(mcx, image)?;
        }
        None => nulls[pc::Anum_pg_class_relacl as usize - 1] = true,
    }
    // if (reloptions != (Datum) 0) values[..reloptions] = reloptions; else NULL.
    match &row.reloptions {
        Some(image) => {
            values[pc::Anum_pg_class_reloptions as usize - 1] = bytes_datum(mcx, image)?;
        }
        None => nulls[pc::Anum_pg_class_reloptions as usize - 1] = true,
    }

    // relpartbound is set by updating this tuple, if necessary.
    nulls[pc::Anum_pg_class_relpartbound as usize - 1] = true;

    // tup = heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    // CatalogTupleInsert(pg_class_desc, tup); heap_freetuple(tup);
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/* ======================================================================== *
 * pg_attribute — InsertPgAttributeTuples (catalog/heap.c).
 * ======================================================================== */

/// `InsertPgAttributeTuples(pg_attribute_rel, tupdesc, new_rel_oid, extra,
/// indstate)` (catalog/heap.c): form one `pg_attribute` heap tuple per row and
/// multi-insert the batch (`CatalogOpenIndexes` +
/// `CatalogTuplesMultiInsertWithInfo` + `CatalogCloseIndexes`).
///
/// The C batches into `MAX_CATALOG_MULTI_INSERT_BYTES`-sized slot windows and
/// re-uses an existing `indstate` when passed; the typed-row design opens the
/// indexes once for the whole batch (the index-state lifecycle is per-call
/// here, which is logic-invisible — the same index entries are inserted). The
/// caller pre-resolves each row's `attrelid` (the C
/// `new_rel_oid != InvalidOid ? new_rel_oid : attrs->attrelid` selection).
fn catalog_insert_pg_attribute_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    rows: &[cat::pg_attribute::PgAttributeInsertRow],
) -> PgResult<()> {
    use types_tuple::backend_access_common_heaptuple::FormedTuple;

    // /* Nothing to do */ — no rows, no index work.
    if rows.is_empty() {
        return Ok(());
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tuples: mcx::PgVec<'mcx, FormedTuple<'mcx>> = mcx::vec_with_capacity_in(mcx, rows.len())?;
    for row in rows {
        let (values, nulls) = attribute_values(mcx, row)?;
        let tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
        tuples.push(tup);
    }

    // indstate = CatalogOpenIndexes(pg_attribute_rel);
    let mut indstate = CatalogOpenIndexes(mcx, rel)?;
    // CatalogTuplesMultiInsertWithInfo(pg_attribute_rel, slot, slotCount, indstate);
    crate::keystone::CatalogTuplesMultiInsertWithInfo(mcx, rel, tuples, &mut indstate)?;
    // CatalogCloseIndexes(indstate);
    CatalogCloseIndexes(indstate)
}

/// `AppendAttributeTuples(indexRelation, attopts, stattargets)`
/// (catalog/index.c:511): `table_open(AttributeRelationId, RowExclusiveLock)`,
/// build one `pg_attribute` row per index column from
/// `RelationGetDescr(indexRelation)` (the per-attno `Form_pg_attribute`, whose
/// `attrelid` `InitializeAttributeOids` already scribbled with the new index's
/// OID — the C `new_rel_oid == InvalidOid` branch reads `attrs->attrelid`), and
/// `InsertPgAttributeTuples`. The `attopts`/`stattargets` overrides ride in
/// optional parallel arrays indexed by attno-1; the C builds `attrs_extra` only
/// when `attopts != NULL`, so when `attopts` is `None` every row's
/// `attstattarget`/`attoptions` is SQL NULL.
fn append_attribute_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    index_relation: &Relation<'mcx>,
    attopts: Option<&[Option<std::vec::Vec<u8>>]>,
    stattargets: Option<&[Option<i16>]>,
) -> PgResult<()> {
    // indexTupDesc = RelationGetDescr(indexRelation);
    let tupdesc = index_relation.rd_att_clone_in(mcx)?;
    let natts = tupdesc.natts as usize;

    let mut rows: std::vec::Vec<cat::pg_attribute::PgAttributeInsertRow> =
        std::vec::Vec::with_capacity(natts);
    for i in 0..natts {
        let a = tupdesc.attr(i);
        // attrs_extra[i]: only populated when the C `attopts != NULL`.
        let (attstattarget, attoptions) = if attopts.is_some() {
            let opt = attopts.and_then(|o| o.get(i)).and_then(|v| v.clone());
            // attrs_extra[i].attstattarget = stattargets[i] (else isnull).
            let stat = stattargets.and_then(|s| s.get(i).copied()).flatten();
            (stat, opt)
        } else {
            // attrs_extra == NULL ⇒ both fields default to SQL NULL.
            (None, None)
        };
        rows.push(cat::pg_attribute::PgAttributeInsertRow {
            // new_rel_oid == InvalidOid ⇒ attrs->attrelid (set by InitializeAttributeOids).
            attrelid: a.attrelid,
            attname: a.attname.data,
            atttypid: a.atttypid,
            attlen: a.attlen,
            attnum: a.attnum,
            atttypmod: a.atttypmod,
            attndims: a.attndims,
            attbyval: a.attbyval,
            attalign: a.attalign,
            attstorage: a.attstorage,
            attcompression: a.attcompression,
            attnotnull: a.attnotnull,
            atthasdef: a.atthasdef,
            atthasmissing: a.atthasmissing,
            attidentity: a.attidentity,
            attgenerated: a.attgenerated,
            attisdropped: a.attisdropped,
            attislocal: a.attislocal,
            attinhcount: a.attinhcount,
            attcollation: a.attcollation,
            attstattarget,
            attoptions,
        });
    }

    // pg_attribute = table_open(AttributeRelationId, RowExclusiveLock);
    let pg_attribute = table_open(mcx, cat::pg_attribute::AttributeRelationId, RowExclusiveLock)?;
    // InsertPgAttributeTuples(pg_attribute, indexTupDesc, InvalidOid, attrs_extra, indstate)
    // — CatalogOpenIndexes / MultiInsert / CatalogCloseIndexes happen inside.
    catalog_insert_pg_attribute_tuples(mcx, &pg_attribute, &rows)?;
    // table_close(pg_attribute, RowExclusiveLock);
    pg_attribute.close(RowExclusiveLock)
}

/// The `values[]` / `nulls[]` for one `pg_attribute` row (the per-slot fill in
/// `InsertPgAttributeTuples`).
fn attribute_values<'mcx>(
    mcx: Mcx<'mcx>,
    row: &cat::pg_attribute::PgAttributeInsertRow,
) -> PgResult<(mcx::PgVec<'mcx, Datum<'mcx>>, mcx::PgVec<'mcx, bool>)> {
    use cat::pg_attribute as pa;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pa::Natts_pg_attribute)?;
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pa::Natts_pg_attribute)?;
    for _ in 0..pa::Natts_pg_attribute {
        values.push(Datum::null());
        nulls.push(false);
    }

    // The fixed-layout part (columns 1..=20), from the Form_pg_attribute.
    values[pa::Anum_pg_attribute_attrelid as usize - 1] = Datum::from_oid(row.attrelid);
    values[pa::Anum_pg_attribute_attname as usize - 1] = name_datum(mcx, &row.attname)?;
    values[pa::Anum_pg_attribute_atttypid as usize - 1] = Datum::from_oid(row.atttypid);
    values[pa::Anum_pg_attribute_attlen as usize - 1] = Datum::from_i16(row.attlen);
    values[pa::Anum_pg_attribute_attnum as usize - 1] = Datum::from_i16(row.attnum);
    values[pa::Anum_pg_attribute_atttypmod as usize - 1] = Datum::from_i32(row.atttypmod);
    values[pa::Anum_pg_attribute_attndims as usize - 1] = Datum::from_i16(row.attndims);
    values[pa::Anum_pg_attribute_attbyval as usize - 1] = Datum::from_bool(row.attbyval);
    values[pa::Anum_pg_attribute_attalign as usize - 1] = Datum::from_char(row.attalign);
    values[pa::Anum_pg_attribute_attstorage as usize - 1] = Datum::from_char(row.attstorage);
    values[pa::Anum_pg_attribute_attcompression as usize - 1] =
        Datum::from_char(row.attcompression);
    values[pa::Anum_pg_attribute_attnotnull as usize - 1] = Datum::from_bool(row.attnotnull);
    values[pa::Anum_pg_attribute_atthasdef as usize - 1] = Datum::from_bool(row.atthasdef);
    values[pa::Anum_pg_attribute_atthasmissing as usize - 1] = Datum::from_bool(row.atthasmissing);
    values[pa::Anum_pg_attribute_attidentity as usize - 1] = Datum::from_char(row.attidentity);
    values[pa::Anum_pg_attribute_attgenerated as usize - 1] = Datum::from_char(row.attgenerated);
    values[pa::Anum_pg_attribute_attisdropped as usize - 1] = Datum::from_bool(row.attisdropped);
    values[pa::Anum_pg_attribute_attislocal as usize - 1] = Datum::from_bool(row.attislocal);
    values[pa::Anum_pg_attribute_attinhcount as usize - 1] = Datum::from_i16(row.attinhcount);
    values[pa::Anum_pg_attribute_attcollation as usize - 1] = Datum::from_oid(row.attcollation);

    // attstattarget / attoptions from FormExtraData_pg_attribute (NULL when
    // unset, the no-`tupdesc_extra` path or an explicitly-null extra field).
    match row.attstattarget {
        Some(v) => values[pa::Anum_pg_attribute_attstattarget as usize - 1] = Datum::from_i16(v),
        None => nulls[pa::Anum_pg_attribute_attstattarget as usize - 1] = true,
    }
    match &row.attoptions {
        Some(image) => {
            values[pa::Anum_pg_attribute_attoptions as usize - 1] = bytes_datum(mcx, image)?
        }
        None => nulls[pa::Anum_pg_attribute_attoptions as usize - 1] = true,
    }

    // The remaining fields are not set for new columns.
    nulls[pa::Anum_pg_attribute_attacl as usize - 1] = true;
    nulls[pa::Anum_pg_attribute_attfdwoptions as usize - 1] = true;
    nulls[pa::Anum_pg_attribute_attmissingval as usize - 1] = true;

    Ok((values, nulls))
}

/// The `ALTER TABLE` per-`Anum` `pg_attribute` field-modify path (the `ATExec*`
/// pattern, commands/tablecmds.c): `heap_modify_tuple` over the
/// selectively-replaced columns carried in [`cat::pg_attribute::PgAttributeUpdateRow`],
/// then `CatalogTupleUpdate`. The caller holds the original scanned tuple (the C
/// `SearchSysCacheCopy(ATTNUM, relid, attnum)` copy); the non-replaced columns
/// are preserved by `heap_modify_tuple`, and the update is applied at
/// `attr_tuple->t_self`.
fn catalog_tuple_update_pg_attribute<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attr_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    row: &cat::pg_attribute::PgAttributeUpdateRow,
) -> PgResult<()> {
    use cat::pg_attribute as pa;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pa::Natts_pg_attribute)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pa::Natts_pg_attribute)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pa::Natts_pg_attribute)?;
    for _ in 0..pa::Natts_pg_attribute {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    // Helper: mark a non-null fixed column for replacement and store its value.
    // (Each corresponds to a C `replaces[Anum_xxx - 1] = true; values[..] = v`.)
    macro_rules! set_col {
        ($anum:ident, $field:expr, $datum:expr) => {
            if let Some(v) = $field {
                let i = pa::$anum as usize - 1;
                replaces[i] = true;
                values[i] = $datum(v);
            }
        };
    }

    if let Some(image) = &row.attname {
        let i = pa::Anum_pg_attribute_attname as usize - 1;
        replaces[i] = true;
        values[i] = name_datum(mcx, image)?;
    }
    set_col!(Anum_pg_attribute_atttypid, row.atttypid, Datum::from_oid);
    set_col!(Anum_pg_attribute_attlen, row.attlen, Datum::from_i16);
    set_col!(Anum_pg_attribute_atttypmod, row.atttypmod, Datum::from_i32);
    set_col!(Anum_pg_attribute_attndims, row.attndims, Datum::from_i16);
    set_col!(Anum_pg_attribute_attbyval, row.attbyval, Datum::from_bool);
    set_col!(Anum_pg_attribute_attalign, row.attalign, Datum::from_char);
    set_col!(Anum_pg_attribute_attstorage, row.attstorage, Datum::from_char);
    set_col!(
        Anum_pg_attribute_attcompression,
        row.attcompression,
        Datum::from_char
    );
    set_col!(Anum_pg_attribute_attnotnull, row.attnotnull, Datum::from_bool);
    set_col!(Anum_pg_attribute_atthasdef, row.atthasdef, Datum::from_bool);
    set_col!(
        Anum_pg_attribute_atthasmissing,
        row.atthasmissing,
        Datum::from_bool
    );
    set_col!(
        Anum_pg_attribute_attidentity,
        row.attidentity,
        Datum::from_char
    );
    set_col!(
        Anum_pg_attribute_attgenerated,
        row.attgenerated,
        Datum::from_char
    );
    set_col!(
        Anum_pg_attribute_attisdropped,
        row.attisdropped,
        Datum::from_bool
    );
    set_col!(Anum_pg_attribute_attislocal, row.attislocal, Datum::from_bool);
    set_col!(
        Anum_pg_attribute_attinhcount,
        row.attinhcount,
        Datum::from_i16
    );
    set_col!(
        Anum_pg_attribute_attcollation,
        row.attcollation,
        Datum::from_oid
    );

    // attstattarget — Some(None) stores SQL NULL (SET STATISTICS DEFAULT).
    if let Some(stat) = &row.attstattarget {
        let i = pa::Anum_pg_attribute_attstattarget as usize - 1;
        replaces[i] = true;
        match stat {
            Some(v) => values[i] = Datum::from_i16(*v),
            None => isnull[i] = true,
        }
    }
    // attoptions — Some(None) stores SQL NULL (RESET).
    if let Some(opts) = &row.attoptions {
        let i = pa::Anum_pg_attribute_attoptions as usize - 1;
        replaces[i] = true;
        match opts {
            Some(image) => values[i] = bytes_datum(mcx, image)?,
            None => isnull[i] = true,
        }
    }
    // attmissingval — Some(None) stores SQL NULL (RelationClearMissing),
    // Some(Some(image)) stores the anyarray varlena (StoreAttrMissingVal).
    if let Some(missing) = &row.attmissingval {
        let i = pa::Anum_pg_attribute_attmissingval as usize - 1;
        replaces[i] = true;
        match missing {
            Some(image) => values[i] = bytes_datum(mcx, image)?,
            None => isnull[i] = true,
        }
    }
    // attacl — Some(Some(image)) stores the aclitem[] varlena
    // (change_owner_fix_column_acls), Some(None) stores SQL NULL.
    if let Some(acl) = &row.attacl {
        let i = pa::Anum_pg_attribute_attacl as usize - 1;
        replaces[i] = true;
        match acl {
            Some(image) => values[i] = bytes_datum(mcx, image)?,
            None => isnull[i] = true,
        }
    }

    // new_tuple = heap_modify_tuple(attr_tuple, RelationGetDescr(pg_attribute_rel),
    //                               values, isnull, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, attr_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    // CatalogTupleUpdate(pg_attribute_rel, &new_tuple->t_self, new_tuple);
    crate::keystone::CatalogTupleUpdate(mcx, rel, attr_tuple.tuple.t_self, &mut new_tuple)
}

/// The `pg_class.relchecks`-preserving field-modify path (the C
/// `RemoveConstraintById` / `MergeConstraintsIntoExisting` /
/// `StoreRelCheck` pattern, catalog/pg_constraint.c & commands/tablecmds.c):
/// `heap_modify_tuple` replacing ONLY the `relchecks` column over the original
/// scanned `pg_class` tuple, then `CatalogTupleUpdate`. The caller holds the
/// original `SearchSysCacheCopy1(RELOID, relid)` copy; `heap_modify_tuple`
/// preserves all other 33 columns (no lossy reform of the fixed-length
/// `Form_pg_class`), and the update is applied at `class_tuple->t_self`. This
/// is the relchecks-shaped analog of [`catalog_tuple_update_pg_attribute`].
fn catalog_tuple_update_relchecks_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    class_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    new_relchecks: i16,
) -> PgResult<()> {
    use cat::pg_class as pc;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    for _ in 0..pc::Natts_pg_class {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    // classForm->relchecks-- ; the C path mutates the Form_pg_class field in
    // place and updates the whole tuple. We replace only that one column.
    let i = pc::Anum_pg_class_relchecks as usize - 1;
    replaces[i] = true;
    values[i] = Datum::from_i16(new_relchecks);

    // new_tuple = heap_modify_tuple(relTup, RelationGetDescr(pgrel),
    //                               values, isnull, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, class_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    // CatalogTupleUpdate(pgrel, &relTup->t_self, relTup);
    crate::keystone::CatalogTupleUpdate(mcx, rel, class_tuple.tuple.t_self, &mut new_tuple)
}

/// The `pg_class.relowner`/`relacl`-preserving field-modify path
/// (`ATExecChangeOwner`, commands/tablecmds.c): replace ONLY the `relowner`
/// column (and, when `new_acl` is `Some`, the `relacl` column) over the original
/// scanned `pg_class` tuple, then `CatalogTupleUpdate`. The remaining columns are
/// preserved verbatim (no lossy reform of the fixed-length `Form_pg_class`). The
/// owner-change path computes `new_acl` via `aclnewowner` only when the existing
/// relacl is non-null (else `None` leaves the column untouched). This is the
/// owner-shaped analog of [`catalog_tuple_update_relchecks_pg_class`].
fn catalog_tuple_update_relowner_pg_class<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    class_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    new_owner_id: types_core::Oid,
    new_acl: Option<Datum<'mcx>>,
) -> PgResult<()> {
    use cat::pg_class as pc;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pc::Natts_pg_class)?;
    for _ in 0..pc::Natts_pg_class {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    // repl_repl[Anum_pg_class_relowner - 1] = true;
    // repl_val[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(newOwnerId);
    let i = pc::Anum_pg_class_relowner as usize - 1;
    replaces[i] = true;
    values[i] = Datum::from_oid(new_owner_id);

    // if (!isNull) { repl_repl[Anum_pg_class_relacl - 1] = true;
    //               repl_val[Anum_pg_class_relacl - 1] = PointerGetDatum(newAcl); }
    if let Some(acl_datum) = new_acl {
        let j = pc::Anum_pg_class_relacl as usize - 1;
        replaces[j] = true;
        values[j] = acl_datum;
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, class_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    crate::keystone::CatalogTupleUpdate(mcx, rel, class_tuple.tuple.t_self, &mut new_tuple)
}

/* ======================================================================== *
 * pg_attrdef — StoreAttrDefault's insert (catalog/heap.c).
 * ======================================================================== */

/// `StoreAttrDefault`'s pg_attrdef INSERT (catalog/heap.c): allocate the
/// pg_attrdef OID (`GetNewOidWithIndex(adrel, AttrDefaultOidIndexId,
/// Anum_pg_attrdef_oid)`), form the 4-column row (`oid`, `adrelid`, `adnum`,
/// `adbin` pg_node_tree text), `CatalogTupleInsert`, and return the OID.
fn catalog_tuple_insert_pg_attrdef<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_attrdef::PgAttrdefInsertRow,
) -> PgResult<types_core::Oid> {
    use cat::pg_attrdef as pd;

    // attrdefOid = GetNewOidWithIndex(adrel, AttrDefaultOidIndexId,
    //                                 Anum_pg_attrdef_oid);
    let attrdef_oid =
        GetNewOidWithIndex(rel, pd::AttrDefaultOidIndexId, pd::Anum_pg_attrdef_oid)?;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pd::Natts_pg_attrdef)?;
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pd::Natts_pg_attrdef)?;
    for _ in 0..pd::Natts_pg_attrdef {
        values.push(Datum::null());
        nulls.push(false);
    }

    // values[Anum_pg_attrdef_oid - 1]    = ObjectIdGetDatum(attrdefOid);
    // values[Anum_pg_attrdef_adrelid - 1]= ObjectIdGetDatum(RelationGetRelid(rel));
    // values[Anum_pg_attrdef_adnum - 1]  = Int16GetDatum(attnum);
    // values[Anum_pg_attrdef_adbin - 1]  = CStringGetTextDatum(adbin);
    values[pd::Anum_pg_attrdef_oid as usize - 1] = Datum::from_oid(attrdef_oid);
    values[pd::Anum_pg_attrdef_adrelid as usize - 1] = Datum::from_oid(row.adrelid);
    values[pd::Anum_pg_attrdef_adnum as usize - 1] = Datum::from_i16(row.adnum);
    values[pd::Anum_pg_attrdef_adbin as usize - 1] = cstring_to_text_datum(mcx, &row.adbin)?;

    // tuple = heap_form_tuple(adrel->rd_att, values, nulls);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    // CatalogTupleInsert(adrel, tuple);
    CatalogTupleInsert(mcx, rel, &mut tup)?;

    Ok(attrdef_oid)
}

/* ======================================================================== *
 * pg_index — UpdateIndexRelation (catalog/index.c).
 * ======================================================================== */

/// `buildint2vector(int2s, n)` (utils/adt/int.c): the on-disk `int2vector`
/// image — a varlena whose header (`SET_VARSIZE`, then `ndim=1`,
/// `dataoffset=0`, `elemtype=INT2OID`, `dim1=n`, `lbound1=0`) is followed by the
/// `n` `int16` values. `Int2VectorSize(n) = offsetof(int2vector, values) + n *
/// sizeof(int16) = 24 + 2n`. Returned as the verbatim `Datum::ByRef` bytes.
fn buildint2vector<'mcx>(mcx: Mcx<'mcx>, int2s: &[i16]) -> PgResult<Datum<'mcx>> {
    const INT2OID: u32 = 21;
    const HEADER: usize = 24;
    let n = int2s.len();
    let total = HEADER + n * core::mem::size_of::<i16>();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes());
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes());
    buf[12..16].copy_from_slice(&INT2OID.to_ne_bytes());
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes());
    for (i, v) in int2s.iter().enumerate() {
        let off = HEADER + i * 2;
        buf[off..off + 2].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `buildoidvector(oids, n)` (utils/adt/oid.c): the on-disk `oidvector` image —
/// the same `int2vector`-shaped fixed-layout struct, but `elemtype=OIDOID` and
/// 4-byte `Oid` values. `OidVectorSize(n) = offsetof(oidvector, values) + n *
/// sizeof(Oid) = 24 + 4n`. Returned as the verbatim `Datum::ByRef` bytes.
fn buildoidvector<'mcx>(
    mcx: Mcx<'mcx>,
    oids: &[types_core::primitive::Oid],
) -> PgResult<Datum<'mcx>> {
    const OIDOID: u32 = 26;
    const HEADER: usize = 24;
    let n = oids.len();
    let total = HEADER + n * core::mem::size_of::<u32>();
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[4..8].copy_from_slice(&1i32.to_ne_bytes());
    buf[8..12].copy_from_slice(&0i32.to_ne_bytes());
    buf[12..16].copy_from_slice(&OIDOID.to_ne_bytes());
    buf[16..20].copy_from_slice(&(n as i32).to_ne_bytes());
    buf[20..24].copy_from_slice(&0i32.to_ne_bytes());
    for (i, v) in oids.iter().enumerate() {
        let off = HEADER + i * 4;
        buf[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    }
    Ok(Datum::ByRef(buf))
}

/// `UpdateIndexRelation` (catalog/index.c): build the full 21-column `pg_index`
/// row from the typed [`cat::pg_index::PgIndexInsertRow`],
/// `heap_form_tuple(RelationGetDescr(pg_index), values, nulls)`, and
/// `CatalogTupleInsert(pg_index, tuple)`.
fn catalog_tuple_insert_pg_index<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_index::PgIndexInsertRow,
) -> PgResult<()> {
    use cat::pg_index as pi;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pi::Natts_pg_index)?;
    let mut nulls: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pi::Natts_pg_index)?;
    for _ in 0..pi::Natts_pg_index {
        values.push(Datum::null());
        nulls.push(false);
    }

    // values[Anum_pg_index_indexrelid - 1] = ObjectIdGetDatum(indexoid); ...
    values[pi::Anum_pg_index_indexrelid as usize - 1] = Datum::from_oid(row.indexrelid);
    values[pi::Anum_pg_index_indrelid as usize - 1] = Datum::from_oid(row.indrelid);
    values[pi::Anum_pg_index_indnatts as usize - 1] = Datum::from_i16(row.indnatts);
    values[pi::Anum_pg_index_indnkeyatts as usize - 1] = Datum::from_i16(row.indnkeyatts);
    values[pi::Anum_pg_index_indisunique as usize - 1] = Datum::from_bool(row.indisunique);
    values[pi::Anum_pg_index_indnullsnotdistinct as usize - 1] =
        Datum::from_bool(row.indnullsnotdistinct);
    values[pi::Anum_pg_index_indisprimary as usize - 1] = Datum::from_bool(row.indisprimary);
    values[pi::Anum_pg_index_indisexclusion as usize - 1] = Datum::from_bool(row.indisexclusion);
    values[pi::Anum_pg_index_indimmediate as usize - 1] = Datum::from_bool(row.indimmediate);
    values[pi::Anum_pg_index_indisclustered as usize - 1] = Datum::from_bool(row.indisclustered);
    values[pi::Anum_pg_index_indisvalid as usize - 1] = Datum::from_bool(row.indisvalid);
    values[pi::Anum_pg_index_indcheckxmin as usize - 1] = Datum::from_bool(row.indcheckxmin);
    values[pi::Anum_pg_index_indisready as usize - 1] = Datum::from_bool(row.indisready);
    values[pi::Anum_pg_index_indislive as usize - 1] = Datum::from_bool(row.indislive);
    values[pi::Anum_pg_index_indisreplident as usize - 1] = Datum::from_bool(row.indisreplident);

    // values[indkey] = PointerGetDatum(buildint2vector(...)); the attnums are
    // AttrNumber (== int16).
    let indkey_i16: mcx::PgVec<'mcx, i16> = {
        let mut v: mcx::PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, row.indkey.len())?;
        for a in &row.indkey {
            v.push(*a as i16);
        }
        v
    };
    values[pi::Anum_pg_index_indkey as usize - 1] = buildint2vector(mcx, &indkey_i16)?;
    values[pi::Anum_pg_index_indcollation as usize - 1] = buildoidvector(mcx, &row.indcollation)?;
    values[pi::Anum_pg_index_indclass as usize - 1] = buildoidvector(mcx, &row.indclass)?;
    values[pi::Anum_pg_index_indoption as usize - 1] = buildint2vector(mcx, &row.indoption)?;

    // exprsDatum = ii_Expressions != NIL ? CStringGetTextDatum(nodeToString(...))
    // : (Datum) 0; nulls[indexprs] = (exprsDatum == 0).
    match &row.indexprs {
        Some(text) => {
            values[pi::Anum_pg_index_indexprs as usize - 1] = cstring_to_text_datum(mcx, text)?;
        }
        None => nulls[pi::Anum_pg_index_indexprs as usize - 1] = true,
    }
    // predDatum likewise.
    match &row.indpred {
        Some(text) => {
            values[pi::Anum_pg_index_indpred as usize - 1] = cstring_to_text_datum(mcx, text)?;
        }
        None => nulls[pi::Anum_pg_index_indpred as usize - 1] = true,
    }

    // tuple = heap_form_tuple(RelationGetDescr(pg_index), values, nulls);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    // CatalogTupleInsert(pg_index, tuple); heap_freetuple(tuple);
    CatalogTupleInsert(mcx, rel, &mut tup)
}

/* ======================================================================== *
 * pg_policy — CreatePolicy / AlterPolicy inserts & updates (commands/policy.c).
 * ======================================================================== */

/// `construct_array_builtin(role_oids, nitems, OIDOID)` — a 1-D `Oid[]` array
/// varlena (the `polroles` column). OID is pass-by-value.
fn build_oid_array<'mcx>(
    mcx: Mcx<'mcx>,
    oids: &[types_core::Oid],
) -> PgResult<Datum<'mcx>> {
    const OIDOID: types_core::Oid = 26;
    let mut elems: mcx::PgVec<'mcx, types_datum::datum::Datum> =
        mcx::vec_with_capacity_in(mcx, oids.len())?;
    for &o in oids {
        elems.push(types_datum::datum::Datum::from_oid(o));
    }
    // construct_array(.., OIDOID): elmlen=4, elmbyval=true, elmalign='i'.
    let buf = backend_utils_adt_arrayfuncs::construct::construct_array(
        mcx, &elems, OIDOID, 4, true, b'i',
    )?;
    Ok(Datum::ByRef(buf))
}

/// `DirectFunctionCall1(namein, CStringGetDatum(name))` — the `name` type is a
/// fixed-length 64-byte `NameData`, NUL-padded. `namein` truncates input longer
/// than `NAMEDATALEN - 1`.
fn namein_datum<'mcx>(mcx: Mcx<'mcx>, name: &str) -> PgResult<Datum<'mcx>> {
    const NAMEDATALEN: usize = 64;
    let bytes = name.as_bytes();
    let len = bytes.len().min(NAMEDATALEN - 1);
    let mut image = [0u8; NAMEDATALEN];
    image[..len].copy_from_slice(&bytes[..len]);
    Ok(Datum::ByRef(mcx::slice_in(mcx, &image[..])?))
}

/// `CreatePolicy`'s pg_policy INSERT (commands/policy.c): allocate the OID,
/// form the 8-column row, `CatalogTupleInsert`, return the OID.
fn catalog_tuple_insert_pg_policy<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_policy::PgPolicyInsertRow,
) -> PgResult<types_core::Oid> {
    use cat::pg_policy as pp;

    // policy_id = GetNewOidWithIndex(pg_policy_rel, PolicyOidIndexId,
    //                                Anum_pg_policy_oid);
    let policy_id = GetNewOidWithIndex(rel, pp::PolicyOidIndexId, pp::Anum_pg_policy_oid)?;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    for _ in 0..pp::Natts_pg_policy {
        values.push(Datum::null());
        isnull.push(false);
    }

    // values[Anum_pg_policy_oid - 1]          = ObjectIdGetDatum(policy_id);
    // values[Anum_pg_policy_polrelid - 1]     = ObjectIdGetDatum(table_id);
    // values[Anum_pg_policy_polname - 1]      = DirectFunctionCall1(namein, ...);
    // values[Anum_pg_policy_polcmd - 1]       = CharGetDatum(polcmd);
    // values[Anum_pg_policy_polpermissive - 1]= BoolGetDatum(stmt->permissive);
    // values[Anum_pg_policy_polroles - 1]     = PointerGetDatum(role_ids);
    values[pp::Anum_pg_policy_oid as usize - 1] = Datum::from_oid(policy_id);
    values[pp::Anum_pg_policy_polname as usize - 1] = namein_datum(mcx, &row.polname)?;
    values[pp::Anum_pg_policy_polrelid as usize - 1] = Datum::from_oid(row.polrelid);
    values[pp::Anum_pg_policy_polcmd as usize - 1] = Datum::from_char(row.polcmd);
    values[pp::Anum_pg_policy_polpermissive as usize - 1] = Datum::from_bool(row.polpermissive);
    values[pp::Anum_pg_policy_polroles as usize - 1] = build_oid_array(mcx, &row.polroles)?;

    // Add qual / WITH CHECK qual if present, else isnull.
    match &row.polqual {
        Some(s) => {
            values[pp::Anum_pg_policy_polqual as usize - 1] = cstring_to_text_datum(mcx, s)?;
        }
        None => isnull[pp::Anum_pg_policy_polqual as usize - 1] = true,
    }
    match &row.polwithcheck {
        Some(s) => {
            values[pp::Anum_pg_policy_polwithcheck as usize - 1] = cstring_to_text_datum(mcx, s)?;
        }
        None => isnull[pp::Anum_pg_policy_polwithcheck as usize - 1] = true,
    }

    // policy_tuple = heap_form_tuple(RelationGetDescr(pg_policy_rel), values, isnull);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &isnull)?;
    // CatalogTupleInsert(pg_policy_rel, policy_tuple);
    CatalogTupleInsert(mcx, rel, &mut tup)?;

    Ok(policy_id)
}

/// `construct_array_builtin(text_datums, n, TEXTOID)` — a 1-D `text[]` array
/// varlena (the `evttags` column), built directly from the UTF-8 strings.
fn build_text_array<'mcx>(mcx: Mcx<'mcx>, strs: &[std::string::String]) -> PgResult<Datum<'mcx>> {
    let refs: std::vec::Vec<&str> = strs.iter().map(|s| s.as_str()).collect();
    let buf = backend_utils_adt_arrayfuncs::construct::build_text_array(mcx, &refs)?;
    Ok(Datum::ByRef(buf))
}

/// `CreateEventTrigger`'s pg_event_trigger INSERT (commands/event_trigger.c
/// `insert_event_trigger_tuple`): allocate the OID, form the 7-column row,
/// `CatalogTupleInsert`, return the OID.
fn catalog_tuple_insert_pg_event_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_event_trigger::PgEventTriggerInsertRow,
) -> PgResult<types_core::Oid> {
    use cat::pg_event_trigger as pe;

    // trigoid = GetNewOidWithIndex(tgrel, EventTriggerOidIndexId,
    //                              Anum_pg_event_trigger_oid);
    let trigoid = GetNewOidWithIndex(rel, pe::EventTriggerOidIndexId, pe::Anum_pg_event_trigger_oid)?;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    let mut isnull: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    for _ in 0..pe::Natts_pg_event_trigger {
        values.push(Datum::null());
        isnull.push(false);
    }

    // values[Anum_pg_event_trigger_oid - 1]       = ObjectIdGetDatum(trigoid);
    // namestrcpy(&evtnamedata, trigname);   values[evtname - 1] = NameGetDatum(...);
    // namestrcpy(&evteventdata, eventname); values[evtevent - 1] = NameGetDatum(...);
    // values[evtowner - 1]   = ObjectIdGetDatum(evtOwner);
    // values[evtfoid - 1]    = ObjectIdGetDatum(funcoid);
    // values[evtenabled - 1] = CharGetDatum(TRIGGER_FIRES_ON_ORIGIN);
    values[pe::Anum_pg_event_trigger_oid as usize - 1] = Datum::from_oid(trigoid);
    values[pe::Anum_pg_event_trigger_evtname as usize - 1] = namein_datum(mcx, &row.evtname)?;
    values[pe::Anum_pg_event_trigger_evtevent as usize - 1] = namein_datum(mcx, &row.evtevent)?;
    values[pe::Anum_pg_event_trigger_evtowner as usize - 1] = Datum::from_oid(row.evtowner);
    values[pe::Anum_pg_event_trigger_evtfoid as usize - 1] = Datum::from_oid(row.evtfoid);
    values[pe::Anum_pg_event_trigger_evtenabled as usize - 1] = Datum::from_char(row.evtenabled);

    // if (taglist == NIL) nulls[evttags - 1] = true;
    // else values[evttags - 1] = filter_list_to_array(taglist);
    match &row.evttags {
        Some(tags) => {
            values[pe::Anum_pg_event_trigger_evttags as usize - 1] = build_text_array(mcx, tags)?;
        }
        None => isnull[pe::Anum_pg_event_trigger_evttags as usize - 1] = true,
    }

    // tuple = heap_form_tuple(tgrel->rd_att, values, nulls);
    // CatalogTupleInsert(tgrel, tuple);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &isnull)?;
    CatalogTupleInsert(mcx, rel, &mut tup)?;

    Ok(trigoid)
}

/// `AlterEventTrigger`'s pg_event_trigger UPDATE (commands/event_trigger.c):
/// `evtForm->evtenabled = tgenabled;` over the syscache-copied tuple, then
/// `CatalogTupleUpdate(tgrel, &tup->t_self, tup)`. Replaces only the
/// `evtenabled` column, preserving every other column of the held tuple.
fn catalog_tuple_update_pg_event_trigger_enabled<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    evt_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    tgenabled: i8,
) -> PgResult<()> {
    use cat::pg_event_trigger as pe;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    let mut isnull: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    for _ in 0..pe::Natts_pg_event_trigger {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    let i = pe::Anum_pg_event_trigger_evtenabled as usize - 1;
    replaces[i] = true;
    values[i] = Datum::from_char(tgenabled);

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, evt_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    crate::keystone::CatalogTupleUpdate(mcx, rel, evt_tuple.tuple.t_self, &mut new_tuple)
}

/// `AlterEventTriggerOwner_internal`'s pg_event_trigger UPDATE
/// (commands/event_trigger.c): set the row's `evtowner` to `new_owner_id` and
/// `CatalogTupleUpdate`. `rel` is the open pg_event_trigger relation; `evt_tuple`
/// is the writable held copy. `Err` carries the heap/index mutation failure.
fn catalog_tuple_update_pg_event_trigger_owner<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    evt_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    new_owner_id: types_core::Oid,
) -> PgResult<()> {
    use cat::pg_event_trigger as pe;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    let mut isnull: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pe::Natts_pg_event_trigger)?;
    for _ in 0..pe::Natts_pg_event_trigger {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    let i = pe::Anum_pg_event_trigger_evtowner as usize - 1;
    replaces[i] = true;
    values[i] = Datum::from_oid(new_owner_id);

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, evt_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    crate::keystone::CatalogTupleUpdate(mcx, rel, evt_tuple.tuple.t_self, &mut new_tuple)
}

/// `CStringGetByteaDatum` over a raw payload: a `bytea` varlena image (4-byte
/// header `SET_VARSIZE(len + VARHDRSZ)` then the verbatim bytes). C builds the
/// `tgargs` bytea as `arg1\0arg2\0...`; this wraps that payload unchanged.
fn bytea_datum<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum<'mcx>> {
    let total = 4 + payload.len();
    let word = (total as u32) << 2;
    let mut buf: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    buf[0..4].copy_from_slice(&word.to_ne_bytes());
    buf[4..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// `CreateTrigger`'s pg_trigger INSERT/UPDATE (commands/trigger.c): allocate the
/// OID (fresh INSERT) or reuse the existing one (OR REPLACE / internal update),
/// form the 19-column row, and `CatalogTupleInsert` / `CatalogTupleUpdate`.
fn catalog_tuple_insert_pg_trigger<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    row: &cat::pg_trigger::PgTriggerInsertRow,
) -> PgResult<types_core::Oid> {
    use cat::pg_trigger as pt;

    // trigoid = GetNewOidWithIndex(tgrel, TriggerOidIndexId, Anum_pg_trigger_oid)
    // for a fresh trigger; reuse the existing OID for OR REPLACE.
    let trigoid = match &row.existing {
        Some((oid, _)) => *oid,
        None => GetNewOidWithIndex(rel, pt::TriggerOidIndexId, pt::Anum_pg_trigger_oid)?,
    };

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pt::Natts_pg_trigger)?;
    let mut isnull: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pt::Natts_pg_trigger)?;
    for _ in 0..pt::Natts_pg_trigger {
        values.push(Datum::null());
        isnull.push(false);
    }

    values[pt::Anum_pg_trigger_oid as usize - 1] = Datum::from_oid(trigoid);
    values[pt::Anum_pg_trigger_tgrelid as usize - 1] = Datum::from_oid(row.tgrelid);
    values[pt::Anum_pg_trigger_tgparentid as usize - 1] = Datum::from_oid(row.tgparentid);
    values[pt::Anum_pg_trigger_tgname as usize - 1] = namein_datum(mcx, &row.tgname)?;
    values[pt::Anum_pg_trigger_tgfoid as usize - 1] = Datum::from_oid(row.tgfoid);
    values[pt::Anum_pg_trigger_tgtype as usize - 1] = Datum::from_i16(row.tgtype);
    values[pt::Anum_pg_trigger_tgenabled as usize - 1] = Datum::from_char(row.tgenabled);
    values[pt::Anum_pg_trigger_tgisinternal as usize - 1] = Datum::from_bool(row.tgisinternal);
    values[pt::Anum_pg_trigger_tgconstrrelid as usize - 1] = Datum::from_oid(row.tgconstrrelid);
    values[pt::Anum_pg_trigger_tgconstrindid as usize - 1] = Datum::from_oid(row.tgconstrindid);
    values[pt::Anum_pg_trigger_tgconstraint as usize - 1] = Datum::from_oid(row.tgconstraint);
    values[pt::Anum_pg_trigger_tgdeferrable as usize - 1] = Datum::from_bool(row.tgdeferrable);
    values[pt::Anum_pg_trigger_tginitdeferred as usize - 1] = Datum::from_bool(row.tginitdeferred);
    values[pt::Anum_pg_trigger_tgnargs as usize - 1] = Datum::from_i16(row.tgnargs);
    values[pt::Anum_pg_trigger_tgattr as usize - 1] = buildint2vector(mcx, &row.tgattr)?;
    values[pt::Anum_pg_trigger_tgargs as usize - 1] = bytea_datum(mcx, &row.tgargs)?;

    match &row.tgqual {
        Some(s) => values[pt::Anum_pg_trigger_tgqual as usize - 1] = cstring_to_text_datum(mcx, s)?,
        None => isnull[pt::Anum_pg_trigger_tgqual as usize - 1] = true,
    }
    match &row.tgoldtable {
        Some(s) => {
            values[pt::Anum_pg_trigger_tgoldtable as usize - 1] = namein_datum(mcx, s)?;
        }
        None => isnull[pt::Anum_pg_trigger_tgoldtable as usize - 1] = true,
    }
    match &row.tgnewtable {
        Some(s) => {
            values[pt::Anum_pg_trigger_tgnewtable as usize - 1] = namein_datum(mcx, s)?;
        }
        None => isnull[pt::Anum_pg_trigger_tgnewtable as usize - 1] = true,
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &isnull)?;

    match &row.existing {
        // CatalogTupleUpdate(tgrel, &tuple->t_self, newtup);
        Some((_, tid)) => crate::keystone::CatalogTupleUpdate(mcx, rel, *tid, &mut tup)?,
        // CatalogTupleInsert(tgrel, tuple);
        None => CatalogTupleInsert(mcx, rel, &mut tup)?,
    }

    Ok(trigoid)
}

/// `AlterPolicy` / `RemoveRoleFromObjectPolicy`'s pg_policy UPDATE
/// (commands/policy.c): `heap_modify_tuple` over the selectively-replaced
/// columns, then `CatalogTupleUpdate`.
fn catalog_tuple_update_pg_policy<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    policy_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    row: &cat::pg_policy::PgPolicyUpdateRow,
) -> PgResult<()> {
    use cat::pg_policy as pp;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    for _ in 0..pp::Natts_pg_policy {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    // replaces[Anum_pg_policy_polroles - 1] = true; values[..] = PointerGetDatum(role_ids);
    if let Some(role_oids) = &row.polroles {
        replaces[pp::Anum_pg_policy_polroles as usize - 1] = true;
        values[pp::Anum_pg_policy_polroles as usize - 1] = build_oid_array(mcx, role_oids)?;
    }

    // replaces[Anum_pg_policy_polqual - 1] = true; values[..] = CStringGetTextDatum(...) (or NULL).
    if let Some(qual) = &row.polqual {
        replaces[pp::Anum_pg_policy_polqual as usize - 1] = true;
        match qual {
            Some(s) => {
                values[pp::Anum_pg_policy_polqual as usize - 1] = cstring_to_text_datum(mcx, s)?
            }
            None => isnull[pp::Anum_pg_policy_polqual as usize - 1] = true,
        }
    }

    if let Some(wc) = &row.polwithcheck {
        replaces[pp::Anum_pg_policy_polwithcheck as usize - 1] = true;
        match wc {
            Some(s) => {
                values[pp::Anum_pg_policy_polwithcheck as usize - 1] =
                    cstring_to_text_datum(mcx, s)?
            }
            None => isnull[pp::Anum_pg_policy_polwithcheck as usize - 1] = true,
        }
    }

    // new_tuple = heap_modify_tuple(policy_tuple, RelationGetDescr(pg_policy_rel),
    //                               values, isnull, replaces);
    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, policy_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    // CatalogTupleUpdate(pg_policy_rel, &new_tuple->t_self, new_tuple);
    crate::keystone::CatalogTupleUpdate(mcx, rel, policy_tuple.tuple.t_self, &mut new_tuple)
}

/// `rename_policy`'s pg_policy rename (commands/policy.c): rewrite only the
/// `polname` `NameData` column of the scanned tuple, then `CatalogTupleUpdate`.
fn rename_policy_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    policy_tuple: &types_tuple::backend_access_common_heaptuple::FormedTuple<'mcx>,
    newname: &str,
) -> PgResult<()> {
    use cat::pg_policy as pp;

    let mut values: mcx::PgVec<'mcx, Datum<'mcx>> =
        mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    let mut isnull: mcx::PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    let mut replaces: mcx::PgVec<'mcx, bool> =
        mcx::vec_with_capacity_in(mcx, pp::Natts_pg_policy)?;
    for _ in 0..pp::Natts_pg_policy {
        values.push(Datum::null());
        isnull.push(false);
        replaces.push(false);
    }

    // namestrcpy(&GETSTRUCT(policy_tuple)->polname, stmt->newname);
    replaces[pp::Anum_pg_policy_polname as usize - 1] = true;
    values[pp::Anum_pg_policy_polname as usize - 1] = namein_datum(mcx, newname)?;

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut new_tuple = backend_access_common_heaptuple::heap_modify_tuple(
        mcx, policy_tuple, &tupdesc, &values, &isnull, &replaces,
    )?;
    crate::keystone::CatalogTupleUpdate(mcx, rel, policy_tuple.tuple.t_self, &mut new_tuple)
}

/// Install the F3 DDL-cluster catalog-write seams. Wired from
/// [`crate::init_seams`].
pub fn install() {
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_policy::set(
        catalog_tuple_insert_pg_policy,
    );
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_trigger::set(
        catalog_tuple_insert_pg_trigger,
    );
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_event_trigger::set(
        catalog_tuple_insert_pg_event_trigger,
    );
    backend_catalog_indexing_seams::catalog_tuple_update_pg_event_trigger_enabled::set(
        catalog_tuple_update_pg_event_trigger_enabled,
    );
    backend_catalog_indexing_seams::catalog_tuple_update_pg_event_trigger_owner::set(
        catalog_tuple_update_pg_event_trigger_owner,
    );
    backend_catalog_indexing_seams::catalog_tuple_update_pg_policy::set(
        catalog_tuple_update_pg_policy,
    );
    backend_catalog_indexing_seams::rename_policy_tuple::set(rename_policy_tuple);
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_class::set(
        catalog_tuple_insert_pg_class,
    );
    backend_catalog_indexing_seams::catalog_insert_pg_attribute_tuples::set(
        catalog_insert_pg_attribute_tuples,
    );
    backend_catalog_indexing_seams::append_attribute_tuples::set(append_attribute_tuples);
    backend_catalog_indexing_seams::catalog_tuple_update_pg_attribute::set(
        catalog_tuple_update_pg_attribute,
    );
    backend_catalog_indexing_seams::catalog_tuple_update_relchecks_pg_class::set(
        catalog_tuple_update_relchecks_pg_class,
    );
    backend_catalog_indexing_seams::catalog_tuple_update_relowner_pg_class::set(
        catalog_tuple_update_relowner_pg_class,
    );
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_attrdef::set(
        catalog_tuple_insert_pg_attrdef,
    );
    backend_catalog_indexing_seams::catalog_tuple_insert_pg_index::set(
        catalog_tuple_insert_pg_index,
    );
}
