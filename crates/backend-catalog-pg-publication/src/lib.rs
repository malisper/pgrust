//! `backend-catalog-pg-publication` — the per-catalog read + mutate owner over
//! the three publication catalogs (`pg_publication`, `pg_publication_rel`,
//! `pg_publication_namespace`); the faithful port of `catalog/pg_publication.c`.
//!
//! This unit is the keystone that unblocks `backend-commands-publicationcmds`:
//! it owns the marshaling between the on-disk publication tuples and the decoded
//! [`Publication`] / list carriers, the two catalog mutators
//! (`publication_add_relation` / `publication_add_schema` — real
//! `heap_form_tuple` + `CatalogTupleInsert` + dependency recording), and the
//! ~20 catalog-scan getters.
//!
//! It follows the established carrier model (the `pg_database` / `pg_type`
//! pattern): real `heap_form_tuple` over the relation's `TupleDesc`, real
//! `systable_beginscan` / `systable_getnext` / `heap_deform_tuple`, the catalog-
//! mutation engine consumed as `pub` functions from `backend-catalog-indexing`.
//! It does NOT route the substrate through one facade seam (the src-idiomatic
//! crate's model, deliberately not copied).
//!
//! Family layout (mirrors `pg_publication.c`):
//! * **F1** — the carrier mutators + their helpers (`publication_add_relation`,
//!   `publication_add_schema`, `check_publication_add_relation`,
//!   `check_publication_add_schema`, `pub_collist_validate`,
//!   `pub_collist_to_bitmapset`, `pub_form_cols_map`,
//!   `check_and_fetch_column_list`, `attnumstoint2vector`).
//! * **F2** — the catalog-scan getters (`GetPublication`(`ByName`),
//!   `GetRelationPublications`, `GetPublicationRelations`,
//!   `GetAllTablesPublications`(`Relations`), `GetPublicationSchemas`,
//!   `GetSchemaPublications`(`Relations`), `GetAllSchemaPublicationRelations`,
//!   `GetPubPartitionOptionRelations`, `GetTopMostAncestorInPublication`,
//!   `is_publishable_relation`, `is_schema_publication`, `filter_partitions`).
//! * **F3** — the SRF row builder (`build_publication_table_rows`, the portable
//!   body of `pg_get_publication_tables`). The per-call `FuncCallContext` SRF
//!   SQL wrapper is NOT installed here — that protocol is unported (panics); a
//!   future SRF owner adapts these rows.

#![allow(non_snake_case)]

use mcx::{Mcx, PgBox, PgString, PgVec};

use types_catalog::catalog_dependency::{
    DEPENDENCY_AUTO, DEPENDENCY_NORMAL, InvalidObjectAddress, ObjectAddress,
};
use types_catalog::pg_publication::{
    Anum_pg_publication_namespace_oid, Anum_pg_publication_namespace_pnnspid,
    Anum_pg_publication_namespace_pnpubid, Anum_pg_publication_oid,
    Anum_pg_publication_puballtables, Anum_pg_publication_pubdelete,
    Anum_pg_publication_pubgencols, Anum_pg_publication_pubinsert, Anum_pg_publication_pubname,
    Anum_pg_publication_pubtruncate, Anum_pg_publication_pubupdate, Anum_pg_publication_pubviaroot,
    Anum_pg_publication_rel_oid, Anum_pg_publication_rel_prattrs, Anum_pg_publication_rel_prpubid,
    Anum_pg_publication_rel_prqual, Anum_pg_publication_rel_prrelid, Natts_pg_publication_namespace,
    Natts_pg_publication_rel, Publication, PublicationActions, PublicationPartOpt,
    PublicationRelObjectIndexId, PublicationNamespaceObjectIndexId,
    PublicationNamespacePnnspidPnpubidIndexId, PublicationNamespaceRelationId,
    PublicationRelPrpubidIndexId, PublicationRelRelationId, PublicationRelationId,
    PublicationTableRow, PgClassRow, PublishGencolsType, PublishedRel,
};

use types_core::catalog::FirstNormalObjectId;
use types_core::fmgr::{F_BOOLEQ, F_CHAREQ, F_OIDEQ};
use types_core::primitive::{AttrNumber, Oid};
use types_error::{
    ERROR, PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_COLUMN,
};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::nodes::Node;
use types_rel::Relation;
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock};
use types_tuple::access::{
    ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION,
};
use types_tuple::backend_access_common_heaptuple::{Datum, FormedTuple};

use backend_access_common_heaptuple::heap_form_tuple;
use backend_utils_error::ereport;
use backend_access_common_scankey::ScanKeyInit;
use backend_catalog_catalog::{GetNewOidWithIndex, IsCatalogNamespace, IsCatalogRelation,
    IsCatalogRelationOid, IsToastNamespace};
use backend_catalog_dependency::recordDependencyOnSingleRelExpr;
use backend_catalog_indexing::keystone::CatalogTupleInsert;

use backend_access_index_genam_seams as genam;
use backend_access_table_table_seams as table_seams;
use backend_catalog_namespace_seams as namespace_seams;
use backend_catalog_partition_seams as partition_seams;
use backend_catalog_pg_depend_seams as pg_depend_seams;
use backend_catalog_pg_inherits_seams as inherits_seams;
use backend_nodes_core_seams as bms;
use backend_nodes_outfuncs::nodeToString;
use backend_utils_cache_inval::cache_invalidate::CacheInvalidateRelcacheByRelid;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache::{
    SearchSysCache1, SearchSysCache2, SearchSysCacheExists, SearchSysCacheList1, SysCacheGetAttr,
};
use types_cache::syscache::SysCacheKey;
use types_datum::Datum as KeyDatum;
use types_syscache::syscache_ids::{
    PUBLICATIONNAMESPACEMAP, PUBLICATIONOID, PUBLICATIONRELMAP,
};

/* ==========================================================================
 * Small varlena / scan-key helpers.
 * ========================================================================== */

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`.
fn oid_key<'mcx>(attno: i32, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno as AttrNumber,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_BOOLEQ,
/// BoolGetDatum(value))`.
fn bool_key<'mcx>(attno: i32, value: bool) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno as AttrNumber,
        BTEqualStrategyNumber,
        F_BOOLEQ,
        Datum::from_bool(value),
    )?;
    Ok(key)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_CHAREQ,
/// CharGetDatum(value))`.
fn char_key<'mcx>(attno: i32, value: u8) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno as AttrNumber,
        BTEqualStrategyNumber,
        F_CHAREQ,
        Datum::from_char(value as i8),
    )?;
    Ok(key)
}

/// `CStringGetTextDatum(s)` (`postgres.h` → `cstring_to_text`): a `text` varlena
/// with the standard 4-byte header followed by the payload — the verbatim
/// `Datum::ByRef` bytes (header included), as `heap_form_tuple` reads via
/// `VARSIZE_ANY`.
fn cstring_to_text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    const VARHDRSZ: usize = 4;
    let payload = s.as_bytes();
    let total = VARHDRSZ + payload.len();
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    buf.resize(total, 0u8);
    let vl_len: u32 = (total as u32) << 2;
    buf[0..4].copy_from_slice(&vl_len.to_ne_bytes());
    buf[VARHDRSZ..].copy_from_slice(payload);
    Ok(Datum::ByRef(buf))
}

/// `buildint2vector(int2s, n)` (`utils/adt/int.c`): the on-disk `int2vector`
/// varlena image — `vl_len_`, `ndim=1`, `dataoffset=0`, `elemtype=INT2OID`,
/// `dim1=n`, `lbound1=0`, then the `n` `int16` values out of line. Returned as
/// the verbatim `Datum::ByRef` bytes (header included).
fn buildint2vector_bytes<'mcx>(mcx: Mcx<'mcx>, int2s: &[i16]) -> PgResult<PgVec<'mcx, u8>> {
    const INT2OID: Oid = 21;
    const HEADER: usize = 24; // vl_len_ + ndim + dataoffset + elemtype + dim1 + lbound1
    let n = int2s.len();
    let total = HEADER + n * core::mem::size_of::<i16>();
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, total)?;
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
    Ok(buf)
}

/// `int16` element values of an `int2vector`, returned as a plain `Vec` (no
/// allocator needed — these are tiny and immediately folded into a bitmapset).
fn int2vector_elems_vec(bytes: &[u8]) -> PgResult<std::vec::Vec<i16>> {
    const HEADER: usize = 24;
    if bytes.len() < HEADER {
        return Err(PgError::error("int2vector image too short"));
    }
    let nelems = i32::from_ne_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    if nelems < 0 {
        return Err(PgError::error("int2vector has negative dim1"));
    }
    let nelems = nelems as usize;
    let need = HEADER + nelems * 2;
    if bytes.len() < need {
        return Err(PgError::error("int2vector image shorter than dim1 implies"));
    }
    let mut out = std::vec::Vec::with_capacity(nelems);
    for i in 0..nelems {
        let off = HEADER + i * 2;
        out.push(i16::from_ne_bytes([bytes[off], bytes[off + 1]]));
    }
    Ok(out)
}

/// Read a `NameData` (NUL-padded) by-reference image as a `&str` (up to the
/// first NUL). `NameStr(pubform->pubname)`.
fn name_str(bytes: &[u8]) -> &str {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// `namein(s)` — a `NAMEDATALEN`-byte NUL-padded `NameData` by-reference Datum.
fn name_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    use types_core::fmgr::NAMEDATALEN;
    let mut image: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, NAMEDATALEN as usize)?;
    let src = s.as_bytes();
    let take = core::cmp::min(src.len(), (NAMEDATALEN as usize) - 1);
    for &b in &src[..take] {
        image.push(b);
    }
    while image.len() < NAMEDATALEN as usize {
        image.push(0);
    }
    Ok(Datum::ByRef(image))
}

/// `ObjectIdGetDatum` as a syscache key.
fn oid_cache_key(value: Oid) -> SysCacheKey<'static> {
    SysCacheKey::Value(KeyDatum::from_oid(value))
}

/* ==========================================================================
 * F1 helpers: checks + column-list validation.
 * ========================================================================== */

/// `check_publication_add_relation(targetrel)` (pg_publication.c:55).
fn check_publication_add_relation(targetrel: &Relation<'_>) -> PgResult<()> {
    // Must be a regular or partitioned table.
    let relkind = targetrel.rd_rel.relkind;
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        let detail =
            backend_catalog_pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add relation \"{}\" to publication",
                targetrel.name()
            ))
            .errdetail(detail)
            .into_error());
    }

    // Can't be a system table.
    if IsCatalogRelation(targetrel) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add relation \"{}\" to publication",
                targetrel.name()
            ))
            .errdetail("This operation is not supported for system tables.".to_string())
            .into_error());
    }

    // UNLOGGED and TEMP relations cannot be part of a publication.
    let persist = targetrel.rd_rel.relpersistence;
    if persist == types_tuple::access::RELPERSISTENCE_TEMP {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add relation \"{}\" to publication",
                targetrel.name()
            ))
            .errdetail("This operation is not supported for temporary tables.".to_string())
            .into_error());
    } else if persist == types_tuple::access::RELPERSISTENCE_UNLOGGED {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add relation \"{}\" to publication",
                targetrel.name()
            ))
            .errdetail("This operation is not supported for unlogged tables.".to_string())
            .into_error());
    }

    Ok(())
}

/// `check_publication_add_schema(schemaid)` (pg_publication.c:94).
fn check_publication_add_schema(mcx: Mcx<'_>, schemaid: Oid) -> PgResult<()> {
    // Can't be a system namespace.
    if IsCatalogNamespace(schemaid) || IsToastNamespace(schemaid) {
        let nm = lsyscache::get_namespace_name::call(mcx, schemaid)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add schema \"{}\" to publication",
                nm.as_deref().unwrap_or("")
            ))
            .errdetail("This operation is not supported for system schemas.".to_string())
            .into_error());
    }

    // Can't be a temporary namespace.
    if namespace_seams::is_any_temp_namespace::call(mcx, schemaid)? {
        let nm = lsyscache::get_namespace_name::call(mcx, schemaid)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "cannot add schema \"{}\" to publication",
                nm.as_deref().unwrap_or("")
            ))
            .errdetail("Temporary schemas cannot be replicated.".to_string())
            .into_error());
    }

    Ok(())
}

/// `is_publishable_class(relid, reltuple)` (pg_publication.c:133).
fn is_publishable_class(relid: Oid, row: &PgClassRow) -> bool {
    (row.relkind == RELKIND_RELATION || row.relkind == RELKIND_PARTITIONED_TABLE)
        && !IsCatalogRelationOid(relid)
        && row.relpersistence == types_core::catalog::RELPERSISTENCE_PERMANENT
        && relid >= FirstNormalObjectId
}

/// `is_publishable_relation(rel)` (pg_publication.c:146).
fn is_publishable_relation(rel: &Relation<'_>) -> PgResult<bool> {
    let row = PgClassRow {
        oid: rel.rd_id,
        relkind: rel.rd_rel.relkind,
        relpersistence: rel.rd_rel.relpersistence,
        relispartition: rel.rd_rel.relispartition,
        relnamespace: rel.rd_rel.relnamespace,
    };
    Ok(is_publishable_class(rel.rd_id, &row))
}

/// `pub_collist_validate(targetrel, columns)` (pg_publication.c:555).
fn pub_collist_validate<'mcx>(
    mcx: Mcx<'mcx>,
    targetrel: &Relation<'mcx>,
    columns: &[PgBox<'mcx, Node<'mcx>>],
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut set: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let tupdesc = &targetrel.rd_att;

    for col in columns {
        // colname = strVal(lfirst(lc));
        let colname = match &**col {
            Node::String(s) => s.sval.as_str(),
            _ => {
                return Err(PgError::error(
                    "pub_collist_validate: column list entry is not a String node",
                ));
            }
        };

        // attnum = get_attnum(RelationGetRelid(targetrel), colname);
        let attnum = lsyscache::get_attnum::call(targetrel.rd_id, colname)?;

        if attnum == 0 {
            // InvalidAttrNumber
            return Err(ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_COLUMN)
                .errmsg(format!(
                    "column \"{}\" of relation \"{}\" does not exist",
                    colname,
                    targetrel.name()
                ))
                .into_error());
        }

        // !AttrNumberIsForUserDefinedAttr(attnum)  =>  attnum <= 0
        if attnum <= 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "cannot use system column \"{}\" in publication column list",
                    colname
                ))
                .into_error());
        }

        // Virtual generated columns are disallowed.
        let att = tupdesc.attr(attnum as usize - 1);
        if att.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_COLUMN_REFERENCE)
                .errmsg(format!(
                    "cannot use virtual generated column \"{}\" in publication column list",
                    colname
                ))
                .into_error());
        }

        // Duplicate column?
        if bms::bms_is_member::call(attnum as i32, set.as_deref()) {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!(
                    "duplicate column \"{}\" in publication column list",
                    colname
                ))
                .into_error());
        }

        set = Some(bms::bms_add_member::call(mcx, set, attnum as i32)?);
    }

    Ok(set)
}

/// `pub_collist_to_bitmapset(columns, pubcols, mcxt)` (pg_publication.c:604).
fn pub_collist_to_bitmapset<'mcx>(
    mcx: Mcx<'mcx>,
    columns: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    pubcols: &[u8],
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result = columns;
    let elems = int2vector_elems_vec(pubcols)?;
    for e in elems {
        result = Some(bms::bms_add_member::call(mcx, result, e as i32)?);
    }
    Ok(result)
}

/// `pub_form_cols_map(relation, include_gencols_type)` (pg_publication.c:636).
fn pub_form_cols_map<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &Relation<'mcx>,
    include_gencols_type: PublishGencolsType,
) -> PgResult<Option<PgBox<'mcx, Bitmapset<'mcx>>>> {
    let mut result: Option<PgBox<'mcx, Bitmapset<'mcx>>> = None;
    let desc = &relation.rd_att;

    for i in 0..(desc.natts as usize) {
        let att = desc.attr(i);
        if att.attisdropped {
            continue;
        }
        if att.attgenerated != 0 {
            // Only STORED generated columns can be replicated.
            if att.attgenerated != ATTRIBUTE_GENERATED_STORED {
                continue;
            }
            if include_gencols_type != PublishGencolsType::Stored {
                continue;
            }
        }
        result = Some(bms::bms_add_member::call(mcx, result, att.attnum as i32)?);
    }
    Ok(result)
}

/// `attnumstoint2vector(attrs)` (pg_publication.c:404): the 0-based attnums of
/// the bitmapset, in ascending order, as an `int2vector` varlena `Datum`.
fn attnumstoint2vector<'mcx>(
    mcx: Mcx<'mcx>,
    attrs: Option<&Bitmapset<'mcx>>,
) -> PgResult<Datum<'mcx>> {
    let mut vals: std::vec::Vec<i16> = std::vec::Vec::new();
    let mut i = -1;
    loop {
        i = bms::bms_next_member::call(attrs, i);
        if i < 0 {
            break;
        }
        vals.push(i as i16);
    }
    Ok(Datum::ByRef(buildint2vector_bytes(mcx, &vals)?))
}

/// `check_and_fetch_column_list(pub, relid, mcxt, &cols)` (pg_publication.c:267).
fn check_and_fetch_column_list<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pub_alltables: bool,
    relid: Oid,
    prior: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
) -> PgResult<(bool, Option<PgBox<'mcx, Bitmapset<'mcx>>>)> {
    if pub_alltables {
        return Ok((false, prior));
    }

    let cftuple = SearchSysCache2(
        mcx,
        PUBLICATIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(pubid),
    )?;

    if let Some(tup) = cftuple {
        let (cfdatum, isnull) =
            SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &tup, Anum_pg_publication_rel_prattrs)?;
        if !isnull {
            // *cols = pub_collist_to_bitmapset(*cols, cfdatum, mcxt);
            let bytes: &[u8] = cfdatum.as_ref_bytes();
            let cols = pub_collist_to_bitmapset(mcx, prior, bytes)?;
            return Ok((true, cols));
        }
        return Ok((false, prior));
    }

    Ok((false, prior))
}

/* ==========================================================================
 * F2: decode one `pg_publication` tuple into a Publication.
 * ========================================================================== */

/// `GetPublication(pubid)` (pg_publication.c:1069).
fn GetPublication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<Publication<'mcx>> {
    let tup = SearchSysCache1(mcx, PUBLICATIONOID, oid_cache_key(pubid))?;
    let Some(tup) = tup else {
        return Err(PgError::error(format!(
            "cache lookup failed for publication {pubid}"
        )));
    };

    // Read the fixed-width columns via SysCacheGetAttr (GETSTRUCT analog).
    let getbool = |attno: i32| -> PgResult<bool> {
        let (v, _isnull) = SysCacheGetAttr(mcx, PUBLICATIONOID, &tup, attno)?;
        Ok(v.as_bool())
    };
    let pubname = {
        let (v, _isnull) = SysCacheGetAttr(mcx, PUBLICATIONOID, &tup, Anum_pg_publication_pubname)?;
        PgString::from_str_in(name_str(v.as_ref_bytes()), mcx)?
    };
    let alltables = getbool(Anum_pg_publication_puballtables)?;
    let pubinsert = getbool(Anum_pg_publication_pubinsert)?;
    let pubupdate = getbool(Anum_pg_publication_pubupdate)?;
    let pubdelete = getbool(Anum_pg_publication_pubdelete)?;
    let pubtruncate = getbool(Anum_pg_publication_pubtruncate)?;
    let pubviaroot = getbool(Anum_pg_publication_pubviaroot)?;
    let pubgencols = {
        let (v, _isnull) = SysCacheGetAttr(mcx, PUBLICATIONOID, &tup, Anum_pg_publication_pubgencols)?;
        PublishGencolsType::from_char(v.as_char())
    };

    Ok(Publication {
        oid: pubid,
        name: pubname,
        alltables,
        pubviaroot,
        pubgencols_type: pubgencols,
        pubactions: PublicationActions {
            pubinsert,
            pubupdate,
            pubdelete,
            pubtruncate,
        },
    })
}

/// `GetPublicationByName(pubname, missing_ok)` (pg_publication.c:1101).
fn GetPublicationByName<'mcx>(
    mcx: Mcx<'mcx>,
    pubname: &str,
    missing_ok: bool,
) -> PgResult<Option<Publication<'mcx>>> {
    // oid = get_publication_oid(pubname, missing_ok);
    let oid = lsyscache::get_publication_oid::call(pubname, missing_ok)?;
    if oid == 0 {
        return Ok(None);
    }
    Ok(Some(GetPublication(mcx, oid)?))
}

/* ==========================================================================
 * F2: list getters via systable scans.
 * ========================================================================== */

/// `GetRelationPublications(relid)` (pg_publication.c:750).
fn GetRelationPublications<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let members = SearchSysCacheList1(mcx, PUBLICATIONRELMAP, oid_cache_key(relid))?;
    for tup in members.iter() {
        let (v, _isnull) =
            SysCacheGetAttr(mcx, PUBLICATIONRELMAP, tup, Anum_pg_publication_rel_prpubid)?;
        result.push(v.as_oid());
    }
    Ok(result)
}

/// `GetSchemaPublications(schemaid)` (pg_publication.c:962).
fn GetSchemaPublications<'mcx>(mcx: Mcx<'mcx>, schemaid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let members = SearchSysCacheList1(mcx, PUBLICATIONNAMESPACEMAP, oid_cache_key(schemaid))?;
    for tup in members.iter() {
        let (v, _isnull) = SysCacheGetAttr(
            mcx,
            PUBLICATIONNAMESPACEMAP,
            tup,
            Anum_pg_publication_namespace_pnpubid,
        )?;
        result.push(v.as_oid());
    }
    Ok(result)
}

/// `GetPubPartitionOptionRelations(result, pub_partopt, relid)`
/// (pg_publication.c:309).
fn GetPubPartitionOptionRelations<'mcx>(
    mcx: Mcx<'mcx>,
    mut result: PgVec<'mcx, Oid>,
    pub_partopt: PublicationPartOpt,
    relid: Oid,
) -> PgResult<PgVec<'mcx, Oid>> {
    if lsyscache::get_rel_relkind::call(relid)? == RELKIND_PARTITIONED_TABLE
        && pub_partopt != PublicationPartOpt::Root
    {
        let all_parts = inherits_seams::find_all_inheritors::call(mcx, relid, NoLock)?;
        match pub_partopt {
            PublicationPartOpt::All => {
                for p in all_parts.iter() {
                    result.push(*p);
                }
            }
            PublicationPartOpt::Leaf => {
                for p in all_parts.iter() {
                    if lsyscache::get_rel_relkind::call(*p)? != RELKIND_PARTITIONED_TABLE {
                        result.push(*p);
                    }
                }
            }
            PublicationPartOpt::Root => unreachable!("guarded above"),
        }
    } else {
        result.push(relid);
    }
    Ok(result)
}

/// `GetPublicationRelations(pubid, pub_partopt)` (pg_publication.c:779).
fn GetPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    let pubrelsrel = table_seams::table_open::call(mcx, PublicationRelRelationId, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_publication_rel_prpubid, pubid)?];

    let mut scan =
        genam::systable_beginscan::call(&pubrelsrel, PublicationRelPrpubidIndexId, true, None, &keys)?;

    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    loop {
        let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
            break;
        };
        let cols = deform(mcx, &pubrelsrel, &tup)?;
        let prrelid = cols[(Anum_pg_publication_rel_prrelid - 1) as usize].0.as_oid();
        result = GetPubPartitionOptionRelations(mcx, result, pub_partopt, prrelid)?;
    }
    scan.end()?;
    pubrelsrel.close(AccessShareLock)?;

    // list_sort(result, list_oid_cmp); list_deduplicate_oid(result);
    sort_dedup_oids(&mut result);
    Ok(result)
}

/// `GetAllTablesPublications()` (pg_publication.c:822).
fn GetAllTablesPublications<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, Oid>> {
    let rel = table_seams::table_open::call(mcx, PublicationRelationId, AccessShareLock)?;
    let keys = [bool_key(Anum_pg_publication_puballtables, true)?];

    // systable_beginscan(rel, InvalidOid, false, NULL, 1, &scankey) — a
    // sequential catalog scan (index_ok=false), the table_beginscan_catalog
    // analog.
    let mut scan = genam::systable_beginscan::call(&rel, 0, false, None, &keys)?;

    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    loop {
        let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
            break;
        };
        let cols = deform(mcx, &rel, &tup)?;
        result.push(cols[(Anum_pg_publication_oid - 1) as usize].0.as_oid());
    }
    scan.end()?;
    rel.close(AccessShareLock)?;
    Ok(result)
}

/// `GetPublicationSchemas(pubid)` (pg_publication.c:924).
fn GetPublicationSchemas<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let pubschsrel =
        table_seams::table_open::call(mcx, PublicationNamespaceRelationId, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_publication_namespace_pnpubid, pubid)?];

    let mut scan = genam::systable_beginscan::call(
        &pubschsrel,
        PublicationNamespacePnnspidPnpubidIndexId,
        true,
        None,
        &keys,
    )?;

    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    loop {
        let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
            break;
        };
        let cols = deform(mcx, &pubschsrel, &tup)?;
        result.push(cols[(Anum_pg_publication_namespace_pnnspid - 1) as usize].0.as_oid());
    }
    scan.end()?;
    pubschsrel.close(AccessShareLock)?;
    Ok(result)
}

/// `is_schema_publication(pubid)` (pg_publication.c:232).
fn is_schema_publication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<bool> {
    let pubschsrel =
        table_seams::table_open::call(mcx, PublicationNamespaceRelationId, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_publication_namespace_pnpubid, pubid)?];

    let mut scan = genam::systable_beginscan::call(
        &pubschsrel,
        PublicationNamespacePnnspidPnpubidIndexId,
        true,
        None,
        &keys,
    )?;
    let result = genam::systable_getnext::call(mcx, scan.desc_mut())?.is_some();
    scan.end()?;
    pubschsrel.close(AccessShareLock)?;
    Ok(result)
}

/// `GetAllTablesPublicationRelations(pubviaroot)` (pg_publication.c:863).
fn GetAllTablesPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubviaroot: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let class_rel = table_seams::table_open::call(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;

    // Scan pg_class for ordinary tables.
    {
        let keys = [char_key(Anum_pg_class_relkind, RELKIND_RELATION)?];
        let mut scan = genam::systable_beginscan::call(&class_rel, 0, false, None, &keys)?;
        loop {
            let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
                break;
            };
            let row = pg_class_row(mcx, &class_rel, &tup)?;
            if is_publishable_class(row.oid, &row) && !(row.relispartition && pubviaroot) {
                result.push(row.oid);
            }
        }
        scan.end()?;
    }

    if pubviaroot {
        let keys = [char_key(Anum_pg_class_relkind, RELKIND_PARTITIONED_TABLE)?];
        let mut scan = genam::systable_beginscan::call(&class_rel, 0, false, None, &keys)?;
        loop {
            let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
                break;
            };
            let row = pg_class_row(mcx, &class_rel, &tup)?;
            if is_publishable_class(row.oid, &row) && !row.relispartition {
                result.push(row.oid);
            }
        }
        scan.end()?;
    }

    class_rel.close(AccessShareLock)?;
    Ok(result)
}

/// `GetSchemaPublicationRelations(schemaid, pub_partopt)`
/// (pg_publication.c:988).
fn GetSchemaPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    schemaid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    debug_assert!(schemaid != 0);
    let class_rel = table_seams::table_open::call(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let keys = [oid_key(Anum_pg_class_relnamespace, schemaid)?];
    let mut scan = genam::systable_beginscan::call(&class_rel, 0, false, None, &keys)?;

    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    loop {
        let Some(tup) = genam::systable_getnext::call(mcx, scan.desc_mut())? else {
            break;
        };
        let row = pg_class_row(mcx, &class_rel, &tup)?;
        if !is_publishable_class(row.oid, &row) {
            continue;
        }
        let relkind = lsyscache::get_rel_relkind::call(row.oid)?;
        if relkind == RELKIND_RELATION {
            result.push(row.oid);
        } else if relkind == RELKIND_PARTITIONED_TABLE {
            let partitionrels: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
            let partitionrels =
                GetPubPartitionOptionRelations(mcx, partitionrels, pub_partopt, row.oid)?;
            // list_concat_unique_oid(result, partitionrels)
            for p in partitionrels.iter() {
                if !result.iter().any(|x| x == p) {
                    result.push(*p);
                }
            }
        }
    }
    scan.end()?;
    class_rel.close(AccessShareLock)?;
    Ok(result)
}

/// `GetAllSchemaPublicationRelations(pubid, pub_partopt)`
/// (pg_publication.c:1045).
fn GetAllSchemaPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let pubschemalist = GetPublicationSchemas(mcx, pubid)?;
    for schemaid in pubschemalist.iter() {
        let schema_rels = GetSchemaPublicationRelations(mcx, *schemaid, pub_partopt)?;
        for r in schema_rels.iter() {
            result.push(*r);
        }
    }
    Ok(result)
}

/// `GetTopMostAncestorInPublication(puboid, ancestors, &ancestor_level)`
/// (pg_publication.c:353).
fn GetTopMostAncestorInPublication<'mcx>(
    mcx: Mcx<'mcx>,
    puboid: Oid,
    ancestors: &[Oid],
) -> PgResult<(Oid, i32)> {
    let mut topmost_relid: Oid = 0; // InvalidOid
    let mut level = 0;
    let mut ancestor_level = 0;

    for &ancestor in ancestors {
        level += 1;
        let apubids = GetRelationPublications(mcx, ancestor)?;
        if apubids.iter().any(|x| *x == puboid) {
            topmost_relid = ancestor;
            ancestor_level = level;
        } else {
            let nsp = lsyscache::get_rel_namespace::call(ancestor)?;
            let aschema_pubids = GetSchemaPublications(mcx, nsp)?;
            if aschema_pubids.iter().any(|x| *x == puboid) {
                topmost_relid = ancestor;
                ancestor_level = level;
            }
        }
    }

    Ok((topmost_relid, ancestor_level))
}

/// `filter_partitions(table_infos)` (pg_publication.c:197): drop partitions
/// whose parents are also in the list. Operates on the `PublishedRel` vector,
/// returning the filtered vector.
fn filter_partitions<'mcx>(
    mcx: Mcx<'mcx>,
    table_infos: PgVec<'mcx, PublishedRel>,
) -> PgResult<PgVec<'mcx, PublishedRel>> {
    // Build the set of all relids present, for is_ancestor_member_tableinfos.
    let present: std::vec::Vec<Oid> = table_infos.iter().map(|ti| ti.relid).collect();

    let mut out: PgVec<'mcx, PublishedRel> = mcx::vec_with_capacity_in(mcx, 0)?;
    for ti in table_infos.iter() {
        let mut skip = false;
        if lsyscache::get_rel_relispartition::call(ti.relid)? {
            let ancestors = partition_seams::get_partition_ancestors::call(mcx, ti.relid)?;
            for anc in ancestors.iter() {
                if present.iter().any(|r| r == anc) {
                    skip = true;
                    break;
                }
            }
        }
        if !skip {
            out.push(*ti);
        }
    }
    Ok(out)
}

/* ==========================================================================
 * F1: the mutators.
 * ========================================================================== */

/// `InvalidatePublicationRels(relids)` (publicationcmds.c helper, declared in
/// pg_publication.c's consumers): invalidate each relation's relcache entry so
/// the publication info is rebuilt. The C calls `CacheInvalidateRelcacheByRelid`
/// per relid (the function lives in publicationcmds.c, but pg_publication.c is
/// its only caller via this path; we mirror it inline).
fn InvalidatePublicationRels(relids: &[Oid]) -> PgResult<()> {
    for &relid in relids {
        CacheInvalidateRelcacheByRelid(relid)?;
    }
    Ok(())
}

/// `publication_add_relation(pubid, pri, if_not_exists)` (pg_publication.c:427).
fn publication_add_relation<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    targetrel: &Relation<'mcx>,
    where_clause: Option<&Node<'mcx>>,
    columns: Option<&[PgBox<'mcx, Node<'mcx>>]>,
    if_not_exists: bool,
) -> PgResult<ObjectAddress> {
    let relid = targetrel.rd_id;
    let publication = GetPublication(mcx, pubid)?;

    let rel = table_seams::table_open::call(mcx, PublicationRelRelationId, RowExclusiveLock)?;

    // Check for duplicates (nicer error; the unique index is the real guard).
    if SearchSysCacheExists(
        mcx,
        PUBLICATIONRELMAP,
        oid_cache_key(relid),
        oid_cache_key(pubid),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )? {
        rel.close(RowExclusiveLock)?;
        if if_not_exists {
            return Ok(InvalidObjectAddress);
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "relation \"{}\" is already member of publication \"{}\"",
                targetrel.name(),
                publication.name.as_str()
            ))
            .into_error());
    }

    check_publication_add_relation(targetrel)?;

    // Validate + translate the column names into a Bitmapset of attnums.
    let attnums = match columns {
        Some(cols) => pub_collist_validate(mcx, targetrel, cols)?,
        None => None,
    };

    // Form a tuple.
    let mut values: [Datum<'mcx>; Natts_pg_publication_rel] =
        core::array::from_fn(|_| Datum::null());
    let mut nulls = [false; Natts_pg_publication_rel];
    let idx = |attno: i32| (attno - 1) as usize;

    let pubreloid =
        GetNewOidWithIndex(&rel, PublicationRelObjectIndexId, Anum_pg_publication_rel_oid as AttrNumber)?;
    values[idx(Anum_pg_publication_rel_oid)] = Datum::from_oid(pubreloid);
    values[idx(Anum_pg_publication_rel_prpubid)] = Datum::from_oid(pubid);
    values[idx(Anum_pg_publication_rel_prrelid)] = Datum::from_oid(relid);

    // Add qualifications, if available.
    if let Some(wc) = where_clause {
        let s = nodeToString(mcx, wc)?;
        values[idx(Anum_pg_publication_rel_prqual)] = cstring_to_text_datum(mcx, s.as_str())?;
    } else {
        nulls[idx(Anum_pg_publication_rel_prqual)] = true;
    }

    // Add column list, if available.
    if columns.is_some() {
        values[idx(Anum_pg_publication_rel_prattrs)] =
            attnumstoint2vector(mcx, attnums.as_deref())?;
    } else {
        nulls[idx(Anum_pg_publication_rel_prattrs)] = true;
    }

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    // CatalogTupleInsert(rel, tup);
    CatalogTupleInsert(mcx, &rel, &mut tup)?;

    // Register dependencies.
    let myself = ObjectAddress {
        classId: PublicationRelRelationId,
        objectId: pubreloid,
        objectSubId: 0,
    };

    // Dependency on the publication.
    let referenced = ObjectAddress {
        classId: PublicationRelationId,
        objectId: pubid,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_AUTO)?;

    // Dependency on the relation.
    let referenced = ObjectAddress {
        classId: RELATION_RELATION_ID,
        objectId: relid,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_AUTO)?;

    // Dependency on the objects in the qualifications.
    if let Some(wc) = where_clause {
        recordDependencyOnSingleRelExpr(
            &myself,
            wc,
            relid,
            DEPENDENCY_NORMAL,
            DEPENDENCY_NORMAL,
            false,
        )?;
    }

    // Dependency on the listed columns.
    let mut i = -1;
    loop {
        i = bms::bms_next_member::call(attnums.as_deref(), i);
        if i < 0 {
            break;
        }
        let referenced = ObjectAddress {
            classId: RELATION_RELATION_ID,
            objectId: relid,
            objectSubId: i,
        };
        pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_NORMAL)?;
    }

    rel.close(RowExclusiveLock)?;

    // Invalidate relcache for the whole partition hierarchy.
    let relids: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
    let relids = GetPubPartitionOptionRelations(mcx, relids, PublicationPartOpt::All, relid)?;
    InvalidatePublicationRels(relids.as_slice())?;

    Ok(myself)
}

/// `publication_add_schema(pubid, schemaid, if_not_exists)`
/// (pg_publication.c:669).
fn publication_add_schema<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    schemaid: Oid,
    if_not_exists: bool,
) -> PgResult<ObjectAddress> {
    let publication = GetPublication(mcx, pubid)?;

    let rel = table_seams::table_open::call(mcx, PublicationNamespaceRelationId, RowExclusiveLock)?;

    if SearchSysCacheExists(
        mcx,
        PUBLICATIONNAMESPACEMAP,
        oid_cache_key(schemaid),
        oid_cache_key(pubid),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )? {
        rel.close(RowExclusiveLock)?;
        if if_not_exists {
            return Ok(InvalidObjectAddress);
        }
        let nm = lsyscache::get_namespace_name::call(mcx, schemaid)?;
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!(
                "schema \"{}\" is already member of publication \"{}\"",
                nm.as_deref().unwrap_or(""),
                publication.name.as_str()
            ))
            .into_error());
    }

    check_publication_add_schema(mcx, schemaid)?;

    // Form a tuple.
    let mut values: [Datum<'mcx>; Natts_pg_publication_namespace] =
        core::array::from_fn(|_| Datum::null());
    let nulls = [false; Natts_pg_publication_namespace];
    let idx = |attno: i32| (attno - 1) as usize;

    let psschid = GetNewOidWithIndex(
        &rel,
        PublicationNamespaceObjectIndexId,
        Anum_pg_publication_namespace_oid as AttrNumber,
    )?;
    values[idx(Anum_pg_publication_namespace_oid)] = Datum::from_oid(psschid);
    values[idx(Anum_pg_publication_namespace_pnpubid)] = Datum::from_oid(pubid);
    values[idx(Anum_pg_publication_namespace_pnnspid)] = Datum::from_oid(schemaid);

    let tupdesc = rel.rd_att_clone_in(mcx)?;
    let mut tup = heap_form_tuple(mcx, &tupdesc, &values, &nulls)
        .map_err(|e| PgError::error(format!("heap_form_tuple failed: {e:?}")))?;

    CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress {
        classId: PublicationNamespaceRelationId,
        objectId: psschid,
        objectSubId: 0,
    };

    // Dependency on the publication.
    let referenced = ObjectAddress {
        classId: PublicationRelationId,
        objectId: pubid,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_AUTO)?;

    // Dependency on the schema.
    let referenced = ObjectAddress {
        classId: NAMESPACE_RELATION_ID,
        objectId: schemaid,
        objectSubId: 0,
    };
    pg_depend_seams::recordDependencyOn::call(mcx, &myself, &referenced, DEPENDENCY_AUTO)?;

    rel.close(RowExclusiveLock)?;

    // Invalidate relcache for the schema's tables (whole hierarchy).
    let schema_rels = GetSchemaPublicationRelations(mcx, schemaid, PublicationPartOpt::All)?;
    InvalidatePublicationRels(schema_rels.as_slice())?;

    Ok(myself)
}

/* ==========================================================================
 * F3: the portable SRF row builder.
 * ========================================================================== */

/// `gather_publication_tables` + `build_publication_table_rows` — the portable
/// body of `pg_get_publication_tables(pubnames text[])` (pg_publication.c:1116).
/// Builds one [`PublicationTableRow`] per published table.
fn build_publication_table_rows<'mcx>(
    mcx: Mcx<'mcx>,
    pubnames: &[&str],
) -> PgResult<PgVec<'mcx, PublicationTableRow<'mcx>>> {
    // --- gather: one published_rel per (table, publication). ---
    let mut table_infos: PgVec<'mcx, PublishedRel> = mcx::vec_with_capacity_in(mcx, 0)?;
    let mut viaroot = false;

    for &name in pubnames {
        let pub_elem = match GetPublicationByName(mcx, name, false)? {
            Some(p) => p,
            None => continue, // GetPublicationByName(false) errors, never None here.
        };

        let pub_elem_tables: PgVec<'mcx, Oid> = if pub_elem.alltables {
            GetAllTablesPublicationRelations(mcx, pub_elem.pubviaroot)?
        } else {
            let partopt = if pub_elem.pubviaroot {
                PublicationPartOpt::Root
            } else {
                PublicationPartOpt::Leaf
            };
            let relids = GetPublicationRelations(mcx, pub_elem.oid, partopt)?;
            let schemarelids = GetAllSchemaPublicationRelations(mcx, pub_elem.oid, partopt)?;
            // list_concat_unique_oid(relids, schemarelids)
            let mut combined: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 0)?;
            for r in relids.iter() {
                combined.push(*r);
            }
            for r in schemarelids.iter() {
                if !combined.iter().any(|x| x == r) {
                    combined.push(*r);
                }
            }
            combined
        };

        for relid in pub_elem_tables.iter() {
            table_infos.push(PublishedRel {
                relid: *relid,
                pubid: pub_elem.oid,
            });
        }

        if pub_elem.pubviaroot {
            viaroot = true;
        }
    }

    if viaroot {
        table_infos = filter_partitions(mcx, table_infos)?;
    }

    // --- build rows. ---
    let mut rows: PgVec<'mcx, PublicationTableRow<'mcx>> = mcx::vec_with_capacity_in(mcx, 0)?;

    for ti in table_infos.iter() {
        let relid = ti.relid;
        let schemaid = lsyscache::get_rel_namespace::call(relid)?;
        let publication = GetPublication(mcx, ti.pubid)?;

        let mut attrs: Option<PgVec<'mcx, u8>> = None;
        let mut qual: Option<PgVec<'mcx, u8>> = None;

        // Row filters / column lists are ignored for FOR ALL TABLES / FOR
        // TABLES IN SCHEMA publications.
        let schema_member = SearchSysCacheExists(
            mcx,
            PUBLICATIONNAMESPACEMAP,
            oid_cache_key(schemaid),
            oid_cache_key(publication.oid),
            SysCacheKey::UNUSED,
            SysCacheKey::UNUSED,
        )?;

        if !publication.alltables && !schema_member {
            let pubtuple = SearchSysCache2(
                mcx,
                PUBLICATIONRELMAP,
                oid_cache_key(relid),
                oid_cache_key(publication.oid),
            )?;
            if let Some(tup) = pubtuple {
                let (a, anull) =
                    SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &tup, Anum_pg_publication_rel_prattrs)?;
                if !anull {
                    attrs = Some(mcx::slice_in(mcx, a.as_ref_bytes())?);
                }
                let (q, qnull) =
                    SysCacheGetAttr(mcx, PUBLICATIONRELMAP, &tup, Anum_pg_publication_rel_prqual)?;
                if !qnull {
                    qual = Some(mcx::slice_in(mcx, q.as_ref_bytes())?);
                }
            }
        }

        // Show all columns when no column list was specified.
        if attrs.is_none() {
            let rel = table_seams::table_open::call(mcx, relid, AccessShareLock)?;
            let desc = &rel.rd_att;
            let mut attnums: std::vec::Vec<i16> = std::vec::Vec::new();
            for i in 0..(desc.natts as usize) {
                let att = desc.attr(i);
                if att.attisdropped {
                    continue;
                }
                if att.attgenerated != 0 {
                    if att.attgenerated != ATTRIBUTE_GENERATED_STORED {
                        continue;
                    }
                    if publication.pubgencols_type != PublishGencolsType::Stored {
                        continue;
                    }
                }
                attnums.push(att.attnum);
            }
            if !attnums.is_empty() {
                attrs = Some(buildint2vector_bytes(mcx, &attnums)?);
            }
            rel.close(AccessShareLock)?;
        }

        rows.push(PublicationTableRow {
            pubid: publication.oid,
            relid,
            attrs,
            qual,
        });
    }

    Ok(rows)
}

/* ==========================================================================
 * Small shared scan/decode utilities + pg_class constants.
 * ========================================================================== */

/// `RelationRelationId` — `pg_class` (used for the full-catalog scans).
const RELATION_RELATION_ID: Oid = 1259;
/// `NamespaceRelationId` — `pg_namespace`.
const NAMESPACE_RELATION_ID: Oid = 2615;
/// `Anum_pg_class_oid` = 1 (`pg_class.h` CATALOG order).
const Anum_pg_class_oid: i32 = 1;
/// `Anum_pg_class_relnamespace` = 3.
const Anum_pg_class_relnamespace: i32 = 3;
/// `Anum_pg_class_relpersistence` = 17.
const Anum_pg_class_relpersistence: i32 = 17;
/// `Anum_pg_class_relkind` = 18.
const Anum_pg_class_relkind: i32 = 18;
/// `Anum_pg_class_relispartition` = 28.
const Anum_pg_class_relispartition: i32 = 28;

/// Deform a freshly scanned tuple into its `(value, isnull)` columns.
fn deform<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<PgVec<'mcx, (Datum<'mcx>, bool)>> {
    let desc = rel.rd_att_clone_in(mcx)?;
    backend_access_common_heaptuple::heap_deform_tuple(mcx, &tup.tuple, &desc, &tup.data)
}

/// Project a scanned `pg_class` tuple into the [`PgClassRow`] the publication
/// scans need.
fn pg_class_row<'mcx>(
    mcx: Mcx<'mcx>,
    class_rel: &Relation<'mcx>,
    tup: &FormedTuple<'mcx>,
) -> PgResult<PgClassRow> {
    let cols = deform(mcx, class_rel, tup)?;
    Ok(PgClassRow {
        oid: cols[(Anum_pg_class_oid - 1) as usize].0.as_oid(),
        relkind: cols[(Anum_pg_class_relkind - 1) as usize].0.as_char() as u8,
        relpersistence: cols[(Anum_pg_class_relpersistence - 1) as usize].0.as_char() as u8,
        relispartition: cols[(Anum_pg_class_relispartition - 1) as usize].0.as_bool(),
        relnamespace: cols[(Anum_pg_class_relnamespace - 1) as usize].0.as_oid(),
    })
}

/// `list_sort(result, list_oid_cmp)` + `list_deduplicate_oid(result)`.
fn sort_dedup_oids(v: &mut PgVec<'_, Oid>) {
    // Sort ascending, then drop adjacent duplicates (the C de-dup only removes
    // adjacent equals after a sort, which is the same as full de-dup here).
    let mut tmp: std::vec::Vec<Oid> = v.iter().copied().collect();
    tmp.sort_unstable();
    tmp.dedup();
    v.clear();
    for x in tmp {
        v.push(x);
    }
}

/* ==========================================================================
 * Seam installation.
 * ========================================================================== */

/// Install every inward seam this unit owns. Wired into `seams-init::init_all`.
pub fn init_seams() {
    use backend_catalog_pg_publication_seams as s;

    s::GetPublication::set(GetPublication);
    s::GetPublicationByName::set(GetPublicationByName);
    s::GetRelationPublications::set(GetRelationPublications);
    s::GetPublicationRelations::set(GetPublicationRelations);
    s::GetAllTablesPublications::set(GetAllTablesPublications);
    s::GetAllTablesPublicationRelations::set(GetAllTablesPublicationRelations);
    s::GetPublicationSchemas::set(GetPublicationSchemas);
    s::GetSchemaPublications::set(GetSchemaPublications);
    s::GetSchemaPublicationRelations::set(GetSchemaPublicationRelations);
    s::GetAllSchemaPublicationRelations::set(GetAllSchemaPublicationRelations);
    s::GetPubPartitionOptionRelations::set(GetPubPartitionOptionRelations);
    s::GetTopMostAncestorInPublication::set(GetTopMostAncestorInPublication);
    s::is_publishable_relation::set(is_publishable_relation);
    s::is_schema_publication::set(is_schema_publication);
    s::check_and_fetch_column_list::set(check_and_fetch_column_list);
    s::pub_collist_validate::set(pub_collist_validate);
    s::pub_collist_to_bitmapset::set(pub_collist_to_bitmapset);
    s::pub_form_cols_map::set(pub_form_cols_map);
    s::publication_add_relation::set(publication_add_relation);
    s::publication_add_schema::set(publication_add_schema);
    s::build_publication_table_rows::set(build_publication_table_rows);
}
