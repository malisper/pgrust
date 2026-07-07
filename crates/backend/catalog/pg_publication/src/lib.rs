// pg_publication.c: publication catalog API. InvalidatePublicationRels
// (C: commands/publicationcmds.c) is hosted here until publicationcmds ports.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgString, PgVec};
use types_core::catalog::{
    FirstNormalObjectId, ATTRIBUTE_GENERATED_STORED, ATTRIBUTE_GENERATED_VIRTUAL,
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use types_core::fmgr::{F_BOOLEQ, F_CHAREQ, F_OIDEQ};
use types_core::primitive::RegProcedure;
use types_core::{
    AttrNumber, InvalidOid, Oid, INT2VECTOROID, NAMESPACE_RELATION_ID, OIDOID, RECORDOID,
    RELATION_RELATION_ID, TEXTOID,
};
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_INVALID_COLUMN_REFERENCE,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_UNDEFINED_COLUMN,
};
use types_fmgr::{byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::{Node, NodeList};
use types_rel::pg_class::{RELKIND_PARTITIONED_TABLE, RELKIND_RELATION};
use types_rel::{AccessShareLock, NoLock, Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, NameData, TupleDescData};

use cache_syscache::cacheinfo::{
    PUBLICATIONNAME, PUBLICATIONNAMESPACEMAP, PUBLICATIONOID, PUBLICATIONRELMAP, RELOID,
};
use cache_syscache::{
    GetSysCacheOid, ReleaseSysCache, ReleaseSysCacheList, SearchSysCache1, SearchSysCache2,
    SearchSysCacheExists, SearchSysCacheList1, SysCacheGetAttr, SysCacheKey,
};
use pg_depend::{recordDependencyOn, recordDependencyOnSingleRelExpr, DependencyType, ObjectAddress};

pub const PublicationRelationId: Oid = 6104;
pub const PublicationObjectIndexId: Oid = 6110;
pub const PublicationNameIndexId: Oid = 6111;
pub const PublicationRelRelationId: Oid = 6106;
pub const PgPublicationRelToastTable: Oid = 6228;
pub const PgPublicationRelToastIndex: Oid = 6229;
pub const PublicationRelObjectIndexId: Oid = 6112;
pub const PublicationRelPrrelidPrpubidIndexId: Oid = 6113;
pub const PublicationRelPrpubidIndexId: Oid = 6116;
pub const PublicationNamespaceRelationId: Oid = 6237;
pub const PublicationNamespaceObjectIndexId: Oid = 6238;
pub const PublicationNamespacePnnspidPnpubidIndexId: Oid = 6239;

pub const Anum_pg_publication_oid: i32 = 1;
pub const Anum_pg_publication_pubname: i32 = 2;
pub const Anum_pg_publication_pubowner: i32 = 3;
pub const Anum_pg_publication_puballtables: i32 = 4;
pub const Anum_pg_publication_pubinsert: i32 = 5;
pub const Anum_pg_publication_pubupdate: i32 = 6;
pub const Anum_pg_publication_pubdelete: i32 = 7;
pub const Anum_pg_publication_pubtruncate: i32 = 8;
pub const Anum_pg_publication_pubviaroot: i32 = 9;
pub const Anum_pg_publication_pubgencols: i32 = 10;
pub const Natts_pg_publication: usize = 10;

pub const Anum_pg_publication_rel_oid: i32 = 1;
pub const Anum_pg_publication_rel_prpubid: i32 = 2;
pub const Anum_pg_publication_rel_prrelid: i32 = 3;
pub const Anum_pg_publication_rel_prqual: i32 = 4;
pub const Anum_pg_publication_rel_prattrs: i32 = 5;
pub const Natts_pg_publication_rel: usize = 5;

pub const Anum_pg_publication_namespace_oid: i32 = 1;
pub const Anum_pg_publication_namespace_pnpubid: i32 = 2;
pub const Anum_pg_publication_namespace_pnnspid: i32 = 3;
pub const Natts_pg_publication_namespace: usize = 3;

pub const PUBLISH_GENCOLS_NONE: u8 = b'n';
pub const PUBLISH_GENCOLS_STORED: u8 = b's';

const Anum_pg_class_oid: i32 = 1;
const Anum_pg_class_relnamespace: i32 = 3;
const Anum_pg_class_relpersistence: i32 = 17;
const Anum_pg_class_relkind: i32 = 18;
const Anum_pg_class_relispartition: i32 = 28;
const PG_NODE_TREEOID: Oid = 194;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationPartOpt {
    Root,
    Leaf,
    All,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

pub struct Publication<'mcx> {
    pub oid: Oid,
    pub name: PgString<'mcx>,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubgencols_type: u8,
    pub pubactions: PublicationActions,
}

pub struct PublicationRelInfo<'a, 'mcx> {
    pub relation: &'a Relation<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub columns: &'a NodeList<'mcx>,
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn getattr(td: &TupleDescData<'_>, tup: &HeapTupleData<'_>, attno: i32) -> (Datum, bool) {
    let mut isnull = false;
    // SAFETY: tup is a catalog row read under its relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attno, td, &mut isnull) };
    (d, isnull)
}

fn cache_td(cache_id: i32) -> PgResult<&'static TupleDescData<'static>> {
    if let Some(td) = catcache::cache_tupdesc(cache_id) {
        return Ok(td);
    }
    catcache::InitCatCachePhase2(cache_id, false)?;
    Ok(catcache::cache_tupdesc(cache_id).expect("phase-2 init left no tupdesc"))
}

fn name_from_datum(d: Datum) -> NameData {
    // SAFETY: a name attr datum addresses NAMEDATALEN in-tuple bytes.
    unsafe { core::ptr::read_unaligned(d.as_usize() as *const NameData) }
}

fn detoast_datum<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    detoast::detoast_attr(mcx, raw)
}

fn varlena_payload(image: &[u8]) -> &[u8] {
    if image[0] & 0x01 == 0x01 {
        &image[1..(image[0] >> 1) as usize]
    } else {
        &image[4..(u32::from_ne_bytes(image[..4].try_into().unwrap()) >> 2) as usize]
    }
}

fn text_datum(mcx: Mcx<'_>, s: &str) -> PgResult<Datum> {
    let img = varlena::cstring_to_text(mcx, s.as_bytes())?.into_image().leak();
    Ok(Datum::from_usize(img.as_ptr() as usize))
}

fn cannot_add_relation(relname: &str, detail: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot add relation \"{relname}\" to publication"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail(detail),
    )
}

fn check_publication_add_relation(targetrel: &Relation<'_>) -> PgResult<()> {
    let relkind = targetrel.rd_rel.relkind as u8;
    if relkind != RELKIND_RELATION && relkind != RELKIND_PARTITIONED_TABLE {
        return Err(cannot_add_relation(
            targetrel.name(),
            pg_class_seams::errdetail_relkind_not_supported::call(relkind)?,
        ));
    }
    if catalog::IsCatalogRelation(targetrel) {
        return Err(cannot_add_relation(
            targetrel.name(),
            "This operation is not supported for system tables.".into(),
        ));
    }
    if targetrel.rd_rel.relpersistence == RELPERSISTENCE_TEMP {
        return Err(cannot_add_relation(
            targetrel.name(),
            "This operation is not supported for temporary tables.".into(),
        ));
    }
    if targetrel.rd_rel.relpersistence == RELPERSISTENCE_UNLOGGED {
        return Err(cannot_add_relation(
            targetrel.name(),
            "This operation is not supported for unlogged tables.".into(),
        ));
    }
    Ok(())
}

fn cannot_add_schema(nspname: &str, detail: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("cannot add schema \"{nspname}\" to publication"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_detail(detail),
    )
}

fn check_publication_add_schema(mcx: Mcx<'_>, schemaid: Oid) -> PgResult<()> {
    if catalog::IsCatalogNamespace(schemaid) || catalog::IsToastNamespace(schemaid) {
        let nspname = lsyscache::get_namespace_name(mcx, schemaid)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        return Err(cannot_add_schema(
            &nspname,
            "This operation is not supported for system schemas.",
        ));
    }
    if catalog_namespace::isAnyTempNamespace(schemaid)? {
        let nspname = lsyscache::get_namespace_name(mcx, schemaid)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        return Err(cannot_add_schema(&nspname, "Temporary schemas cannot be replicated."));
    }
    Ok(())
}

pub fn is_publishable_class(relid: Oid, relkind: u8, relpersistence: u8) -> bool {
    (relkind == RELKIND_RELATION || relkind == RELKIND_PARTITIONED_TABLE)
        && !catalog::IsCatalogRelationOid(relid)
        && relpersistence == RELPERSISTENCE_PERMANENT
        && relid >= FirstNormalObjectId
}

pub fn is_publishable_relation(rel: &Relation<'_>) -> bool {
    is_publishable_class(rel.rd_id, rel.rd_rel.relkind as u8, rel.rd_rel.relpersistence)
}

pub fn is_schema_publication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<bool> {
    let pubschsrel = table::table_open(mcx, PublicationNamespaceRelationId, AccessShareLock)?;
    let keys = [eq_key(
        Anum_pg_publication_namespace_pnpubid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(pubid),
    )];
    // C divergence: C scans the (pnnspid, pnpubid) index keyed on the second
    // column only (nbtree skip scan); ours is unported, so heap scan.
    let mut scan = genam::systable_beginscan(mcx, &pubschsrel, InvalidOid, false, None, &keys)?;
    let result = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    pubschsrel.close(AccessShareLock)?;
    Ok(result)
}

pub fn check_and_fetch_column_list<'mcx>(
    mcx: Mcx<'mcx>,
    publication: &Publication<'_>,
    relid: Oid,
    cols: Option<&mut Bitmapset<'mcx>>,
) -> PgResult<bool> {
    if publication.alltables {
        return Ok(false);
    }
    let mut found = false;
    if let Some(cftuple) = SearchSysCache2(
        PUBLICATIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_oid(publication.oid)),
    )? {
        let (cfdatum, isnull) =
            SysCacheGetAttr(PUBLICATIONRELMAP, &cftuple, Anum_pg_publication_rel_prattrs)?;
        if !isnull {
            if let Some(cols) = cols {
                pub_collist_to_bitmapset(mcx, cols, cfdatum)?;
            }
            found = true;
        }
        ReleaseSysCache(cftuple);
    }
    Ok(found)
}

pub fn GetPubPartitionOptionRelations<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut PgVec<'mcx, Oid>,
    pub_partopt: PublicationPartOpt,
    relid: Oid,
) -> PgResult<()> {
    if lsyscache::get_rel_relkind(relid)? as u8 == RELKIND_PARTITIONED_TABLE
        && pub_partopt != PublicationPartOpt::Root
    {
        let all_parts = pg_inherits::find_all_inheritors(mcx, relid, NoLock)?;
        match pub_partopt {
            PublicationPartOpt::All => {
                for &part in all_parts.iter() {
                    result.push(part);
                }
            }
            PublicationPartOpt::Leaf => {
                for &part in all_parts.iter() {
                    if lsyscache::get_rel_relkind(part)? as u8 != RELKIND_PARTITIONED_TABLE {
                        result.push(part);
                    }
                }
            }
            PublicationPartOpt::Root => unreachable!(),
        }
    } else {
        result.push(relid);
    }
    Ok(())
}

pub fn GetTopMostAncestorInPublication<'mcx>(
    mcx: Mcx<'mcx>,
    puboid: Oid,
    ancestors: &[Oid],
    mut ancestor_level: Option<&mut i32>,
) -> PgResult<Oid> {
    let mut topmost_relid = InvalidOid;
    let mut level = 0;
    for &ancestor in ancestors {
        level += 1;
        let apubids = GetRelationPublications(mcx, ancestor)?;
        if apubids.contains(&puboid) {
            topmost_relid = ancestor;
            if let Some(l) = ancestor_level.as_deref_mut() {
                *l = level;
            }
        } else {
            let aschema_pubids =
                GetSchemaPublications(mcx, lsyscache::get_rel_namespace(ancestor)?)?;
            if aschema_pubids.contains(&puboid) {
                topmost_relid = ancestor;
                if let Some(l) = ancestor_level.as_deref_mut() {
                    *l = level;
                }
            }
        }
    }
    Ok(topmost_relid)
}

fn attnumstoint2vector<'mcx>(mcx: Mcx<'mcx>, attrs: &Bitmapset<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut values: PgVec<'mcx, i16> = mcx::vec_with_capacity_in(mcx, attrs.num_members() as usize)?;
    let mut i = -1;
    loop {
        i = attrs.next_member(i);
        if i < 0 {
            break;
        }
        values.push(i as i16);
    }
    adt_int::buildint2vector(mcx, &values)
}

// InvalidatePublicationRels (C: commands/publicationcmds.c).
pub fn InvalidatePublicationRels(relids: &[Oid]) -> PgResult<()> {
    const MAX_RELCACHE_INVAL_MSGS: usize = 4096;
    if relids.len() < MAX_RELCACHE_INVAL_MSGS {
        for &relid in relids {
            inval::invalidate::CacheInvalidateRelcacheByRelid(relid)?;
        }
    } else {
        inval::invalidate::CacheInvalidateRelcacheAll()?;
    }
    Ok(())
}

pub fn publication_add_relation<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pri: &PublicationRelInfo<'_, 'mcx>,
    if_not_exists: bool,
) -> PgResult<ObjectAddress> {
    let targetrel = pri.relation;
    let relid = targetrel.rd_id;
    let publication = GetPublication(mcx, pubid)?;

    let rel = table::table_open(mcx, PublicationRelRelationId, RowExclusiveLock)?;

    if SearchSysCacheExists(
        PUBLICATIONRELMAP,
        SysCacheKey::Value(Datum::from_oid(relid)),
        SysCacheKey::Value(Datum::from_oid(pubid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )? {
        rel.close(RowExclusiveLock)?;
        if if_not_exists {
            return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
        }
        return Err(Box::new(
            PgError::error(format!(
                "relation \"{}\" is already member of publication \"{}\"",
                targetrel.name(),
                publication.name.as_str()
            ))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }

    check_publication_add_relation(targetrel)?;

    let attnums = pub_collist_validate(mcx, targetrel, pri.columns)?;

    let mut values = [Datum::null(); Natts_pg_publication_rel];
    let mut nulls = [false; Natts_pg_publication_rel];

    let pubreloid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        PublicationRelObjectIndexId,
        Anum_pg_publication_rel_oid as AttrNumber,
    )?;
    values[(Anum_pg_publication_rel_oid - 1) as usize] = Datum::from_oid(pubreloid);
    values[(Anum_pg_publication_rel_prpubid - 1) as usize] = Datum::from_oid(pubid);
    values[(Anum_pg_publication_rel_prrelid - 1) as usize] = Datum::from_oid(relid);

    let qual_text = match pri.whereClause {
        Some(w) => Some(outfuncs::nodeToString(mcx, w)?),
        None => None,
    };
    match &qual_text {
        Some(t) => {
            values[(Anum_pg_publication_rel_prqual - 1) as usize] = text_datum(mcx, t.as_str())?
        }
        None => nulls[(Anum_pg_publication_rel_prqual - 1) as usize] = true,
    }

    let attrs_img;
    if !pri.columns.is_nil() {
        attrs_img = attnumstoint2vector(mcx, &attnums)?;
        values[(Anum_pg_publication_rel_prattrs - 1) as usize] =
            Datum::from_usize(attrs_img.as_ptr() as usize);
    } else {
        nulls[(Anum_pg_publication_rel_prattrs - 1) as usize] = true;
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(PublicationRelRelationId, pubreloid);
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PublicationRelationId, pubid),
        DependencyType::Auto,
    )?;
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(RELATION_RELATION_ID, relid),
        DependencyType::Auto,
    )?;
    if let Some(w) = pri.whereClause {
        recordDependencyOnSingleRelExpr(
            mcx,
            &myself,
            w,
            relid,
            DependencyType::Normal,
            DependencyType::Normal,
            false,
        )?;
    }
    let mut i = -1;
    loop {
        i = attnums.next_member(i);
        if i < 0 {
            break;
        }
        recordDependencyOn(
            mcx,
            &myself,
            &ObjectAddress::sub_set(RELATION_RELATION_ID, relid, i),
            DependencyType::Normal,
        )?;
    }

    rel.close(RowExclusiveLock)?;

    let mut relids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    GetPubPartitionOptionRelations(mcx, &mut relids, PublicationPartOpt::All, relid)?;
    InvalidatePublicationRels(&relids)?;

    Ok(myself)
}

pub fn pub_collist_validate<'mcx>(
    mcx: Mcx<'mcx>,
    targetrel: &Relation<'mcx>,
    columns: &NodeList<'_>,
) -> PgResult<Bitmapset<'mcx>> {
    let mut set = Bitmapset::empty();
    let tupdesc = targetrel.descr();
    for cell in columns.iter() {
        let colname = cell.as_string().expect("publication column list cell is a String").sval;
        let attnum = lsyscache::get_attnum(targetrel.rd_id, colname)?;
        if attnum == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "column \"{colname}\" of relation \"{}\" does not exist",
                    targetrel.name()
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
            ));
        }
        if attnum <= 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot use system column \"{colname}\" in publication column list"
                ))
                .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
            ));
        }
        if tupdesc.attr((attnum - 1) as usize).attgenerated as u8 == ATTRIBUTE_GENERATED_VIRTUAL {
            return Err(Box::new(
                PgError::error(format!(
                    "cannot use virtual generated column \"{colname}\" in publication column list"
                ))
                .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE),
            ));
        }
        if set.is_member(attnum as i32) {
            return Err(Box::new(
                PgError::error(format!(
                    "duplicate column \"{colname}\" in publication column list"
                ))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
            ));
        }
        set.add_member(mcx, attnum as i32)?;
    }
    Ok(set)
}

pub fn pub_collist_to_bitmapset<'mcx>(
    mcx: Mcx<'mcx>,
    columns: &mut Bitmapset<'mcx>,
    pubcols: Datum,
) -> PgResult<()> {
    let img = detoast_datum(mcx, pubcols)?;
    let dim1 = i32::from_ne_bytes(img[16..20].try_into().unwrap());
    for i in 0..dim1 as usize {
        let off = 24 + 2 * i;
        let v = i16::from_ne_bytes(img[off..off + 2].try_into().unwrap());
        columns.add_member(mcx, v as i32)?;
    }
    Ok(())
}

pub fn publication_add_schema<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    schemaid: Oid,
    if_not_exists: bool,
) -> PgResult<ObjectAddress> {
    let publication = GetPublication(mcx, pubid)?;

    let rel = table::table_open(mcx, PublicationNamespaceRelationId, RowExclusiveLock)?;

    if SearchSysCacheExists(
        PUBLICATIONNAMESPACEMAP,
        SysCacheKey::Value(Datum::from_oid(schemaid)),
        SysCacheKey::Value(Datum::from_oid(pubid)),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )? {
        rel.close(RowExclusiveLock)?;
        if if_not_exists {
            return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
        }
        let nspname = lsyscache::get_namespace_name(mcx, schemaid)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        return Err(Box::new(
            PgError::error(format!(
                "schema \"{nspname}\" is already member of publication \"{}\"",
                publication.name.as_str()
            ))
            .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }

    check_publication_add_schema(mcx, schemaid)?;

    let mut values = [Datum::null(); Natts_pg_publication_namespace];
    let nulls = [false; Natts_pg_publication_namespace];

    let psschid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        PublicationNamespaceObjectIndexId,
        Anum_pg_publication_namespace_oid as AttrNumber,
    )?;
    values[(Anum_pg_publication_namespace_oid - 1) as usize] = Datum::from_oid(psschid);
    values[(Anum_pg_publication_namespace_pnpubid - 1) as usize] = Datum::from_oid(pubid);
    values[(Anum_pg_publication_namespace_pnnspid - 1) as usize] = Datum::from_oid(schemaid);

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(PublicationNamespaceRelationId, psschid);
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(PublicationRelationId, pubid),
        DependencyType::Auto,
    )?;
    recordDependencyOn(
        mcx,
        &myself,
        &ObjectAddress::set(NAMESPACE_RELATION_ID, schemaid),
        DependencyType::Auto,
    )?;

    rel.close(RowExclusiveLock)?;

    let schemaRels = GetSchemaPublicationRelations(mcx, schemaid, PublicationPartOpt::All)?;
    InvalidatePublicationRels(&schemaRels)?;

    Ok(myself)
}

pub fn GetRelationPublications<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let pubrellist =
        SearchSysCacheList1(PUBLICATIONRELMAP, SysCacheKey::Value(Datum::from_oid(relid)))?;
    let td = cache_td(PUBLICATIONRELMAP)?;
    for i in 0..pubrellist.n_members() as usize {
        let m = pubrellist.member(i);
        let t = m.tuple();
        let (d, _) = getattr(td, &t, Anum_pg_publication_rel_prpubid);
        result.push(d.as_oid());
    }
    ReleaseSysCacheList(pubrellist);
    Ok(result)
}

pub fn GetPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let pubrelsrel = table::table_open(mcx, PublicationRelRelationId, AccessShareLock)?;
    let keys = [eq_key(
        Anum_pg_publication_rel_prpubid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(pubid),
    )];
    let mut scan = genam::systable_beginscan(
        mcx,
        &pubrelsrel,
        PublicationRelPrpubidIndexId,
        true,
        None,
        &keys,
    )?;
    let td = pubrelsrel.descr();
    let mut relids: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (d, _) = getattr(td, tup, Anum_pg_publication_rel_prrelid);
        relids.push(d.as_oid());
    }
    genam::systable_endscan(mcx, scan)?;
    pubrelsrel.close(AccessShareLock)?;

    for &relid in relids.iter() {
        GetPubPartitionOptionRelations(mcx, &mut result, pub_partopt, relid)?;
    }
    result.sort_unstable();
    result.dedup();
    Ok(result)
}

pub fn GetAllTablesPublications<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let rel = table::table_open(mcx, PublicationRelationId, AccessShareLock)?;
    let keys = [eq_key(
        Anum_pg_publication_puballtables as AttrNumber,
        F_BOOLEQ,
        Datum::from_bool(true),
    )];
    let mut scan = genam::systable_beginscan(mcx, &rel, InvalidOid, false, None, &keys)?;
    let td = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (d, _) = getattr(td, tup, Anum_pg_publication_oid);
        result.push(d.as_oid());
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(result)
}

pub fn GetAllTablesPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubviaroot: bool,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let classRel = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let td = classRel.descr();

    let keys = [eq_key(
        Anum_pg_class_relkind as AttrNumber,
        F_CHAREQ,
        Datum::from_char(RELKIND_RELATION as i8),
    )];
    let mut scan = genam::systable_beginscan(mcx, &classRel, InvalidOid, false, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let relid = getattr(td, tup, Anum_pg_class_oid).0.as_oid();
        let relpersistence = getattr(td, tup, Anum_pg_class_relpersistence).0.as_u8();
        let relispartition = getattr(td, tup, Anum_pg_class_relispartition).0.as_bool();
        if is_publishable_class(relid, RELKIND_RELATION, relpersistence)
            && !(relispartition && pubviaroot)
        {
            result.push(relid);
        }
    }
    genam::systable_endscan(mcx, scan)?;

    if pubviaroot {
        let keys = [eq_key(
            Anum_pg_class_relkind as AttrNumber,
            F_CHAREQ,
            Datum::from_char(RELKIND_PARTITIONED_TABLE as i8),
        )];
        let mut scan = genam::systable_beginscan(mcx, &classRel, InvalidOid, false, None, &keys)?;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let relid = getattr(td, tup, Anum_pg_class_oid).0.as_oid();
            let relpersistence = getattr(td, tup, Anum_pg_class_relpersistence).0.as_u8();
            let relispartition = getattr(td, tup, Anum_pg_class_relispartition).0.as_bool();
            if is_publishable_class(relid, RELKIND_PARTITIONED_TABLE, relpersistence)
                && !relispartition
            {
                result.push(relid);
            }
        }
        genam::systable_endscan(mcx, scan)?;
    }

    classRel.close(AccessShareLock)?;
    Ok(result)
}

pub fn GetPublicationSchemas<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let pubschsrel = table::table_open(mcx, PublicationNamespaceRelationId, AccessShareLock)?;
    let keys = [eq_key(
        Anum_pg_publication_namespace_pnpubid as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(pubid),
    )];
    // C divergence: see is_schema_publication (skip scan unported).
    let mut scan = genam::systable_beginscan(mcx, &pubschsrel, InvalidOid, false, None, &keys)?;
    let td = pubschsrel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let (d, _) = getattr(td, tup, Anum_pg_publication_namespace_pnnspid);
        result.push(d.as_oid());
    }
    genam::systable_endscan(mcx, scan)?;
    pubschsrel.close(AccessShareLock)?;
    Ok(result)
}

pub fn GetSchemaPublications<'mcx>(mcx: Mcx<'mcx>, schemaid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let pubschlist = SearchSysCacheList1(
        PUBLICATIONNAMESPACEMAP,
        SysCacheKey::Value(Datum::from_oid(schemaid)),
    )?;
    let td = cache_td(PUBLICATIONNAMESPACEMAP)?;
    for i in 0..pubschlist.n_members() as usize {
        let m = pubschlist.member(i);
        let t = m.tuple();
        let (d, _) = getattr(td, &t, Anum_pg_publication_namespace_pnpubid);
        result.push(d.as_oid());
    }
    ReleaseSysCacheList(pubschlist);
    Ok(result)
}

pub fn GetSchemaPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    schemaid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    debug_assert!(schemaid != InvalidOid);
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let classRel = table::table_open(mcx, RELATION_RELATION_ID, AccessShareLock)?;
    let td = classRel.descr();
    let keys = [eq_key(
        Anum_pg_class_relnamespace as AttrNumber,
        F_OIDEQ,
        Datum::from_oid(schemaid),
    )];
    let mut scan = genam::systable_beginscan(mcx, &classRel, InvalidOid, false, None, &keys)?;
    let mut rows: PgVec<'mcx, (Oid, u8, u8)> = PgVec::new_in(mcx);
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let relid = getattr(td, tup, Anum_pg_class_oid).0.as_oid();
        let relkind = getattr(td, tup, Anum_pg_class_relkind).0.as_u8();
        let relpersistence = getattr(td, tup, Anum_pg_class_relpersistence).0.as_u8();
        rows.push((relid, relkind, relpersistence));
    }
    genam::systable_endscan(mcx, scan)?;
    classRel.close(AccessShareLock)?;

    for &(relid, tup_relkind, relpersistence) in rows.iter() {
        if !is_publishable_class(relid, tup_relkind, relpersistence) {
            continue;
        }
        let relkind = lsyscache::get_rel_relkind(relid)? as u8;
        if relkind == RELKIND_RELATION {
            result.push(relid);
        } else if relkind == RELKIND_PARTITIONED_TABLE {
            let mut partitionrels: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
            GetPubPartitionOptionRelations(mcx, &mut partitionrels, pub_partopt, relid)?;
            for &part in partitionrels.iter() {
                if !result.contains(&part) {
                    result.push(part);
                }
            }
        }
    }
    Ok(result)
}

pub fn GetAllSchemaPublicationRelations<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    pub_partopt: PublicationPartOpt,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let pubschemalist = GetPublicationSchemas(mcx, pubid)?;
    for &schemaid in pubschemalist.iter() {
        let schemaRels = GetSchemaPublicationRelations(mcx, schemaid, pub_partopt)?;
        for &r in schemaRels.iter() {
            result.push(r);
        }
    }
    Ok(result)
}

pub fn GetPublication<'mcx>(mcx: Mcx<'mcx>, pubid: Oid) -> PgResult<Publication<'mcx>> {
    let Some(tup) = SearchSysCache1(PUBLICATIONOID, SysCacheKey::Value(Datum::from_oid(pubid)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for publication {pubid}"
        ))));
    };
    let attr = |anum: i32| -> PgResult<Datum> {
        Ok(SysCacheGetAttr(PUBLICATIONOID, &tup, anum)?.0)
    };
    let name_data = name_from_datum(attr(Anum_pg_publication_pubname)?);
    let name = PgString::from_str_in(
        core::str::from_utf8(name_data.name_str()).expect("pubname is UTF-8"),
        mcx,
    )?;
    let publication = Publication {
        oid: pubid,
        name,
        alltables: attr(Anum_pg_publication_puballtables)?.as_bool(),
        pubviaroot: attr(Anum_pg_publication_pubviaroot)?.as_bool(),
        pubgencols_type: attr(Anum_pg_publication_pubgencols)?.as_u8(),
        pubactions: PublicationActions {
            pubinsert: attr(Anum_pg_publication_pubinsert)?.as_bool(),
            pubupdate: attr(Anum_pg_publication_pubupdate)?.as_bool(),
            pubdelete: attr(Anum_pg_publication_pubdelete)?.as_bool(),
            pubtruncate: attr(Anum_pg_publication_pubtruncate)?.as_bool(),
        },
    };
    ReleaseSysCache(tup);
    Ok(publication)
}

pub fn GetPublicationByName<'mcx>(
    mcx: Mcx<'mcx>,
    pubname: &str,
    missing_ok: bool,
) -> PgResult<Option<Publication<'mcx>>> {
    let oid = lsyscache::get_publication_oid(pubname, missing_ok)?;
    if oid == InvalidOid {
        return Ok(None);
    }
    Ok(Some(GetPublication(mcx, oid)?))
}

fn seam_lookup_pg_publication_oid(pubname: &str) -> PgResult<Oid> {
    GetSysCacheOid(
        PUBLICATIONNAME,
        Anum_pg_publication_oid,
        SysCacheKey::Str(pubname),
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
        SysCacheKey::UNUSED,
    )
}

fn seam_pg_publication_pubname(pubid: Oid) -> PgResult<Option<NameData>> {
    let Some(tup) = SearchSysCache1(PUBLICATIONOID, SysCacheKey::Value(Datum::from_oid(pubid)))?
    else {
        return Ok(None);
    };
    let (d, _) = SysCacheGetAttr(PUBLICATIONOID, &tup, Anum_pg_publication_pubname)?;
    let name = name_from_datum(d);
    ReleaseSysCache(tup);
    Ok(Some(name))
}

fn fc_pg_relation_is_publishable(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    let Some(tup) = SearchSysCache1(RELOID, SysCacheKey::Value(Datum::from_oid(relid)))? else {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    };
    let relkind = SysCacheGetAttr(RELOID, &tup, Anum_pg_class_relkind)?.0.as_u8();
    let relpersistence = SysCacheGetAttr(RELOID, &tup, Anum_pg_class_relpersistence)?.0.as_u8();
    ReleaseSysCache(tup);
    Ok(Datum::from_bool(is_publishable_class(relid, relkind, relpersistence)))
}

// Cross-arena SRF carrier (fn_extra is 'static): std Vec by necessity.
struct PubTablesRows {
    tuples: Vec<Vec<u8>>,
}

fn filter_partitions(mcx: Mcx<'_>, table_infos: &mut Vec<(Oid, Oid)>) -> PgResult<()> {
    let mut i = 0;
    while i < table_infos.len() {
        let relid = table_infos[i].0;
        let mut skip = false;
        if lsyscache::get_rel_relispartition(relid)? {
            let ancestors = pg_inherits::get_partition_ancestors(mcx, relid)?;
            for &ancestor in ancestors.iter() {
                if table_infos.iter().any(|&(r, _)| r == ancestor) {
                    skip = true;
                    break;
                }
            }
        }
        if skip {
            table_infos.remove(i);
        } else {
            i += 1;
        }
    }
    Ok(())
}

fn collect_publication_tables(fcinfo: &Fcinfo) -> PgResult<PubTablesRows> {
    let mcx = fcinfo.result_mcx();
    let arr_img = detoast_datum(mcx, fcinfo.arg(0))?;
    let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, &arr_img, TEXTOID, false)?;

    let mut table_infos: Vec<(Oid, Oid)> = Vec::new();
    let mut viaroot = false;
    for &elem in elems.iter() {
        let name_img = detoast_datum(mcx, elem)?;
        let pubname =
            core::str::from_utf8(varlena_payload(&name_img)).expect("publication name is UTF-8");
        let pub_elem =
            GetPublicationByName(mcx, pubname, false)?.expect("missing_ok=false yields an error");

        let pub_elem_tables = if pub_elem.alltables {
            GetAllTablesPublicationRelations(mcx, pub_elem.pubviaroot)?
        } else {
            let part = if pub_elem.pubviaroot {
                PublicationPartOpt::Root
            } else {
                PublicationPartOpt::Leaf
            };
            let mut relids = GetPublicationRelations(mcx, pub_elem.oid, part)?;
            let schemarelids = GetAllSchemaPublicationRelations(mcx, pub_elem.oid, part)?;
            for &r in schemarelids.iter() {
                if !relids.contains(&r) {
                    relids.push(r);
                }
            }
            relids
        };

        for &relid in pub_elem_tables.iter() {
            table_infos.push((relid, pub_elem.oid));
        }
        if pub_elem.pubviaroot {
            viaroot = true;
        }
    }

    if viaroot {
        filter_partitions(mcx, &mut table_infos)?;
    }

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 4)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("pubid"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("relid"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 3, Some("attrs"), INT2VECTOROID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 4, Some("qual"), PG_NODE_TREEOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc: consumers of the composite datums (put_composite_row's
    // rowtype lookup, record_out) need the registered typmod.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut tuples = Vec::with_capacity(table_infos.len());
    for &(relid, pubid) in &table_infos {
        let publication = GetPublication(mcx, pubid)?;
        let schemaid = lsyscache::get_rel_namespace(relid)?;

        let mut attrs_img: Option<PgVec<'_, u8>> = None;
        let mut qual_img: Option<PgVec<'_, u8>> = None;
        if !publication.alltables
            && !SearchSysCacheExists(
                PUBLICATIONNAMESPACEMAP,
                SysCacheKey::Value(Datum::from_oid(schemaid)),
                SysCacheKey::Value(Datum::from_oid(pubid)),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )?
        {
            if let Some(pubtuple) = SearchSysCache2(
                PUBLICATIONRELMAP,
                SysCacheKey::Value(Datum::from_oid(relid)),
                SysCacheKey::Value(Datum::from_oid(pubid)),
            )? {
                let (d, isnull) =
                    SysCacheGetAttr(PUBLICATIONRELMAP, &pubtuple, Anum_pg_publication_rel_prattrs)?;
                if !isnull {
                    attrs_img = Some(detoast_datum(mcx, d)?);
                }
                let (d, isnull) =
                    SysCacheGetAttr(PUBLICATIONRELMAP, &pubtuple, Anum_pg_publication_rel_prqual)?;
                if !isnull {
                    qual_img = Some(detoast_datum(mcx, d)?);
                }
                ReleaseSysCache(pubtuple);
            }
        }

        if attrs_img.is_none() {
            let rel = table::table_open(mcx, relid, AccessShareLock)?;
            let rd = rel.descr();
            let mut attnums: PgVec<'_, i16> = mcx::vec_with_capacity_in(mcx, rd.natts as usize)?;
            for i in 0..rd.natts as usize {
                let att = rd.attr(i);
                if att.attisdropped {
                    continue;
                }
                if att.attgenerated != 0 {
                    if att.attgenerated as u8 != ATTRIBUTE_GENERATED_STORED {
                        continue;
                    }
                    if publication.pubgencols_type != PUBLISH_GENCOLS_STORED {
                        continue;
                    }
                }
                attnums.push(att.attnum);
            }
            if !attnums.is_empty() {
                attrs_img = Some(adt_int::buildint2vector(mcx, &attnums)?);
            }
            rel.close(AccessShareLock)?;
        }

        let mut values = [Datum::null(); 4];
        let mut nulls = [false; 4];
        values[0] = Datum::from_oid(pubid);
        values[1] = Datum::from_oid(relid);
        match &attrs_img {
            Some(v) => values[2] = Datum::from_usize(v.as_ptr() as usize),
            None => nulls[2] = true,
        }
        match &qual_img {
            Some(v) => values[3] = Datum::from_usize(v.as_ptr() as usize),
            None => nulls[3] = true,
        }
        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &nulls)?;
        tuples.push(tuple.image().to_vec());
    }

    Ok(PubTablesRows { tuples })
}

fn fc_pg_get_publication_tables(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_publication_tables: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = collect_publication_tables(fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_get_publication_tables: rows set at first call")
        .downcast_ref::<PubTablesRows>()
        .expect("pg_get_publication_tables: user_fctx is PubTablesRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

const fn b(foid: Oid, name: &'static str, nargs: i16, retset: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset, func }
}

pub const PUBLICATION_BUILTINS: &[FmgrBuiltin] = &[
    b(6119, "pg_get_publication_tables", 1, true, fc_pg_get_publication_tables),
    b(6121, "pg_relation_is_publishable", 1, false, fc_pg_relation_is_publishable),
];

pub fn init_seams() {
    syscache_seams::lookup_pg_publication_oid::set(seam_lookup_pg_publication_oid);
    syscache_seams::pg_publication_pubname::set(seam_pg_publication_pubname);
    fmgr_core::register_late_builtins(PUBLICATION_BUILTINS);
}
