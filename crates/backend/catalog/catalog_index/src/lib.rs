// index.c, bounded to plain btree indexes on non-shared, non-mapped,
// permanent relations built empty (the toast-index lane); unreached arms are
// loud with their C symbol.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use execindexing::IndexInfo;
use mcx::Mcx;
use types_core::{
    AttrNumber, ForkNumber, InvalidOid, Oid, ATTRIBUTE_RELATION_ID, DEFAULT_COLLATION_OID,
    INDEX_RELATION_ID, RELATION_RELATION_ID,
};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_TABLE, ERRCODE_FEATURE_NOT_SUPPORTED};
use types_rel::{
    AccessExclusiveLock, NoLock, Relation, RowExclusiveLock, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_RELATION, RELKIND_TOASTVALUE,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{NameData, TupleDescData};

pub const INDEX_CREATE_IS_PRIMARY: u16 = 1 << 0;
pub const INDEX_CREATE_ADD_CONSTRAINT: u16 = 1 << 1;
pub const INDEX_CREATE_SKIP_BUILD: u16 = 1 << 2;
pub const INDEX_CREATE_CONCURRENT: u16 = 1 << 3;
pub const INDEX_CREATE_IF_NOT_EXISTS: u16 = 1 << 4;
pub const INDEX_CREATE_PARTITIONED: u16 = 1 << 5;
pub const INDEX_CREATE_INVALID: u16 = 1 << 6;

pub const INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: u16 = 1 << 0;
pub const INDEX_CONSTR_CREATE_DEFERRABLE: u16 = 1 << 1;
pub const INDEX_CONSTR_CREATE_INIT_DEFERRED: u16 = 1 << 2;
pub const INDEX_CONSTR_CREATE_UPDATE_INDEX: u16 = 1 << 3;
pub const INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: u16 = 1 << 4;
pub const INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS: u16 = 1 << 5;

pub const BTREE_AM_OID: Oid = 403;
pub const HASH_AM_OID: Oid = 405;
pub const GIN_AM_OID: Oid = 2742;
pub const GIST_AM_OID: Oid = 783;
const INT4OID: Oid = 23;
const OpclassOidIndexId: Oid = 2687;
const IndexRelidIndexId: Oid = 2679;
const Anum_pg_opclass_opcintype: usize = 7;
const Anum_pg_opclass_opckeytype: usize = 9;
const INT2OID: Oid = 21;
const OIDOID: Oid = 26;

const Natts_pg_index: usize = 21;
const Anum_pg_class_oid: usize = 1;
const Anum_pg_class_relpages: usize = 10;
const Anum_pg_class_reltuples: usize = 11;
const Anum_pg_class_relallvisible: usize = 12;
const Anum_pg_class_relallfrozen: usize = 13;
const Anum_pg_class_relhasindex: usize = 15;

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: index.c {what}")
}

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(types_error::ERROR, msg).with_sqlstate(sqlstate))
}

fn oid_scankey(attno: usize, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn getattr(
    tup: &types_tuple::HeapTupleData<'_>,
    attnum: usize,
    desc: &TupleDescData<'_>,
) -> Datum {
    let mut isnull = false;
    // SAFETY: fixed-position catalog column under the relation's descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, desc, &mut isnull) };
    debug_assert!(!isnull);
    d
}

const Anum_pg_index_indisprimary: usize = 7;
const Anum_pg_index_indimmediate: usize = 9;

// index_check_primary_key (index.c); NULLS NOT DISTINCT indexes are
// unreachable here (USING INDEX is loud upstream).
pub fn index_check_primary_key<'mcx>(
    mcx: Mcx<'mcx>,
    heapRel: &Relation<'mcx>,
    indexInfo: &IndexInfo<'mcx>,
    is_alter_table: bool,
) -> PgResult<()> {
    if (is_alter_table || heapRel.rd_rel.relispartition) && relationHasPrimaryKey(mcx, heapRel)? {
        return Err(err(
            format!(
                "multiple primary keys for table \"{}\" are not allowed",
                heapRel.name()
            ),
            types_error::ERRCODE_INVALID_TABLE_DEFINITION,
        ));
    }
    if indexInfo.ii_NullsNotDistinct {
        return Err(err(
            "primary keys cannot use NULLS NOT DISTINCT indexes".into(),
            types_error::ERRCODE_INVALID_TABLE_DEFINITION,
        ));
    }
    for i in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
        let attnum = indexInfo.ii_IndexAttrNumbers[i];
        if attnum == 0 {
            return Err(err(
                "primary keys cannot be expressions".into(),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        if attnum < 0 {
            continue;
        }
        let att = heapRel.rd_att.attr(attnum as usize - 1);
        if !att.attnotnull {
            let colname = core::str::from_utf8(att.attname.name_str()).expect("attname UTF-8");
            return Err(err(
                format!("primary key column \"{colname}\" is not marked NOT NULL"),
                types_error::ERRCODE_INVALID_TABLE_DEFINITION,
            ));
        }
    }
    Ok(())
}

fn relationHasPrimaryKey<'mcx>(mcx: Mcx<'mcx>, rel: &Relation<'mcx>) -> PgResult<bool> {
    let indexes = relcache::RelationGetIndexList(mcx, rel.rd_id)?;
    for &indexoid in indexes.iter() {
        let pg_index = table::table_open(mcx, INDEX_RELATION_ID, types_rel::AccessShareLock)?;
        let key = oid_scankey(1, indexoid);
        let mut scan =
            genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &[key])?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {indexoid}"));
        let isprimary = getattr(tup, Anum_pg_index_indisprimary, pg_index.descr()).as_bool();
        genam::systable_endscan(mcx, scan)?;
        pg_index.close(types_rel::AccessShareLock)?;
        if isprimary {
            return Ok(true);
        }
    }
    Ok(false)
}

// ConstructTupleDescriptor (index.c).
fn ConstructTupleDescriptor<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexInfo: &IndexInfo<'mcx>,
    indexColNames: &[&str],
    accessMethodId: Oid,
    collationIds: &[Oid],
    opclassIds: &[Oid],
) -> PgResult<TupleDescData<'mcx>> {
    // amroutine->amkeytype: InvalidOid for btree/gin/gist/spgist/brin,
    // INT4OID for hash; from_relam covers non-builtin AMs over builtin
    // handlers (index_create registered the mapping).
    let amkeytype = match types_relscan::IndexAmKind::from_relam(accessMethodId) {
        types_relscan::IndexAmKind::Hash => INT4OID,
        _ => InvalidOid,
    };
    let numatts = indexInfo.ii_NumIndexAttrs as usize;
    let numkeyatts = indexInfo.ii_NumIndexKeyAttrs as usize;
    let heapTupDesc = heapRelation.descr();
    let natts = heapTupDesc.natts;

    let mut indexTupDesc = tupdesc::CreateTemplateTupleDesc(mcx, numatts as i32)?;

    let mut indexpr_item = indexInfo.ii_Expressions.iter();
    for i in 0..numatts {
        let atnum = indexInfo.ii_IndexAttrNumbers[i];
        // namestrcpy truncates at NAMEDATALEN-1 bytes, as C.
        let colname = indexColNames[i];
        if atnum != 0 {
            if atnum < 0 || atnum as i32 > natts {
                panic!("invalid column number {atnum}");
            }
            let from = *heapTupDesc.attr(atnum as usize - 1);
            let to = indexTupDesc.attr_mut(i);
            *to = from;
            to.attnum = (i + 1) as i16;
            to.attislocal = true;
            to.attcollation = if i < numkeyatts { collationIds[i] } else { InvalidOid };
            to.attname = NameData::default();
            to.attname.namestrcpy(colname);
            to.attnotnull = false;
            to.atthasdef = false;
            to.atthasmissing = false;
            to.attidentity = 0;
            to.attgenerated = 0;
            to.attisdropped = false;
            to.attinhcount = 0;
            to.attndims = from.attndims;
            to.attrelid = InvalidOid;
        } else {
            let indexkey = indexpr_item.next().expect("too few entries in indexprs list");
            let keyType = nodes_core::expr_type(indexkey);
            // index.c ConstructTupleDescriptor calls CheckAttributeType(flags=0),
            // rejecting any pseudo-type result (e.g. an anonymous record).
            if lsyscache::get_typtype(keyType)? == b'p' as i8 {
                return Err(err(
                    format!(
                        "column \"{colname}\" has pseudo-type {}",
                        format_type::format_type_be(keyType)?
                    ),
                    types_error::ERRCODE_INVALID_TABLE_DEFINITION,
                ));
            }
            let shape = syscache_seams::lookup_pg_type_shape::call(keyType)?
                .unwrap_or_else(|| panic!("cache lookup failed for type {keyType}"));
            let to = indexTupDesc.attr_mut(i);
            to.attnum = (i + 1) as i16;
            to.attislocal = true;
            to.attcollation = if i < numkeyatts { collationIds[i] } else { InvalidOid };
            to.attname = NameData::default();
            to.attname.namestrcpy(colname);
            to.atttypid = keyType;
            to.attlen = shape.typlen;
            to.atttypmod = nodes_core::expr_typmod(indexkey);
            to.attbyval = shape.typbyval;
            to.attalign = shape.typalign;
            to.attstorage = shape.typstorage;
            to.attcompression = 0;
            to.attrelid = InvalidOid;
        }

        // amkeytype, overridable by pg_opclass.opckeytype.
        let mut keyType = amkeytype;
        if i < numkeyatts {
            let (opckeytype, opcintype) = lookup_opclass_keytype(mcx, opclassIds[i])?;
            if opckeytype != InvalidOid {
                keyType = opckeytype;
            }
            const ANYELEMENTOID: Oid = 2283;
            const ANYARRAYOID: Oid = 2277;
            if keyType == ANYELEMENTOID && opcintype == ANYARRAYOID {
                let atttypid = indexTupDesc.attr(i).atttypid;
                keyType = lsyscache::get_base_element_type(atttypid)?;
                if keyType == InvalidOid {
                    panic!("could not get element type of array type {atttypid}");
                }
            }
        }
        if keyType != InvalidOid && keyType != indexTupDesc.attr(i).atttypid {
            let shape = syscache_seams::lookup_pg_type_shape::call(keyType)?
                .unwrap_or_else(|| panic!("cache lookup failed for type {keyType}"));
            let to = indexTupDesc.attr_mut(i);
            to.atttypid = keyType;
            to.atttypmod = -1;
            to.attlen = shape.typlen;
            to.attbyval = shape.typbyval;
            to.attalign = shape.typalign;
            to.attstorage = shape.typstorage;
            to.attcompression = 0;
        }
        tupdesc::populate_compact_attribute(&mut indexTupDesc, i);
    }
    Ok(indexTupDesc)
}

fn lookup_opclass_keytype<'mcx>(mcx: Mcx<'mcx>, opclass: Oid) -> PgResult<(Oid, Oid)> {
    // C: SearchSysCache1(CLAOID); the shape cache lacks opckeytype, so read
    // the row directly.
    let rel = table::table_open(mcx, catalog::OperatorClassRelationId, types_rel::AccessShareLock)?;
    let key = oid_scankey(1, opclass);
    let mut scan = genam::systable_beginscan(mcx, &rel, OpclassOidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for opclass {opclass}"));
    let opckeytype = getattr(tup, Anum_pg_opclass_opckeytype, rel.descr()).as_oid();
    let opcintype = getattr(tup, Anum_pg_opclass_opcintype, rel.descr()).as_oid();
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok((opckeytype, opcintype))
}

fn build_vector_datum<'mcx>(
    mcx: Mcx<'mcx>,
    elemtype: Oid,
    elemlen: usize,
    data: &[u8],
    n: usize,
) -> PgResult<mcx::PgVec<'mcx, u32>> {
    debug_assert!(data.len() == n * elemlen);
    let size = 24 + data.len();
    let words = size.div_ceil(4);
    let mut buf: mcx::PgVec<'mcx, u32> = mcx::vec_with_capacity_in(mcx, words)?;
    buf.resize(words, 0);
    buf[0] = types_tuple::varatt::set_varsize_4b_word(size as u32);
    buf[1] = 1; // ndim
    buf[2] = 0; // dataoffset
    buf[3] = elemtype;
    buf[4] = n as u32; // dim1
    buf[5] = 0; // lbound1
    // SAFETY: tail of the zeroed word buffer, in-bounds by construction.
    unsafe {
        core::ptr::copy_nonoverlapping(
            data.as_ptr(),
            (buf.as_mut_ptr() as *mut u8).add(24),
            data.len(),
        )
    };
    Ok(buf)
}

// UpdateIndexRelation: insert the pg_index row.
#[allow(clippy::too_many_arguments)]
fn UpdateIndexRelation<'mcx>(
    mcx: Mcx<'mcx>,
    indexoid: Oid,
    heapoid: Oid,
    indexInfo: &IndexInfo<'mcx>,
    collationOids: &[Oid],
    opclassOids: &[Oid],
    coloptions: &[i16],
    primary: bool,
    isexclusion: bool,
    immediate: bool,
    isvalid: bool,
    isready: bool,
) -> PgResult<()> {
    let natts = indexInfo.ii_NumIndexAttrs as usize;
    let nkeyatts = indexInfo.ii_NumIndexKeyAttrs as usize;

    let mut indkey_data = [0u8; 2 * types_core::INDEX_MAX_KEYS as usize];
    for i in 0..natts {
        indkey_data[i * 2..i * 2 + 2]
            .copy_from_slice(&indexInfo.ii_IndexAttrNumbers[i].to_ne_bytes());
    }
    let indkey = build_vector_datum(mcx, INT2OID, 2, &indkey_data[..natts * 2], natts)?;
    let mut coll_data = [0u8; 4 * types_core::INDEX_MAX_KEYS as usize];
    let mut class_data = [0u8; 4 * types_core::INDEX_MAX_KEYS as usize];
    let mut opt_data = [0u8; 2 * types_core::INDEX_MAX_KEYS as usize];
    for i in 0..nkeyatts {
        coll_data[i * 4..i * 4 + 4].copy_from_slice(&collationOids[i].to_ne_bytes());
        class_data[i * 4..i * 4 + 4].copy_from_slice(&opclassOids[i].to_ne_bytes());
        opt_data[i * 2..i * 2 + 2].copy_from_slice(&coloptions[i].to_ne_bytes());
    }
    let indcollation = build_vector_datum(mcx, OIDOID, 4, &coll_data[..nkeyatts * 4], nkeyatts)?;
    let indclass = build_vector_datum(mcx, OIDOID, 4, &class_data[..nkeyatts * 4], nkeyatts)?;
    let indoption = build_vector_datum(mcx, INT2OID, 2, &opt_data[..nkeyatts * 2], nkeyatts)?;

    let exprs_text = if indexInfo.ii_Expressions.is_nil() {
        None
    } else {
        let list = types_nodes::Node::mk_list(mcx, indexInfo.ii_Expressions.clone_in(mcx)?)?;
        Some(varlena::cstring_to_text(
            mcx,
            outfuncs::nodeToString(mcx, list)?.as_bytes(),
        )?)
    };
    let pred_text = if indexInfo.ii_Predicate.is_nil() {
        None
    } else {
        let pred = clauses::make_ands_explicit(mcx, &indexInfo.ii_Predicate)?;
        Some(varlena::cstring_to_text(
            mcx,
            outfuncs::nodeToString(mcx, pred)?.as_bytes(),
        )?)
    };

    let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;

    let mut values = [Datum::null(); Natts_pg_index];
    let mut nulls = [false; Natts_pg_index];
    values[0] = Datum::from_oid(indexoid);
    values[1] = Datum::from_oid(heapoid);
    values[2] = Datum::from_i16(indexInfo.ii_NumIndexAttrs as i16);
    values[3] = Datum::from_i16(indexInfo.ii_NumIndexKeyAttrs as i16);
    values[4] = Datum::from_bool(indexInfo.ii_Unique);
    values[5] = Datum::from_bool(indexInfo.ii_NullsNotDistinct);
    values[6] = Datum::from_bool(primary);
    values[7] = Datum::from_bool(isexclusion);
    values[8] = Datum::from_bool(immediate);
    values[9] = Datum::from_bool(false); // indisclustered
    values[10] = Datum::from_bool(isvalid);
    values[11] = Datum::from_bool(false); // indcheckxmin
    values[12] = Datum::from_bool(isready);
    values[13] = Datum::from_bool(true); // indislive
    values[14] = Datum::from_bool(false); // indisreplident
    values[15] = Datum::from_usize(indkey.as_ptr() as usize);
    values[16] = Datum::from_usize(indcollation.as_ptr() as usize);
    values[17] = Datum::from_usize(indclass.as_ptr() as usize);
    values[18] = Datum::from_usize(indoption.as_ptr() as usize);
    match &exprs_text {
        Some(t) => values[19] = Datum::from_usize(t.as_bytes().as_ptr() as usize),
        None => nulls[19] = true,
    }
    match &pred_text {
        Some(t) => values[20] = Datum::from_usize(t.as_bytes().as_ptr() as usize),
        None => nulls[20] = true,
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_index.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_index, &mut tup)?;
    pg_index.close(RowExclusiveLock)
}

// index_opclass_options (indexam.c:1043). Lives here rather than the indexam
// crate: the no-options-proc error needs syscache-backed opclass naming, and
// the syscache sits above indexam in the crate graph.
pub fn index_opclass_options<'mcx>(
    mcx: Mcx<'mcx>,
    indrel: &Relation<'mcx>,
    attnum: AttrNumber,
    attoptions: Datum,
    validate: bool,
) -> PgResult<Option<Vec<u8>>> {
    let kind = types_relscan::IndexAmKind::from_relam(indrel.rd_rel.relam);
    let amoptsprocnum = kind.amoptsprocnum() as usize;
    let amsupport = kind.amsupport() as usize;
    // index_getprocid over the rd_support preload (nkey x amsupport, row-major).
    // amoptsprocnum == 0: the AM has no opclass-options proc (hnsw).
    let procid = if amoptsprocnum == 0 {
        InvalidOid
    } else {
        indrel
            .rd_support
            .get((attnum as usize - 1) * amsupport + (amoptsprocnum - 1))
            .copied()
            .unwrap_or(InvalidOid)
    };
    if procid == InvalidOid {
        if attoptions == Datum::null() {
            return Ok(None);
        }
        let opclass = lsyscache::get_index_column_opclass(indrel.rd_id, attnum as i32)?;
        return Err(err(
            format!(
                "operator class {} has no options",
                ruleutils::generate_opclass_name(mcx, opclass)?
            ),
            types_error::ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }
    let mut relopts = reloptions::LocalRelopts::new();
    let mut finfo = fmgr_seams::fmgr_info::call(procid)?;
    let mut frame = types_fmgr::LocalFcinfo::<1>::new(InvalidOid);
    // C PG_GETARG_POINTER protocol: arg 0 carries &mut LocalRelopts.
    frame.set_arg(
        0,
        Datum::from_usize(&mut relopts as *mut reloptions::LocalRelopts as usize),
    );
    finfo.invoke(&mut frame)?;
    Ok(Some(reloptions::build_local_reloptions(
        mcx, &relopts, attoptions, validate,
    )?))
}

// RelationGetIndexAttOptions (relcache.c): parsed per-key-column opclass
// options, cached on the relcache entry (C rd_opcoptions).
pub fn relation_get_index_att_options(
    rel: &Relation<'_>,
) -> PgResult<std::rc::Rc<[Option<Box<[u8]>>]>> {
    const Anum_pg_attribute_attoptions: i32 = 25;
    if let Some(cached) = rel.rd_opcoptions.borrow().as_ref() {
        return Ok(cached.clone());
    }
    let owner = mcx::MemoryContext::new("RelationGetIndexAttOptions");
    let mcx = owner.mcx();
    let natts = rel.indnkeyatts() as usize;
    let mut opts: Vec<Option<Box<[u8]>>> = Vec::with_capacity(natts);
    for attnum in 1..=natts as AttrNumber {
        let Some(tuple) = cache_syscache::SearchSysCacheAttNum(rel.rd_id, attnum as i16)? else {
            return Err(PgError::error(format!(
                "cache lookup failed for attribute {attnum} of relation {}",
                rel.rd_id
            ))
            .into());
        };
        let (d, isnull) =
            cache_syscache::SysCacheGetAttr(cache_syscache::ATTNUM, &tuple, Anum_pg_attribute_attoptions)?;
        let built = index_opclass_options(
            mcx,
            rel,
            attnum,
            if isnull { Datum::null() } else { d },
            false,
        )?;
        cache_syscache::ReleaseSysCache(tuple);
        opts.push(built.map(|v| v.into_boxed_slice()));
    }
    let rc: std::rc::Rc<[Option<Box<[u8]>>]> = opts.into();
    *rel.rd_opcoptions.borrow_mut() = Some(rc.clone());
    Ok(rc)
}

pub struct IndexCreateExtra<'a> {
    pub flags: u16,
    pub constr_flags: u16,
    pub allow_system_table_mods: bool,
    pub is_internal: bool,
    pub parent_index_relid: Oid,
    pub parent_constraint_id: Oid,
    pub reloptions: Option<&'a [u8]>,
    pub opclass_options: Option<&'a [Datum]>,
    pub stattargets: Option<&'a [datum::NullableDatum]>,
    // C relFileNumber: valid means adopt this existing storage (TryReuseIndex).
    pub old_number: types_core::RelFileNumber,
}

// index_create; relFileNumber/opclassOptions/stattargets fixed at their
// toast-lane values. Returns (index oid, constraint oid or InvalidOid).
#[allow(clippy::too_many_arguments)]
pub fn index_create<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelationName: &str,
    indexRelationId: Oid,
    indexInfo: &mut IndexInfo<'mcx>,
    indexColNames: &[&str],
    accessMethodId: Oid,
    tableSpaceId: Oid,
    collationIds: &[Oid],
    opclassIds: &[Oid],
    coloptions: &[i16],
    extra: &IndexCreateExtra<'_>,
) -> PgResult<(Oid, Oid)> {
    let heapRelationId = heapRelation.rd_id;
    let concurrent = extra.flags & INDEX_CREATE_CONCURRENT != 0;
    let invalid = extra.flags & INDEX_CREATE_INVALID != 0;
    let isprimary = extra.flags & INDEX_CREATE_IS_PRIMARY != 0;
    let partitioned = extra.flags & INDEX_CREATE_PARTITIONED != 0;
    let skip_build = extra.flags & INDEX_CREATE_SKIP_BUILD != 0;
    let parentIndexRelid = extra.parent_index_relid;

    if concurrent && catalog::IsCatalogRelation(heapRelation) {
        return Err(err(
            "concurrent index creation on system catalog tables is not supported".into(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    if concurrent && indexInfo.ii_HasExclusion {
        return Err(err(
            "concurrent index creation for exclusion constraints is not supported".into(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    assert!(!partitioned || skip_build, "partitioned indexes must never be built");
    assert!(
        extra.constr_flags == 0 || extra.flags & INDEX_CREATE_ADD_CONSTRAINT != 0,
        "constr_flags without INDEX_CREATE_ADD_CONSTRAINT"
    );
    let relkind = if partitioned { types_rel::RELKIND_PARTITIONED_INDEX } else { RELKIND_INDEX };
    if extra.constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS != 0 {
        unported("index_create: existing-index constraint flag");
    }

    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    let namespaceId = heapRelation.rd_rel.relnamespace;
    if heapRelation.rd_rel.relisshared {
        unported("index_create: shared relations");
    }
    let relpersistence = heapRelation.rd_rel.relpersistence;

    if indexInfo.ii_NumIndexAttrs < 1 {
        panic!("must index at least one column");
    }
    if !extra.allow_system_table_mods
        && catalog::IsSystemRelation(heapRelation)
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        return Err(err(
            "user-defined indexes on system catalog tables are not supported".to_string(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }
    for i in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
        // TEXT/VARCHAR/BPCHAR_BTREE_PATTERN_OPS_OID (pg_opclass.dat; names
        // are pinned catalog rows, so the CLAOID probe C does is a constant).
        if collationIds[i] != InvalidOid
            && matches!(opclassIds[i], 4217 | 4218 | 4219)
            && !lsyscache::get_collation_isdeterministic(collationIds[i])?
        {
            let opcname = match opclassIds[i] {
                4217 => "text_pattern_ops",
                4218 => "varchar_pattern_ops",
                _ => "bpchar_pattern_ops",
            };
            return Err(err(
                format!(
                    "nondeterministic collations are not supported for operator class \"{opcname}\""
                ),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
    }

    if lsyscache::get_relname_relid(indexRelationName, namespaceId)? != InvalidOid {
        if extra.flags & INDEX_CREATE_IF_NOT_EXISTS != 0 {
            elog_seams::ereport_msg::call(
                types_error::NOTICE,
                format!("relation \"{indexRelationName}\" already exists, skipping"),
                None,
            )?;
            pg_class.close(RowExclusiveLock)?;
            return Ok((InvalidOid, InvalidOid));
        }
        return Err(err(
            format!("relation \"{indexRelationName}\" already exists"),
            ERRCODE_DUPLICATE_TABLE,
        ));
    }

    if extra.flags & INDEX_CREATE_ADD_CONSTRAINT != 0
        && pg_constraint::ConstraintNameIsUsed(
            mcx,
            pg_constraint::ConstraintCategory::Relation,
            heapRelationId,
            indexRelationName,
        )?
    {
        return Err(err(
            format!(
                "constraint \"{indexRelationName}\" for relation \"{}\" already exists",
                heapRelation.name()
            ),
            types_error::ERRCODE_DUPLICATE_OBJECT,
        ));
    }

    let mut indexTupDesc = ConstructTupleDescriptor(
        mcx,
        heapRelation,
        indexInfo,
        indexColNames,
        accessMethodId,
        collationIds,
        opclassIds,
    )?;

    let indexRelationId = if indexRelationId != InvalidOid {
        indexRelationId
    } else {
        catalog::GetNewRelFileNumber(mcx, tableSpaceId, Some(&pg_class), relpersistence)?
    };

    // InitializeAttributeOids runs on the pre-copy descriptor; the relcache
    // copy in heap_create then carries attrelid from the start (C fixes the
    // copy up after the fact — same rows reach pg_attribute).
    for i in 0..indexInfo.ii_NumIndexAttrs as usize {
        indexTupDesc.attr_mut(i).attrelid = indexRelationId;
    }

    let (indexRelation, relfrozenxid, relminmxid) = catalog_heap::heap_create(
        mcx,
        indexRelationName,
        namespaceId,
        tableSpaceId,
        indexRelationId,
        InvalidOid,
        extra.old_number,
        accessMethodId,
        &indexTupDesc,
        relkind,
        relpersistence,
        // mapped_relation = RelationIsMapped(heapRelation) (index.c:786);
        // indexes on mapped catalogs are themselves mapped.
        heapRelation.is_mapped(),
        extra.allow_system_table_mods,
    )?;
    debug_assert!(relfrozenxid == 0 && relminmxid == 0);

    lmgr::LockRelationOid(indexRelationId, AccessExclusiveLock)?;

    let mut form = indexRelation.rd_rel.clone();
    form.relowner = heapRelation.rd_rel.relowner;
    form.relam = accessMethodId;
    form.relispartition = parentIndexRelid != InvalidOid;
    catalog_heap::InsertPgClassTuple(
        mcx,
        &pg_class,
        &form,
        indexTupDesc.natts as i16,
        indexRelationId,
        InvalidOid,
        None,
        extra.reloptions,
    )?;
    pg_class.close(RowExclusiveLock)?;

    // AppendAttributeTuples.
    {
        let natts = indexTupDesc.natts as usize;
        let mut attrs_extra: mcx::PgVec<'_, catalog_heap::FormExtraData_pg_attribute> =
            mcx::PgVec::new_in(mcx);
        if let Some(attopts) = extra.opclass_options {
            for i in 0..natts {
                attrs_extra.push(catalog_heap::FormExtraData_pg_attribute {
                    attoptions: if attopts[i].as_usize() != 0 {
                        datum::NullableDatum::value(attopts[i])
                    } else {
                        datum::NullableDatum::null()
                    },
                    attstattarget: match extra.stattargets {
                        Some(st) => st[i],
                        None => datum::NullableDatum::null(),
                    },
                });
            }
        }
        let pg_attribute = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
        let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &pg_attribute)?;
        catalog_heap::create::InsertPgAttributeTuples(
            mcx,
            &pg_attribute,
            &indexTupDesc.attrs[..natts],
            indexRelationId,
            if attrs_extra.is_empty() { None } else { Some(&attrs_extra[..]) },
            Some(&mut indstate),
        )?;
        catalog_indexing::CatalogCloseIndexes(indstate)?;
        pg_attribute.close(RowExclusiveLock)?;
    }

    UpdateIndexRelation(
        mcx,
        indexRelationId,
        heapRelationId,
        indexInfo,
        collationIds,
        opclassIds,
        coloptions,
        isprimary,
        indexInfo.ii_HasExclusion,
        extra.constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE == 0,
        !concurrent && !invalid,
        !concurrent,
    )?;

    inval::invalidate::CacheInvalidateRelcache(heapRelation)?;

    if parentIndexRelid != InvalidOid {
        pg_inherits::StoreSingleInheritance(mcx, indexRelationId, parentIndexRelid, 1)?;
        lmgr::LockRelationOid(parentIndexRelid, types_rel::ShareUpdateExclusiveLock)?;
        tablecmds_seams::set_relation_has_subclass::call(mcx, parentIndexRelid, true)?;
    }

    let mut constraintId = InvalidOid;
    if !miscinit_seams::is_bootstrap_processing_mode::call() {
        let myself = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, indexRelationId);
        if extra.flags & INDEX_CREATE_ADD_CONSTRAINT != 0 {
            let constraint_type = if isprimary {
                pg_constraint::CONSTRAINT_PRIMARY
            } else if indexInfo.ii_Unique {
                pg_constraint::CONSTRAINT_UNIQUE
            } else if indexInfo.ii_HasExclusion {
                pg_constraint::CONSTRAINT_EXCLUSION
            } else {
                panic!("constraint must be PRIMARY, UNIQUE or EXCLUDE");
            };
            constraintId = index_constraint_create(
                mcx,
                heapRelation,
                indexRelationId,
                extra.parent_constraint_id,
                indexInfo,
                indexRelationName,
                constraint_type,
                extra.constr_flags,
                extra.allow_system_table_mods,
            )?;
        } else {
            let mut addrs: mcx::PgVec<'_, pg_depend::ObjectAddress> = mcx::PgVec::new_in(mcx);
            let mut have_simple_col = false;
            for i in 0..indexInfo.ii_NumIndexAttrs as usize {
                if indexInfo.ii_IndexAttrNumbers[i] != 0 {
                    addrs.push(pg_depend::ObjectAddress::sub_set(
                        RELATION_RELATION_ID,
                        heapRelationId,
                        indexInfo.ii_IndexAttrNumbers[i] as i32,
                    ));
                    have_simple_col = true;
                }
            }
            if !have_simple_col {
                addrs.push(pg_depend::ObjectAddress::set(RELATION_RELATION_ID, heapRelationId));
            }
            pg_depend::record_object_address_dependencies(
                mcx,
                &myself,
                &mut addrs,
                pg_depend::DependencyType::Auto,
            )?;
        }

        // Partition deps are in addition to, not instead of, the ones above.
        if parentIndexRelid != InvalidOid {
            let parent = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, parentIndexRelid);
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &parent,
                pg_depend::DependencyType::PartitionPri,
            )?;
            let heap = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, heapRelationId);
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &heap,
                pg_depend::DependencyType::PartitionSec,
            )?;
        }

        let mut normals: mcx::PgVec<'_, pg_depend::ObjectAddress> = mcx::PgVec::new_in(mcx);
        for i in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
            if collationIds[i] != InvalidOid && collationIds[i] != DEFAULT_COLLATION_OID {
                normals.push(pg_depend::ObjectAddress::set(
                    catalog::CollationRelationId,
                    collationIds[i],
                ));
            }
        }
        for i in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
            normals.push(pg_depend::ObjectAddress::set(
                catalog::OperatorClassRelationId,
                opclassIds[i],
            ));
        }
        pg_depend::record_object_address_dependencies(
            mcx,
            &myself,
            &mut normals,
            pg_depend::DependencyType::Normal,
        )?;

        if !indexInfo.ii_Expressions.is_nil() {
            let exprs = types_nodes::Node::mk_list(mcx, indexInfo.ii_Expressions.clone_in(mcx)?)?;
            pg_depend::recordDependencyOnSingleRelExpr(
                mcx,
                &myself,
                exprs,
                heapRelationId,
                pg_depend::DependencyType::Normal,
                pg_depend::DependencyType::Auto,
                false,
            )?;
        }
        if !indexInfo.ii_Predicate.is_nil() {
            let pred = types_nodes::Node::mk_list(mcx, indexInfo.ii_Predicate.clone_in(mcx)?)?;
            pg_depend::recordDependencyOnSingleRelExpr(
                mcx,
                &myself,
                pred,
                heapRelationId,
                pg_depend::DependencyType::Normal,
                pg_depend::DependencyType::Auto,
                false,
            )?;
        }
    } else {
        unported("index_create: bootstrap-mode index_register");
    }

    xact::CommandCounterIncrement()?;

    // Validate opclass-specific options (index.c:1243-1248).
    if let Some(attopts) = extra.opclass_options {
        let irel = indexam::index_open(mcx, indexRelationId, NoLock)?;
        for i in 0..indexInfo.ii_NumIndexKeyAttrs as usize {
            index_opclass_options(mcx, &irel, (i + 1) as AttrNumber, attopts[i], true)?;
        }
        indexam::index_close(irel, NoLock)?;
    }

    if skip_build {
        // The heap must still be marked as indexed; the caller fills the
        // index later (partitioned indexes never are).
        index_update_stats(mcx, heapRelation, true, -1.0)?;
        xact::CommandCounterIncrement()?;
        drop(indexRelation);
        return Ok((indexRelationId, constraintId));
    }

    // The relcache entry was rebuilt from the catalogs at CCI; reopen to get
    // the index-access fields (C keeps the same pointer, rebuilt in place).
    drop(indexRelation);
    let indexRelation = indexam::index_open(mcx, indexRelationId, NoLock)?;

    index_build(mcx, heapRelation, &indexRelation, indexInfo, false)?;

    indexam::index_close(indexRelation, NoLock)?;
    Ok((indexRelationId, constraintId))
}

// index_constraint_create (index.c).
#[allow(clippy::too_many_arguments)]
pub fn index_constraint_create<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelationId: Oid,
    parentConstraintId: Oid,
    indexInfo: &IndexInfo<'mcx>,
    constraintName: &str,
    constraintType: u8,
    constr_flags: u16,
    allow_system_table_mods: bool,
) -> PgResult<Oid> {
    let namespaceId = heapRelation.rd_rel.relnamespace;
    let is_without_overlaps = constr_flags & INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS != 0;
    let deferrable = constr_flags & INDEX_CONSTR_CREATE_DEFERRABLE != 0;
    let initdeferred = constr_flags & INDEX_CONSTR_CREATE_INIT_DEFERRED != 0;
    debug_assert!(!initdeferred || deferrable);
    if !indexInfo.ii_Expressions.is_nil()
        && constraintType != pg_constraint::CONSTRAINT_EXCLUSION
    {
        panic!("constraints cannot have index expressions");
    }
    if constr_flags & INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS != 0 {
        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            RELATION_RELATION_ID,
            indexRelationId,
            RELATION_RELATION_ID,
            pg_depend::DependencyType::Auto,
        )?;
    }
    if !allow_system_table_mods
        && catalog::IsSystemRelation(heapRelation)
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        return Err(err(
            "user-defined indexes on system catalog tables are not supported".to_string(),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    let mut entry = pg_constraint::ConstraintEntry::base(
        constraintName,
        namespaceId,
        constraintType,
        heapRelation.rd_id,
    );
    entry.conkey = &indexInfo.ii_IndexAttrNumbers[..indexInfo.ii_NumIndexAttrs as usize];
    entry.n_keys = indexInfo.ii_NumIndexKeyAttrs as usize;
    entry.index_relid = indexRelationId;
    entry.deferrable = deferrable;
    entry.deferred = initdeferred;
    if indexInfo.ii_HasExclusion {
        entry.excl_op = &indexInfo.ii_ExclusionOps[..indexInfo.ii_NumIndexKeyAttrs as usize];
    }
    entry.con_period = is_without_overlaps;
    if parentConstraintId != InvalidOid {
        entry.parent_constr_id = parentConstraintId;
        entry.is_local = false;
        entry.inhcount = 1;
        entry.is_no_inherit = false;
    } else {
        entry.is_no_inherit = true;
    }
    let con_oid = pg_constraint::CreateConstraintEntry(mcx, &entry)?;

    let myself = pg_depend::ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, con_oid);
    let idxaddr = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, indexRelationId);
    pg_depend::recordDependencyOn(mcx, &idxaddr, &myself, pg_depend::DependencyType::Internal)?;

    if parentConstraintId != InvalidOid {
        let parent =
            pg_depend::ObjectAddress::set(types_core::CONSTRAINT_RELATION_ID, parentConstraintId);
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &parent,
            pg_depend::DependencyType::PartitionPri,
        )?;
        let tbl = pg_depend::ObjectAddress::set(RELATION_RELATION_ID, heapRelation.rd_id);
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &tbl,
            pg_depend::DependencyType::PartitionSec,
        )?;
    }

    let mark_as_primary = constr_flags & INDEX_CONSTR_CREATE_MARK_AS_PRIMARY != 0;
    if constr_flags & INDEX_CONSTR_CREATE_UPDATE_INDEX != 0 && (mark_as_primary || deferrable) {
        let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
        let key = oid_scankey(1, indexRelationId);
        let mut scan =
            genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &[key])?;
        let tup = genam::systable_getnext(mcx, &mut scan)?
            .unwrap_or_else(|| panic!("cache lookup failed for index {indexRelationId}"));
        let desc = pg_index.descr();
        let isprimary = getattr(tup, Anum_pg_index_indisprimary, desc).as_bool();
        let isimmediate = getattr(tup, Anum_pg_index_indimmediate, desc).as_bool();
        let set_primary = mark_as_primary && !isprimary;
        let set_deferred = deferrable && isimmediate;
        if set_primary || set_deferred {
            let natts = desc.natts as usize;
            let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut nulls: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
            values.resize(natts, Datum::null());
            nulls.resize(natts, false);
            replace.resize(natts, false);
            if set_primary {
                values[Anum_pg_index_indisprimary - 1] = Datum::from_bool(true);
                replace[Anum_pg_index_indisprimary - 1] = true;
            }
            if set_deferred {
                values[Anum_pg_index_indimmediate - 1] = Datum::from_bool(false);
                replace[Anum_pg_index_indimmediate - 1] = true;
            }
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &nulls, &replace)?;
            let otid = tup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &otid, &mut newtup)?;
            // Marking an existing index primary must flush the parent
            // table's relcache entry (replication behavior depends on it).
            if set_primary {
                inval::invalidate::CacheInvalidateRelcacheByRelid(heapRelation.rd_id)?;
            }
        } else {
            genam::systable_endscan(mcx, scan)?;
        }
        pg_index.close(RowExclusiveLock)?;
    }

    if deferrable {
        const F_UNIQUE_KEY_RECHECK: Oid = 1250;
        trigger::CreateTriggerInternal(
            mcx,
            &trigger::InternalTriggerArgs {
                trigname_base: if constraintType == pg_constraint::CONSTRAINT_PRIMARY {
                    "PK_ConstraintTrigger"
                } else {
                    "Unique_ConstraintTrigger"
                },
                relid: heapRelation.rd_id,
                constrrelid: InvalidOid,
                constraint_oid: con_oid,
                index_oid: indexRelationId,
                funcoid: F_UNIQUE_KEY_RECHECK,
                tgtype: types_trigger::TRIGGER_TYPE_ROW
                    | types_trigger::TRIGGER_TYPE_INSERT
                    | types_trigger::TRIGGER_TYPE_UPDATE,
                deferrable: true,
                initdeferred,
                parent_trigger_oid: InvalidOid,
            },
        )?;
    }
    Ok(con_oid)
}

// index_build: btree only, serial only (C divergence: plan_create_index_workers
// is not consulted — every build runs with ii_ParallelWorkers = 0; C picks the
// same for tables under min_parallel_table_scan_size).
pub fn index_build<'mcx>(
    mcx: Mcx<'mcx>,
    heapRelation: &Relation<'mcx>,
    indexRelation: &Relation<'mcx>,
    indexInfo: &mut IndexInfo<'mcx>,
    isreindex: bool,
) -> PgResult<()> {
    let am_kind = types_relscan::IndexAmKind::from_relam(indexRelation.rd_rel.relam);

    let guard = miscinit::SecContextGuard::security_restricted(heapRelation.rd_rel.relowner);
    let save_nestlevel = guc::NewGUCNestLevel();
    guc::RestrictSearchPath()?;

    // C index.c:3033-3043: report which build path was chosen. Every pgrust
    // build is serial (ii_ParallelWorkers == 0 always — see the header
    // comment), so only C's "serially" branch is reachable; the
    // "with request for %d parallel workers" branch belongs to the
    // unported parallel-build path. errmsg_internal: not translated.
    elog_seams::ereport::call(
        PgError::new(
            types_error::DEBUG1,
            format!(
                "building index \"{}\" on table \"{}\" serially",
                indexRelation.name(),
                heapRelation.name()
            ),
        )
        .with_location("index.c", 3034, "index_build"),
    )?;

    let stats = match am_kind {
        types_relscan::IndexAmKind::Btree => {
            let r = nbtsort::btbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Hash => {
            let r = hashsort::hashbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Brin => {
            let r = brin_build::brinbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Spgist => {
            let r = spgist_build::spgbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Gin => {
            let r = ginbuild::ginbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Hnsw => {
            let r = pgvector_hnsw_build::hnswbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        types_relscan::IndexAmKind::Bloom => {
            let r = bloom_build::blbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
        _ => {
            let r = gistbuild::gistbuild(mcx, heapRelation, indexRelation, indexInfo)?;
            (r.heap_tuples, r.index_tuples)
        }
    };

    if indexRelation.rd_rel.relpersistence == types_core::RELPERSISTENCE_UNLOGGED {
        let key = types_storage::RelFileLocatorBackend {
            locator: indexRelation.rd_locator.get(),
            backend: indexRelation.rd_backend,
        };
        smgr::smgropen(key.locator, key.backend)?;
        if !smgr::smgrexists(key, ForkNumber::INIT_FORKNUM)? {
            smgr::smgrcreate(key, ForkNumber::INIT_FORKNUM, false)?;
            catalog_storage::log_smgrcreate(&key.locator, ForkNumber::INIT_FORKNUM)?;
            match am_kind {
                types_relscan::IndexAmKind::Btree => nbtsort::btbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Hash => hashsort::hashbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Gin => ginbuild::ginbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Gist => gistbuild::gistbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Spgist => spgist_build::spgbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Brin => brin_build::brinbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Hnsw => pgvector_hnsw_build::hnswbuildempty(indexRelation)?,
                types_relscan::IndexAmKind::Bloom => bloom_build::blbuildempty(indexRelation)?,
                #[allow(unreachable_patterns)]
                other => unported(&format!("index_build: ambuildempty for AM {other:?}")),
            }
        }
    }

    if indexInfo.ii_BrokenHotChain && !isreindex && !indexInfo.ii_Concurrent {
        set_indcheckxmin(mcx, indexRelation.rd_id)?;
    }

    index_update_stats(mcx, heapRelation, true, stats.0)?;
    index_update_stats(mcx, indexRelation, false, stats.1)?;

    xact::CommandCounterIncrement()?;

    if indexInfo.ii_HasExclusion {
        execindexing::IndexCheckExclusion(mcx, heapRelation, indexRelation, indexInfo)?;
    }

    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();
    Ok(())
}

// index.c:3125 broken-HOT-chain arm: flip pg_index.indcheckxmin in place.
fn set_indcheckxmin<'mcx>(mcx: Mcx<'mcx>, indexId: Oid) -> PgResult<()> {
    const Anum_pg_index_indcheckxmin: usize = 12;
    let pg_index = table::table_open(mcx, INDEX_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(1, indexId);
    let mut scan = genam::systable_beginscan(mcx, &pg_index, IndexRelidIndexId, true, None, &[key])?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for index {indexId}"));
    let desc = pg_index.descr();
    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[Anum_pg_index_indcheckxmin - 1] = Datum::from_bool(true);
    replace[Anum_pg_index_indcheckxmin - 1] = true;
    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let tid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_index, &tid, &mut newtup)?;
    pg_index.close(RowExclusiveLock)
}

// index_update_stats (non-transactional inplace pg_class update).
fn index_update_stats<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    hasindex: bool,
    mut reltuples: f64,
) -> PgResult<()> {
    let relid = rel.rd_id;

    if reltuples == 0.0 && rel.rd_rel.reltuples < 0.0 {
        reltuples = -1.0;
    }

    let mut update_stats = reltuples >= 0.0;

    if matches!(
        rel.rd_rel.relkind,
        RELKIND_RELATION | RELKIND_TOASTVALUE | RELKIND_MATVIEW
    ) {
        if autovacuum_seams::autovacuuming_active::call() {
            if rel.rd_options.as_ref().and_then(|o| o.std()).is_some_and(|o| !o.autovacuum.enabled)
            {
                update_stats = false;
            }
        } else {
            update_stats = false;
        }
    }

    let (mut relpages, mut relallvisible, mut relallfrozen) = (0u32, 0u32, 0u32);
    if update_stats {
        relpages = bufmgr::RelationGetNumberOfBlocksInFork(rel, ForkNumber::MAIN_FORKNUM)?;
        if rel.rd_rel.relkind != RELKIND_INDEX {
            let counts = visibilitymap::visibilitymap_count(rel)?;
            relallvisible = counts.0;
            relallfrozen = counts.1;
        }
    }

    let pg_class = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;
    let key = oid_scankey(Anum_pg_class_oid, relid);
    let Some((ctup, inplace_state)) = genam::systable_inplace_update_begin(
        mcx,
        &pg_class,
        catalog::ClassOidIndexId,
        true,
        &[key],
    )?
    else {
        panic!("could not find tuple for relation {relid}");
    };

    let desc = pg_class.descr();
    let old = ctup.as_tuple();
    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    let mut dirty = false;
    let set = |anum: usize, d: Datum, values: &mut mcx::PgVec<'_, Datum>, replace: &mut mcx::PgVec<'_, bool>, dirty: &mut bool| {
        values[anum - 1] = d;
        replace[anum - 1] = true;
        *dirty = true;
    };

    if getattr(old, Anum_pg_class_relhasindex, desc).as_bool() != hasindex {
        set(Anum_pg_class_relhasindex, Datum::from_bool(hasindex), &mut values, &mut replace, &mut dirty);
    }
    if update_stats {
        if getattr(old, Anum_pg_class_relpages, desc).as_i32() != relpages as i32 {
            set(Anum_pg_class_relpages, Datum::from_i32(relpages as i32), &mut values, &mut replace, &mut dirty);
        }
        if getattr(old, Anum_pg_class_reltuples, desc).as_f32() != reltuples as f32 {
            set(Anum_pg_class_reltuples, Datum::from_f32(reltuples as f32), &mut values, &mut replace, &mut dirty);
        }
        if getattr(old, Anum_pg_class_relallvisible, desc).as_i32() != relallvisible as i32 {
            set(Anum_pg_class_relallvisible, Datum::from_i32(relallvisible as i32), &mut values, &mut replace, &mut dirty);
        }
        if getattr(old, Anum_pg_class_relallfrozen, desc).as_i32() != relallfrozen as i32 {
            set(Anum_pg_class_relallfrozen, Datum::from_i32(relallfrozen as i32), &mut values, &mut replace, &mut dirty);
        }
    }

    if dirty {
        let newtup = heaptuple::heap_modify_tuple(mcx, old, desc, &values, &isnull, &replace)?;
        genam::systable_inplace_update_finish(mcx, inplace_state, newtup.as_tuple())?;
    } else {
        genam::systable_inplace_update_cancel(mcx, inplace_state)?;
        inval::invalidate::CacheInvalidateRelcacheByTuple(old)?;
    }

    pg_class.close(RowExclusiveLock)
}

// CompareIndexInfo (index.c); AM identity and per-key collation/opfamily come
// from the two open index relations, attmap maps rel2's table attnos to
// rel1's (build_attrmap_by_name shape).
pub fn CompareIndexInfo<'mcx>(
    mcx: Mcx<'mcx>,
    info1: &IndexInfo<'mcx>,
    info2: &IndexInfo<'mcx>,
    rel1: &Relation<'_>,
    rel2: &Relation<'_>,
    attmap: &[AttrNumber],
) -> PgResult<bool> {
    if info1.ii_Unique != info2.ii_Unique
        || info1.ii_NullsNotDistinct != info2.ii_NullsNotDistinct
        || rel1.rd_rel.relam != rel2.rd_rel.relam
        || info1.ii_NumIndexAttrs != info2.ii_NumIndexAttrs
        || info1.ii_NumIndexKeyAttrs != info2.ii_NumIndexKeyAttrs
    {
        return Ok(false);
    }
    for i in 0..info1.ii_NumIndexAttrs as usize {
        if (attmap.len() as i32) < info2.ii_IndexAttrNumbers[i] as i32 {
            panic!("incorrect attribute map");
        }
        if !(info1.ii_IndexAttrNumbers[i] == 0 && info2.ii_IndexAttrNumbers[i] == 0) {
            if info1.ii_IndexAttrNumbers[i] == 0 || info2.ii_IndexAttrNumbers[i] == 0 {
                return Ok(false);
            }
            if attmap[info2.ii_IndexAttrNumbers[i] as usize - 1]
                != info1.ii_IndexAttrNumbers[i]
            {
                return Ok(false);
            }
        }
        if i >= info1.ii_NumIndexKeyAttrs as usize {
            continue;
        }
        if rel1.rd_indcollation[i] != rel2.rd_indcollation[i]
            || rel1.rd_opfamily[i] != rel2.rd_opfamily[i]
        {
            return Ok(false);
        }
    }

    let map_list_equal = |l1: &types_nodes::NodeList<'mcx>,
                          l2: &types_nodes::NodeList<'mcx>|
     -> PgResult<bool> {
        if l1.is_nil() != l2.is_nil() {
            return Ok(false);
        }
        if l1.is_nil() {
            return Ok(true);
        }
        if l1.len() != l2.len() {
            return Ok(false);
        }
        for (e1, e2) in l1.iter().zip(l2.iter()) {
            let (mapped, found_whole_row) =
                rewrite_manip::map_variable_attnos(mcx, e2, 1, 0, attmap, types_core::InvalidOid)?;
            if found_whole_row || !types_nodes::equal::equal(e1, mapped) {
                return Ok(false);
            }
        }
        Ok(true)
    };
    if !map_list_equal(&info1.ii_Expressions, &info2.ii_Expressions)? {
        return Ok(false);
    }
    if !map_list_equal(&info1.ii_Predicate, &info2.ii_Predicate)? {
        return Ok(false);
    }
    // C: no support currently for comparing exclusion indexes.
    if info1.ii_HasExclusion || info2.ii_HasExclusion {
        return Ok(false);
    }
    Ok(true)
}

fn ResetReindexState(nest_level: i32) {
    types_rel::reindex::reset_reindex_state(nest_level)
}

fn index_build_dummy<'mcx>(
    mcx: Mcx<'mcx>,
    heap_relation: &Relation<'mcx>,
    index_relation: &Relation<'mcx>,
    isreindex: bool,
) -> types_error::PgResult<()> {
    let mut indexInfo = execindexing::BuildDummyIndexInfo(mcx, index_relation)?;
    index_build(mcx, heap_relation, index_relation, &mut indexInfo, isreindex)
}

pub fn init_seams() {
    catalog_index_seams::reset_reindex_state::set(ResetReindexState);
    catalog_index_seams::index_build_dummy::set(index_build_dummy);
    indexam_seams::relation_get_index_att_options::set(relation_get_index_att_options);
    indexam_seams::index_expression_input_type::set(index_expression_input_type);
    indexam_seams::get_func_rettype::set(lsyscache::get_func_rettype);
}

// GetIndexInputType's expression-column arm (spgutils.c):
// getBaseType(exprType(<indexcol's rd_indexprs entry>)). The expression
// trees live only as long as the scratch context; only the Oid escapes.
fn index_expression_input_type(
    rel: &Relation<'_>,
    indexcol_0based: usize,
) -> types_error::PgResult<types_core::Oid> {
    let form = rel.rd_index.as_ref().expect("index relation without rd_index");
    let scratch = mcx::MemoryContext::new("index_expression_input_type");
    let exprs = execindexing::RelationGetIndexExpressions(scratch.mcx(), rel)?;
    let mut expr_iter = exprs.iter();
    for i in 0..form.indnkeyatts as usize {
        if form.indkey[i] == 0 {
            let Some(expr) = expr_iter.next() else {
                break;
            };
            if i == indexcol_0based {
                return lsyscache::typ::getBaseType(nodes_core::expr_type(expr));
            }
        }
    }
    Err(Box::new(types_error::PgError::error("wrong number of index expressions")))
}

pub use drop::{index_drop, IndexGetRelation};
mod drop;
pub use concurrent::{
    index_concurrently_build, index_concurrently_create_copy, index_concurrently_set_dead,
    index_concurrently_swap, index_set_state_flags, validate_index, IndexStateFlagsAction,
};
mod concurrent;
pub use reindex::{
    reindex_index, reindex_relation, RelationSetNewRelfilenumber, ReindexParams,
    REINDEXOPT_CONCURRENTLY, REINDEXOPT_MISSING_OK, REINDEXOPT_REPORT_PROGRESS,
    REINDEXOPT_VERBOSE, REINDEX_REL_CHECK_CONSTRAINTS,
    REINDEX_REL_FORCE_INDEXES_PERMANENT, REINDEX_REL_FORCE_INDEXES_UNLOGGED,
    REINDEX_REL_PROCESS_TOAST, REINDEX_REL_SUPPRESS_INDEX_USE,
};
mod reindex;
