// heap.c DDL half, plain-table lane. Out of scope (loud or WARNING'd below):
// TOAST, typed/partitioned/shared/mapped rels, constraints/defaults,
// pg_shdepend recording, default ACLs.
use std::rc::Rc;

use catalog::AccessMethodRelationId;
use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{
    AttrNumber, InvalidOid, InvalidRelFileNumber, MultiXactId, Oid, TransactionId,
    ATTRIBUTE_RELATION_ID, DEFAULT_COLLATION_OID, RELATION_RELATION_ID, TYPE_RELATION_ID,
};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_DUPLICATE_TABLE, ERROR};
use types_rel::{
    AccessExclusiveLock, AccessShareLock, Relation, RelationData, RowExclusiveLock,
    RELKIND_COMPOSITE_TYPE, RELKIND_HAS_STORAGE, RELKIND_HAS_TABLESPACE, RELKIND_HAS_TABLE_AM,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::{FormData_pg_attribute, TupleDescData, TYPALIGN_DOUBLE, TYPSTORAGE_EXTENDED};

use crate::SysAtt;

const Natts_pg_class: usize = 34;
const Anum_pg_class_relacl: usize = 32;
const Anum_pg_class_reloptions: usize = 33;
const Anum_pg_class_relpartbound: usize = 34;
const Natts_pg_attribute: usize = 25;

#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

pub fn CheckAttributeNamesTypes(tupdesc: &TupleDescData<'_>, relkind: u8) -> PgResult<()> {
    let natts = tupdesc.natts as usize;
    for i in 0..natts {
        let att = &tupdesc.attrs[i];
        let name = core::str::from_utf8(att.attname.name_str()).expect("non-UTF-8 attname");
        if relkind != RELKIND_VIEW
            && relkind != RELKIND_COMPOSITE_TYPE
            && crate::SystemAttributeByName(name).is_some()
        {
            return Err(err(
                format!("column name \"{name}\" conflicts with a system column name"),
                ERRCODE_DUPLICATE_COLUMN,
            ));
        }
        for j in 0..i {
            if tupdesc.attrs[j].attname.name_str() == att.attname.name_str() {
                return Err(err(
                    format!("column name \"{name}\" specified more than once"),
                    ERRCODE_DUPLICATE_COLUMN,
                ));
            }
        }
        if !att.attisdropped && att.atttypid == InvalidOid {
            panic!("CheckAttributeType (heap.c): full type validation unported; got InvalidOid for \"{name}\"");
        }
    }
    Ok(())
}

pub fn heap_create<'mcx>(
    mcx: Mcx<'mcx>,
    relname: &str,
    relnamespace: Oid,
    reltablespace: Oid,
    relid: Oid,
    reltype: Oid,
    relfilenumber: types_core::RelFileNumber,
    accessmtd: Oid,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
    relpersistence: u8,
    allow_system_table_mods: bool,
) -> PgResult<(Rc<RelationData<'static>>, TransactionId, MultiXactId)> {
    if (catalog::IsCatalogNamespace(relnamespace) || catalog::IsToastNamespace(relnamespace))
        && !allow_system_table_mods
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        return Err(err(
            format!("permission denied to create \"{relname}\": system catalog modifications are currently disallowed"),
            types_error::ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let mut reltablespace = reltablespace;
    if !RELKIND_HAS_TABLESPACE(relkind) {
        reltablespace = InvalidOid;
    }
    let mut relfilenumber = relfilenumber;
    let create_storage = if RELKIND_HAS_STORAGE(relkind) {
        if relfilenumber == InvalidRelFileNumber {
            relfilenumber = relid;
        }
        true
    } else {
        debug_assert!(relfilenumber == InvalidRelFileNumber);
        false
    };
    if reltablespace == init_small::globals::MyDatabaseTableSpace() {
        reltablespace = InvalidOid;
    }

    let rel = relcache::local::RelationBuildLocalRelation(
        relname,
        relnamespace,
        tupdesc,
        relid,
        reltype,
        accessmtd,
        relfilenumber,
        reltablespace,
        false,
        false,
        relpersistence,
        relkind,
    )?;

    let (mut relfrozenxid, mut relminmxid) = (0 as TransactionId, 0 as MultiXactId);
    if create_storage {
        if RELKIND_HAS_TABLE_AM(relkind) {
            let handle = Relation::open_rc(Rc::clone(&rel), None);
            let (fxid, mmxid) = tableam::table_relation_set_new_filelocator(
                &handle,
                &rel.rd_locator.get(),
                relpersistence as i8,
            )?;
            relfrozenxid = fxid;
            relminmxid = mmxid;
        } else {
            catalog_storage::RelationCreateStorage(rel.rd_locator.get(), relpersistence, true)?;
        }
    }
    let _ = mcx;
    Ok((rel, relfrozenxid, relminmxid))
}

fn name_datum(name: &types_tuple::NameData) -> Datum {
    Datum::from_usize(name.data.as_ptr() as usize)
}

pub fn InsertPgClassTuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_class_desc: &Relation<'mcx>,
    rd_rel: &types_rel::FormData_pg_class,
    natts: i16,
    new_rel_oid: Oid,
    reloptions: Option<&[u8]>,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_class];
    let mut nulls = [false; Natts_pg_class];
    // Anum_pg_class_* order (pg_class.h, 18.3: relallfrozen is column 13).
    values[0] = Datum::from_oid(new_rel_oid);
    values[1] = name_datum(&rd_rel.relname);
    values[2] = Datum::from_oid(rd_rel.relnamespace);
    values[3] = Datum::from_oid(rd_rel.reltype);
    values[4] = Datum::from_oid(InvalidOid); // reloftype
    values[5] = Datum::from_oid(rd_rel.relowner);
    values[6] = Datum::from_oid(rd_rel.relam);
    values[7] = Datum::from_oid(rd_rel.relfilenode);
    values[8] = Datum::from_oid(rd_rel.reltablespace);
    values[9] = Datum::from_i32(rd_rel.relpages);
    values[10] = Datum::from_f32(rd_rel.reltuples);
    values[11] = Datum::from_i32(rd_rel.relallvisible);
    values[12] = Datum::from_i32(0); // relallfrozen
    values[13] = Datum::from_oid(rd_rel.reltoastrelid);
    values[14] = Datum::from_bool(rd_rel.relhasindex);
    values[15] = Datum::from_bool(rd_rel.relisshared);
    values[16] = Datum::from_char(rd_rel.relpersistence as i8);
    values[17] = Datum::from_char(rd_rel.relkind as i8);
    values[18] = Datum::from_i16(natts);
    values[19] = Datum::from_i16(0); // relchecks
    values[20] = Datum::from_bool(false); // relhasrules
    values[21] = Datum::from_bool(false); // relhastriggers
    values[22] = Datum::from_bool(rd_rel.relhassubclass);
    values[23] = Datum::from_bool(rd_rel.relrowsecurity);
    values[24] = Datum::from_bool(false); // relforcerowsecurity
    values[25] = Datum::from_bool(rd_rel.relispopulated);
    values[26] = Datum::from_char(rd_rel.relreplident as i8);
    values[27] = Datum::from_bool(rd_rel.relispartition);
    values[28] = Datum::from_oid(InvalidOid); // relrewrite
    values[29] = Datum::from_transaction_id(rd_rel.relfrozenxid);
    values[30] = Datum::from_transaction_id(rd_rel.relminmxid);
    nulls[Anum_pg_class_relacl - 1] = true;
    match reloptions {
        Some(img) => {
            values[Anum_pg_class_reloptions - 1] = Datum::from_usize(img.as_ptr() as usize)
        }
        None => nulls[Anum_pg_class_reloptions - 1] = true,
    }
    nulls[Anum_pg_class_relpartbound - 1] = true;

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_class_desc.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, pg_class_desc, &mut tup)
}

#[allow(clippy::too_many_arguments)]
fn AddNewRelationTuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_class_desc: &Relation<'mcx>,
    new_rel_desc: &RelationData<'static>,
    new_rel_oid: Oid,
    new_type_oid: Oid,
    relowner: Oid,
    relkind: u8,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
    reloptions: Option<&[u8]>,
) -> PgResult<()> {
    let mut form = new_rel_desc.rd_rel.clone();
    form.relpages = 0;
    form.reltuples = -1.0;
    form.relallvisible = 0;
    if relkind == types_rel::RELKIND_SEQUENCE {
        form.relpages = 1;
        form.reltuples = 1.0;
    }
    form.relfrozenxid = relfrozenxid;
    form.relminmxid = relminmxid;
    form.relowner = relowner;
    form.reltype = new_type_oid;
    form.relispartition = false;
    InsertPgClassTuple(
        mcx,
        pg_class_desc,
        &form,
        new_rel_desc.rd_att.natts as i16,
        new_rel_oid,
        reloptions,
    )
}

pub fn insert_pg_attribute_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_attribute_rel: &Relation<'mcx>,
    attrs: &FormData_pg_attribute,
    new_rel_oid: Oid,
    indstate: &mut catalog_indexing::CatalogIndexState<'mcx>,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_attribute];
    let mut nulls = [false; Natts_pg_attribute];
    values[0] = Datum::from_oid(new_rel_oid);
    values[1] = name_datum(&attrs.attname);
    values[2] = Datum::from_oid(attrs.atttypid);
    values[3] = Datum::from_i16(attrs.attlen);
    values[4] = Datum::from_i16(attrs.attnum);
    values[5] = Datum::from_i32(attrs.atttypmod);
    values[6] = Datum::from_i16(attrs.attndims);
    values[7] = Datum::from_bool(attrs.attbyval);
    values[8] = Datum::from_char(attrs.attalign);
    values[9] = Datum::from_char(attrs.attstorage);
    values[10] = Datum::from_char(attrs.attcompression);
    values[11] = Datum::from_bool(attrs.attnotnull);
    values[12] = Datum::from_bool(attrs.atthasdef);
    values[13] = Datum::from_bool(attrs.atthasmissing);
    values[14] = Datum::from_char(attrs.attidentity);
    values[15] = Datum::from_char(attrs.attgenerated);
    values[16] = Datum::from_bool(attrs.attisdropped);
    values[17] = Datum::from_bool(attrs.attislocal);
    values[18] = Datum::from_i16(attrs.attinhcount);
    values[19] = Datum::from_oid(attrs.attcollation);
    // attstattarget, attacl, attoptions, attfdwoptions, attmissingval.
    for n in &mut nulls[20..25] {
        *n = true;
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_attribute_rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsertWithInfo(mcx, pg_attribute_rel, &mut tup, indstate)
}

fn AddNewAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    new_rel_oid: Oid,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
) -> PgResult<()> {
    let rel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &rel)?;

    for i in 0..tupdesc.natts as usize {
        let att = &tupdesc.attrs[i];
        insert_pg_attribute_tuple(mcx, &rel, att, new_rel_oid, &mut indstate)?;

        let myself = ObjectAddress::sub_set(RELATION_RELATION_ID, new_rel_oid, i as i32 + 1);
        let referenced = ObjectAddress::set(TYPE_RELATION_ID, att.atttypid);
        pg_depend::recordDependencyOn(
            mcx,
            &myself,
            &referenced,
            pg_depend::DependencyType::Normal,
        )?;
        if att.attcollation != InvalidOid && att.attcollation != DEFAULT_COLLATION_OID {
            let referenced = ObjectAddress::set(catalog::CollationRelationId, att.attcollation);
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &referenced,
                pg_depend::DependencyType::Normal,
            )?;
        }
    }

    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        for att in SysAtt.iter() {
            insert_pg_attribute_tuple(mcx, &rel, att, new_rel_oid, &mut indstate)?;
        }
    }

    catalog_indexing::CatalogCloseIndexes(indstate)?;
    rel.close(RowExclusiveLock)
}

pub struct HeapCreateParams<'a> {
    pub relname: &'a str,
    pub relnamespace: Oid,
    pub reltablespace: Oid,
    pub ownerid: Oid,
    pub accessmtd: Oid,
    pub relkind: u8,
    pub relpersistence: u8,
    pub allow_system_table_mods: bool,
    pub reloptions: Option<&'a [u8]>,
}

pub fn heap_create_with_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    p: &HeapCreateParams<'_>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<Oid> {
    debug_assert!(
        p.relkind == RELKIND_RELATION
            || p.relkind == types_rel::RELKIND_TOASTVALUE
            || p.relkind == types_rel::RELKIND_SEQUENCE
            || p.relkind == types_rel::RELKIND_PARTITIONED_TABLE,
        "only plain/partitioned tables, toast tables and sequences ported"
    );
    // C: no rowtype/array pg_type entry where the relation is an
    // implementation detail (toast, sequences, indexes).
    let make_rowtype = p.relkind != types_rel::RELKIND_TOASTVALUE
        && p.relkind != types_rel::RELKIND_SEQUENCE;
    let pg_class_desc = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    CheckAttributeNamesTypes(tupdesc, p.relkind)?;

    if lsyscache::get_relname_relid(p.relname, p.relnamespace)? != InvalidOid {
        return Err(err(
            format!("relation \"{}\" already exists", p.relname),
            ERRCODE_DUPLICATE_TABLE,
        ));
    }

    let old_type_oid =
        syscache_seams::lookup_pg_type_oid_by_name::call(p.relname, p.relnamespace)?;
    if old_type_oid != InvalidOid && !pg_type::moveArrayTypeName(old_type_oid, p.relname, p.relnamespace)? {
        return Err(err(
            format!("type \"{}\" already exists", p.relname),
            types_error::ERRCODE_DUPLICATE_OBJECT,
        )
        .with_hint(
            "A relation has an associated type of the same name, so you must use a name \
             that doesn't conflict with any existing type.",
        )
        .into());
    }

    let relid = catalog::GetNewRelFileNumber(
        mcx,
        p.reltablespace,
        Some(&pg_class_desc),
        p.relpersistence,
    )?;
    lmgr::LockRelationOid(relid, AccessExclusiveLock)?;

    // C allocates the array-type oid after heap_create and the composite oid
    // inside TypeCreate; both are hoisted here so the relcache entry can carry
    // reltype at build time. GetNewObjectId order (relid, array, composite)
    // and both TypeCreate calls' catalog effects are unchanged.
    let (new_array_oid, new_type_oid) = if make_rowtype {
        let array_oid = pg_type::AssignTypeArrayOid(mcx)?;
        let pg_type_rel = table::table_open(mcx, types_core::TYPE_RELATION_ID, AccessShareLock)?;
        let oid = catalog::GetNewOidWithIndex(
            mcx,
            &pg_type_rel,
            pg_type::TypeOidIndexId,
            pg_type::Anum_pg_type_oid,
        )?;
        pg_type_rel.close(AccessShareLock)?;
        (array_oid, oid)
    } else {
        (InvalidOid, InvalidOid)
    };

    // relacl: get_user_default_acl unported; pg_default_acl entries (if any)
    // are not honored — relacl is always NULL here.
    let (new_rel_desc, relfrozenxid, relminmxid) = heap_create(
        mcx,
        p.relname,
        p.relnamespace,
        p.reltablespace,
        relid,
        new_type_oid,
        InvalidRelFileNumber,
        p.accessmtd,
        tupdesc,
        p.relkind,
        p.relpersistence,
        p.allow_system_table_mods,
    )?;

    if make_rowtype {
    AddNewRelationType(
        mcx,
        p.relname,
        p.relnamespace,
        relid,
        p.relkind,
        p.ownerid,
        new_type_oid,
        new_array_oid,
    )?;

    let relarrayname = pg_type::makeArrayTypeName(p.relname, p.relnamespace)?;
    pg_type::TypeCreate(
        mcx,
        &pg_type::TypeCreateParams {
            newTypeOid: new_array_oid,
            typeName: core::str::from_utf8(relarrayname.name_str()).expect("non-UTF-8 array type name"),
            typeNamespace: p.relnamespace,
            relationOid: InvalidOid,
            relationKind: 0,
            ownerId: p.ownerid,
            internalSize: -1,
            typeType: pg_type::TYPTYPE_BASE,
            typeCategory: pg_type::TYPCATEGORY_ARRAY,
            typePreferred: false,
            typDelim: pg_type::DEFAULT_TYPDELIM,
            inputProcedure: pg_type::F_ARRAY_IN,
            outputProcedure: pg_type::F_ARRAY_OUT,
            receiveProcedure: pg_type::F_ARRAY_RECV,
            sendProcedure: pg_type::F_ARRAY_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: pg_type::F_ARRAY_TYPANALYZE,
            subscriptProcedure: pg_type::F_ARRAY_SUBSCRIPT_HANDLER,
            elementType: new_type_oid,
            isImplicitArray: true,
            arrayType: InvalidOid,
            baseType: InvalidOid,
            passedByValue: false,
            alignment: TYPALIGN_DOUBLE,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
        },
    )?;
    }

    AddNewRelationTuple(
        mcx,
        &pg_class_desc,
        &new_rel_desc,
        relid,
        new_type_oid,
        p.ownerid,
        p.relkind,
        relfrozenxid,
        relminmxid,
        p.reloptions,
    )?;

    AddNewAttributeTuples(mcx, relid, &new_rel_desc.rd_att, p.relkind)?;

    if p.relkind != types_rel::RELKIND_TOASTVALUE
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        let myself = ObjectAddress::set(RELATION_RELATION_ID, relid);
        pg_depend::recordDependencyOnOwner(RELATION_RELATION_ID, relid, p.ownerid);
        // recordDependencyOnNewAcl: relacl is always NULL here (divergence
        // above) and the owner needs no entry, so C records nothing.
        // recordDependencyOnCurrentExtension: extension.c unported; C no-ops
        // outside CREATE EXTENSION scripts.
        let mut addrs = [
            ObjectAddress::set(catalog::NamespaceRelationId, p.relnamespace),
            ObjectAddress::set(AccessMethodRelationId, p.accessmtd),
        ];
        let live = if p.accessmtd != InvalidOid { 2 } else { 1 };
        pg_depend::record_object_address_dependencies(
            mcx,
            &myself,
            &mut addrs[..live],
            pg_depend::DependencyType::Normal,
        )?;
    }

    pg_class_desc.close(RowExclusiveLock)?;
    Ok(relid)
}

fn AddNewRelationType<'mcx>(
    mcx: Mcx<'mcx>,
    typeName: &str,
    typeNamespace: Oid,
    new_rel_oid: Oid,
    new_rel_kind: u8,
    ownerid: Oid,
    new_row_type: Oid,
    new_array_type: Oid,
) -> PgResult<ObjectAddress> {
    pg_type::TypeCreate(
        mcx,
        &pg_type::TypeCreateParams {
            newTypeOid: new_row_type,
            typeName,
            typeNamespace,
            relationOid: new_rel_oid,
            relationKind: new_rel_kind,
            ownerId: ownerid,
            internalSize: -1,
            typeType: pg_type::TYPTYPE_COMPOSITE,
            typeCategory: pg_type::TYPCATEGORY_COMPOSITE,
            typePreferred: false,
            typDelim: pg_type::DEFAULT_TYPDELIM,
            inputProcedure: pg_type::F_RECORD_IN,
            outputProcedure: pg_type::F_RECORD_OUT,
            receiveProcedure: pg_type::F_RECORD_RECV,
            sendProcedure: pg_type::F_RECORD_SEND,
            typmodinProcedure: InvalidOid,
            typmodoutProcedure: InvalidOid,
            analyzeProcedure: InvalidOid,
            subscriptProcedure: InvalidOid,
            elementType: InvalidOid,
            isImplicitArray: false,
            arrayType: new_array_type,
            baseType: InvalidOid,
            passedByValue: false,
            alignment: TYPALIGN_DOUBLE,
            storage: TYPSTORAGE_EXTENDED,
            typeMod: -1,
            typNDims: 0,
            typeNotNull: false,
            typeCollation: InvalidOid,
        },
    )
}

// RelationClearMissing (heap.c): reset atthasmissing/attmissingval on every
// user column ahead of a table rewrite.
pub fn RelationClearMissing<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, relid, types_rel::NoLock)?;
    let natts = rel.rd_att.natts;
    let has_any = (0..natts as usize).any(|i| rel.rd_att.attr(i).atthasmissing);
    if !has_any {
        rel.close(types_rel::NoLock)?;
        return Ok(());
    }
    let attrrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    for attnum in 1..=natts {
        if !rel.rd_att.attr(attnum as usize - 1).atthasmissing {
            continue;
        }
        let keys = [
            crate::drop::oid_scankey(1, relid),
            crate::drop::int2_scankey(5, attnum as AttrNumber),
        ];
        let mut scan =
            genam::systable_beginscan(mcx, &attrrel, 2659, true, None, &keys)?;
        let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
            panic!("cache lookup failed for attribute {attnum} of relation {relid}")
        });
        let desc = attrrel.descr();
        let n = desc.natts as usize;
        let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, n)?;
        values.resize(n, Datum::null());
        isnull.resize(n, false);
        replace.resize(n, false);
        values[14 - 1] = Datum::from_bool(false); // atthasmissing
        replace[14 - 1] = true;
        isnull[25 - 1] = true; // attmissingval
        replace[25 - 1] = true;
        let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
        let otid = tup.t_self;
        genam::systable_endscan(mcx, scan)?;
        catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
    }
    attrrel.close(RowExclusiveLock)?;
    rel.close(types_rel::NoLock)
}

// StoreAttrMissingVal (heap.c): wrap the evaluated default in a 1-element
// array of the column type and flip atthasmissing. Plain tables only.
pub fn StoreAttrMissingVal<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnum: AttrNumber,
    missingval: Datum,
) -> PgResult<()> {
    debug_assert!(rel.rd_rel.relkind == RELKIND_RELATION);
    let attrrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let keys = [
        crate::drop::oid_scankey(1, rel.rd_id),
        crate::drop::int2_scankey(5, attnum),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &attrrel,
        2659, // AttributeRelidNumIndexId
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?.unwrap_or_else(|| {
        panic!("cache lookup failed for attribute {attnum} of relation {}", rel.rd_id)
    });
    let desc = attrrel.descr();
    let get = |anum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_attribute columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let atttypid = get(3).as_oid();
    let attlen = get(4).as_i16();
    let attbyval = get(8).as_bool();
    let attalign = get(9).as_i8() as u8;

    let arr = arrayfuncs::construct::construct_array(
        mcx,
        core::slice::from_ref(&missingval),
        atttypid,
        attlen as i32,
        attbyval,
        attalign,
    )?;

    let natts = desc.natts as usize;
    let mut values: mcx::PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut isnull: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    let mut replace: mcx::PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    isnull.resize(natts, false);
    replace.resize(natts, false);
    values[14 - 1] = Datum::from_bool(true); // atthasmissing
    replace[14 - 1] = true;
    values[25 - 1] = Datum::from_usize(arr.as_ptr() as usize); // attmissingval
    replace[25 - 1] = true;

    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;
    attrrel.close(RowExclusiveLock)
}
