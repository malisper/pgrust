// heap.c DDL half, plain-table lane. Out of scope (loud or WARNING'd below):
// TOAST, typed/partitioned/shared/mapped rels, constraints/defaults,
// pg_type rowtype + array type rows, pg_depend/pg_shdepend recording,
// default ACLs.
use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use types_core::{
    AttrNumber, InvalidOid, InvalidRelFileNumber, MultiXactId, Oid, TransactionId,
    ATTRIBUTE_RELATION_ID, RELATION_RELATION_ID,
};
use types_error::{PgError, PgResult, ERRCODE_DUPLICATE_COLUMN, ERRCODE_DUPLICATE_TABLE, ERROR};
use types_rel::{
    AccessExclusiveLock, Relation, RelationData, RowExclusiveLock, RELKIND_COMPOSITE_TYPE, RELKIND_HAS_STORAGE,
    RELKIND_HAS_TABLESPACE, RELKIND_HAS_TABLE_AM, RELKIND_RELATION, RELKIND_VIEW,
};
use types_tuple::{FormData_pg_attribute, TupleDescData};

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
        if att.atttypid == InvalidOid {
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

fn InsertPgClassTuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_class_desc: &Relation<'mcx>,
    new_rel_desc: &RelationData<'static>,
    new_rel_oid: Oid,
    ownerid: Oid,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
) -> PgResult<()> {
    let rd_rel = &new_rel_desc.rd_rel;
    let natts = new_rel_desc.rd_att.natts;
    let mut values = [Datum::null(); Natts_pg_class];
    let mut nulls = [false; Natts_pg_class];
    // Anum_pg_class_* order (pg_class.h, 18.3: relallfrozen is column 13).
    values[0] = Datum::from_oid(new_rel_oid);
    values[1] = name_datum(&rd_rel.relname);
    values[2] = Datum::from_oid(rd_rel.relnamespace);
    values[3] = Datum::from_oid(rd_rel.reltype);
    values[4] = Datum::from_oid(InvalidOid); // reloftype
    values[5] = Datum::from_oid(ownerid);
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
    values[18] = Datum::from_i16(natts as i16);
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
    values[29] = Datum::from_transaction_id(relfrozenxid);
    values[30] = Datum::from_transaction_id(relminmxid);
    nulls[Anum_pg_class_relacl - 1] = true;
    nulls[Anum_pg_class_reloptions - 1] = true;
    nulls[Anum_pg_class_relpartbound - 1] = true;

    let mut tup = heaptuple::heap_form_tuple(mcx, pg_class_desc.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, pg_class_desc, &mut tup)
}

fn insert_pg_attribute_tuple<'mcx>(
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
        insert_pg_attribute_tuple(mcx, &rel, &tupdesc.attrs[i], new_rel_oid, &mut indstate)?;
        // C: recordDependencyOn(rel-column -> type/collation) — pg_depend
        // recording unported; DROP-side consistency rides with pg_depend.
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
}

pub fn heap_create_with_catalog<'mcx>(
    mcx: Mcx<'mcx>,
    p: &HeapCreateParams<'_>,
    tupdesc: &TupleDescData<'_>,
) -> PgResult<Oid> {
    debug_assert!(p.relkind == RELKIND_RELATION, "only plain tables ported");
    let pg_class_desc = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    CheckAttributeNamesTypes(tupdesc, p.relkind)?;

    if lsyscache::get_relname_relid(p.relname, p.relnamespace)? != InvalidOid {
        return Err(err(
            format!("relation \"{}\" already exists", p.relname),
            ERRCODE_DUPLICATE_TABLE,
        ));
    }

    let relid = catalog::GetNewRelFileNumber(
        mcx,
        p.reltablespace,
        Some(&pg_class_desc),
        p.relpersistence,
    )?;
    lmgr::LockRelationOid(relid, AccessExclusiveLock)?;

    // relacl: get_user_default_acl unported; pg_default_acl entries (if any)
    // are not honored — relacl is always NULL here.
    let (new_rel_desc, relfrozenxid, relminmxid) = heap_create(
        mcx,
        p.relname,
        p.relnamespace,
        p.reltablespace,
        relid,
        InvalidRelFileNumber,
        p.accessmtd,
        tupdesc,
        p.relkind,
        p.relpersistence,
        p.allow_system_table_mods,
    )?;

    elog::elog(
        types_error::WARNING,
        format!(
            "AddNewRelationType unported (backend-catalog-heap): relation \"{}\" gets \
             reltype = 0, no composite/array pg_type rows, no pg_depend/pg_shdepend rows",
            p.relname
        ),
    )?;

    InsertPgClassTuple(
        mcx,
        &pg_class_desc,
        &new_rel_desc,
        relid,
        p.ownerid,
        relfrozenxid,
        relminmxid,
    )?;

    AddNewAttributeTuples(mcx, relid, &new_rel_desc.rd_att, p.relkind)?;

    pg_class_desc.close(RowExclusiveLock)?;
    Ok(relid)
}
