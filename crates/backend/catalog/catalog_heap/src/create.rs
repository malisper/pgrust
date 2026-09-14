// heap.c DDL half, plain-table lane. Out of scope (loud or WARNING'd below):
// TOAST, typed/partitioned/shared/mapped rels, constraints/defaults,
// pg_shdepend recording.
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
const GLOBALTABLESPACE_OID: Oid = 1664;
const Anum_pg_class_relacl: usize = 32;
const Anum_pg_class_reloptions: usize = 33;
const Anum_pg_class_relpartbound: usize = 34;
const Natts_pg_attribute: usize = 25;
const ATTRIBUTE_GENERATED_VIRTUAL: i8 = b'v' as i8;

#[track_caller]
#[cold]
#[inline(never)]
fn err(msg: String, sqlstate: types_error::SqlState) -> Box<PgError> {
    Box::new(PgError::new(ERROR, msg).with_sqlstate(sqlstate))
}

// binary_upgrade_next_{heap,toast}_pg_class_{oid,relfilenumber}
// (pg_upgrade_support.c / heap.c): set-once, consume-once overrides used by
// heap_create_with_catalog while in binary upgrade mode.
macro_rules! next_oid_override {
    ($cell:ident, $setter:ident, $take:ident) => {
        thread_local! {
            static $cell: std::cell::Cell<Oid> = const { std::cell::Cell::new(InvalidOid) };
        }

        pub fn $setter(oid: Oid) {
            $cell.set(oid);
        }

        pub fn $take() -> Option<Oid> {
            let oid = $cell.get();
            if types_core::OidIsValid(oid) {
                $cell.set(InvalidOid);
                Some(oid)
            } else {
                None
            }
        }
    };
}

next_oid_override!(
    NEXT_HEAP_PG_CLASS_OID,
    SetNextHeapPgClassOid,
    take_next_heap_pg_class_oid
);
next_oid_override!(
    NEXT_HEAP_PG_CLASS_RELFILENUMBER,
    SetNextHeapPgClassRelfilenumber,
    take_next_heap_pg_class_relfilenumber
);
next_oid_override!(
    NEXT_TOAST_PG_CLASS_OID,
    SetNextToastPgClassOid,
    take_next_toast_pg_class_oid
);
next_oid_override!(
    NEXT_TOAST_PG_CLASS_RELFILENUMBER,
    SetNextToastPgClassRelfilenumber,
    take_next_toast_pg_class_relfilenumber
);

// toasting.c's binary-upgrade arm peeks without consuming; heap_create_with
// _catalog does the consume.
pub fn NextToastPgClassOidIsSet() -> bool {
    NEXT_TOAST_PG_CLASS_OID.with(|c| types_core::OidIsValid(c.get()))
}

#[track_caller]
#[cold]
#[inline(never)]
fn binary_upgrade_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, msg.to_string())
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub const CHKATYPE_ANYARRAY: i32 = 0x01;
pub const CHKATYPE_ANYRECORD: i32 = 0x02;
pub const CHKATYPE_IS_PARTKEY: i32 = 0x04;
pub const CHKATYPE_IS_VIRTUAL: i32 = 0x08;

pub fn CheckAttributeNamesTypes<'mcx>(
    mcx: Mcx<'mcx>,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
    flags: i32,
) -> PgResult<()> {
    let natts = tupdesc.natts as usize;
    if natts > types_tuple::htup::MaxHeapAttributeNumber as usize {
        return Err(err(
            format!(
                "tables can have at most {} columns",
                types_tuple::htup::MaxHeapAttributeNumber
            ),
            types_error::ERRCODE_TOO_MANY_COLUMNS,
        ));
    }
    if relkind != RELKIND_VIEW && relkind != RELKIND_COMPOSITE_TYPE {
        for att in &tupdesc.attrs[..natts] {
            let name = core::str::from_utf8(att.attname.name_str()).expect("non-UTF-8 attname");
            if crate::SystemAttributeByName(name).is_some() {
                return Err(err(
                    format!("column name \"{name}\" conflicts with a system column name"),
                    ERRCODE_DUPLICATE_COLUMN,
                ));
            }
        }
    }
    for i in 1..natts {
        for j in 0..i {
            if tupdesc.attrs[j].attname.name_str() == tupdesc.attrs[i].attname.name_str() {
                let name = core::str::from_utf8(tupdesc.attrs[j].attname.name_str())
                    .expect("non-UTF-8 attname");
                return Err(err(
                    format!("column name \"{name}\" specified more than once"),
                    ERRCODE_DUPLICATE_COLUMN,
                ));
            }
        }
    }
    let mut containing_rowtypes: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, 4)?;
    for att in &tupdesc.attrs[..natts] {
        // upstream 01db3f0398fd (18.4): Don't call CheckAttributeType() with InvalidOid on dropped cols
        if att.attisdropped {
            continue;
        }
        let name = core::str::from_utf8(att.attname.name_str()).expect("non-UTF-8 attname");
        CheckAttributeType(
            mcx,
            name,
            att.atttypid,
            att.attcollation,
            &mut containing_rowtypes,
            flags
                | if att.attgenerated == ATTRIBUTE_GENERATED_VIRTUAL {
                    CHKATYPE_IS_VIRTUAL
                } else {
                    0
                },
        )?;
    }
    Ok(())
}

pub fn CheckAttributeType<'mcx>(
    mcx: Mcx<'mcx>,
    attname: &str,
    atttypid: Oid,
    attcollation: Oid,
    containing_rowtypes: &mut mcx::PgVec<'mcx, Oid>,
    flags: i32,
) -> PgResult<()> {
    let att_typtype = lsyscache::typ::get_typtype(atttypid)?;

    stack_depth::check_stack_depth()?;

    if att_typtype == lsyscache::typ::TYPTYPE_PSEUDO {
        if !((atttypid == types_core::catalog::ANYARRAYOID && flags & CHKATYPE_ANYARRAY != 0)
            || (atttypid == types_core::catalog::RECORDOID && flags & CHKATYPE_ANYRECORD != 0)
            || (atttypid == types_core::catalog::RECORDARRAYOID && flags & CHKATYPE_ANYRECORD != 0))
        {
            let tname = format_type::format_type_be(atttypid)?;
            let msg = if flags & CHKATYPE_IS_PARTKEY != 0 {
                format!("partition key column {attname} has pseudo-type {tname}")
            } else {
                format!("column \"{attname}\" has pseudo-type {tname}")
            };
            return Err(err(msg, types_error::ERRCODE_INVALID_TABLE_DEFINITION));
        }
    } else if att_typtype == lsyscache::typ::TYPTYPE_DOMAIN {
        if flags & CHKATYPE_IS_VIRTUAL != 0 {
            return Err(err(
                format!("virtual generated column \"{attname}\" cannot have a domain type"),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        CheckAttributeType(
            mcx,
            attname,
            lsyscache::typ::getBaseType(atttypid)?,
            attcollation,
            containing_rowtypes,
            flags,
        )?;
    } else if att_typtype == lsyscache::typ::TYPTYPE_COMPOSITE {
        if containing_rowtypes.contains(&atttypid) {
            return Err(err(
                format!(
                    "composite type {} cannot be made a member of itself",
                    format_type::format_type_be(atttypid)?
                ),
                types_error::ERRCODE_INVALID_TABLE_DEFINITION,
            ));
        }
        containing_rowtypes.push(atttypid);
        let rel = relation::relation_open(
            mcx,
            lsyscache::typ::get_typ_typrelid(atttypid)?,
            AccessShareLock,
        )?;
        for i in 0..rel.rd_att.natts as usize {
            let attr = rel.rd_att.attr(i);
            if attr.attisdropped {
                continue;
            }
            let inner_name =
                core::str::from_utf8(attr.attname.name_str()).expect("non-UTF-8 attname");
            CheckAttributeType(
                mcx,
                inner_name,
                attr.atttypid,
                attr.attcollation,
                containing_rowtypes,
                flags & !CHKATYPE_IS_PARTKEY,
            )?;
        }
        rel.close(AccessShareLock)?;
        containing_rowtypes.pop();
    } else if att_typtype == lsyscache::typ::TYPTYPE_RANGE {
        CheckAttributeType(
            mcx,
            attname,
            lsyscache::misc::get_range_subtype(atttypid)?,
            lsyscache::misc::get_range_collation(atttypid)?,
            containing_rowtypes,
            flags,
        )?;
    } else if att_typtype == lsyscache::typ::TYPTYPE_MULTIRANGE {
        // upstream ff8f27d6eae2 (18.4): Don't allow composite type to be member of itself via multirange
        // If it's a multirange, recurse to check its plain range type.
        CheckAttributeType(
            mcx,
            attname,
            lsyscache::misc::get_multirange_range(atttypid)?,
            InvalidOid, // range types are not collatable
            containing_rowtypes,
            flags,
        )?;
    } else {
        let att_typelem = lsyscache::typ::get_element_type(atttypid)?;
        if att_typelem != InvalidOid {
            CheckAttributeType(
                mcx,
                attname,
                att_typelem,
                attcollation,
                containing_rowtypes,
                flags,
            )?;
        }
    }

    if flags & CHKATYPE_IS_VIRTUAL != 0 && atttypid >= types_core::FirstUnpinnedObjectId {
        return Err(Box::new(
            (*err(
                format!("virtual generated column \"{attname}\" cannot have a user-defined type"),
                types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
            ))
            .with_detail(
                "Virtual generated columns that make use of user-defined types are not yet supported.",
            ),
        ));
    }

    if attcollation == InvalidOid && lsyscache::typ::type_is_collatable(atttypid)? {
        let tname = format_type::format_type_be(atttypid)?;
        let msg = if flags & CHKATYPE_IS_PARTKEY != 0 {
            format!(
                "no collation was derived for partition key column {attname} with collatable type {tname}"
            )
        } else {
            format!(
                "no collation was derived for column \"{attname}\" with collatable type {tname}"
            )
        };
        return Err(Box::new(
            (*err(msg, types_error::ERRCODE_INVALID_TABLE_DEFINITION))
                .with_hint("Use the COLLATE clause to set the collation explicitly."),
        ));
    }
    Ok(())
}

// heap.c:314-326 (heap_create). heap_create_with_catalog also runs it ahead
// of its hoisted AssignTypeArrayOid, keeping C's error order in binary
// upgrade mode (heap.c:1330 before :1368).
fn check_system_namespace_create(
    mcx: Mcx<'_>,
    relname: &str,
    relnamespace: Oid,
    relkind: u8,
    allow_system_table_mods: bool,
) -> PgResult<()> {
    if ((catalog::IsCatalogNamespace(relnamespace) && relkind != types_rel::RELKIND_INDEX)
        || catalog::IsToastNamespace(relnamespace))
        && !allow_system_table_mods
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        let nspname = lsyscache::get_namespace_name(mcx, relnamespace)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        return Err(Box::new(
            PgError::new(
                ERROR,
                format!("permission denied to create \"{nspname}.{relname}\""),
            )
            .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .with_detail("System catalog modifications are currently disallowed."),
        ));
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
    shared_relation: bool,
    mapped_relation: bool,
    allow_system_table_mods: bool,
    create_storage: bool,
) -> PgResult<(Rc<RelationData<'static>>, TransactionId, MultiXactId)> {
    check_system_namespace_create(mcx, relname, relnamespace, relkind, allow_system_table_mods)?;

    let mut reltablespace = reltablespace;
    if !RELKIND_HAS_TABLESPACE(relkind) {
        reltablespace = InvalidOid;
    }
    let mut relfilenumber = relfilenumber;
    let mut create_storage = create_storage;
    if !RELKIND_HAS_STORAGE(relkind) {
        create_storage = false;
    } else if relfilenumber == InvalidRelFileNumber {
        // Unspecified by the caller: storage gets oid == relid.
        relfilenumber = relid;
    }
    if reltablespace == init_small::globals::MyDatabaseTableSpace() {
        reltablespace = InvalidOid;
    }

    // relcache.c:3566-3568 (RelationBuildLocalRelation): the hardwired
    // shared-relation list must agree with the caller's flag; a shared
    // toast/index for a shared catalog created after initdb (binary-upgrade
    // preset OIDs, allow_system_table_mods) fails here in C too.
    if shared_relation != catalog::IsSharedRelation(relid) {
        return Err(Box::new(PgError::error(format!(
            "shared_relation flag for \"{relname}\" does not match IsSharedRelation({relid})"
        ))));
    }
    // A shared relation whose OID is on that list is only built in
    // bootstrap; pgrust's relcache has no post-initdb shared build.
    if shared_relation {
        return Err(err(
            format!("cannot create shared relation \"{relname}\""),
            types_error::ERRCODE_FEATURE_NOT_SUPPORTED,
        )
        .with_detail("Creating shared relations after initdb is not supported.")
        .into());
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
        shared_relation,
        mapped_relation,
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
    // C: relations without storage protect their tablespace via pg_shdepend
    // instead of a physical file.
    if !create_storage && reltablespace != InvalidOid {
        pg_depend::recordDependencyOnTablespace(mcx, RELATION_RELATION_ID, relid, reltablespace)?;
    }
    // Ensures the stats entry is dropped if the transaction aborts.
    pgstat::relation::pgstat_create_relation(relid, rel.rd_rel.relisshared);
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
    reloftype: Oid,
    relrewrite: Oid,
    relacl: Option<&[u8]>,
    reloptions: Option<&[u8]>,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_class];
    let mut nulls = [false; Natts_pg_class];
    // Anum_pg_class_* order (pg_class.h, 18.3: relallfrozen is column 13).
    values[0] = Datum::from_oid(new_rel_oid);
    values[1] = name_datum(&rd_rel.relname);
    values[2] = Datum::from_oid(rd_rel.relnamespace);
    values[3] = Datum::from_oid(rd_rel.reltype);
    values[4] = Datum::from_oid(reloftype);
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
    values[28] = Datum::from_oid(relrewrite);
    values[29] = Datum::from_transaction_id(rd_rel.relfrozenxid);
    values[30] = Datum::from_transaction_id(rd_rel.relminmxid);
    match relacl {
        Some(img) => values[Anum_pg_class_relacl - 1] = Datum::from_usize(img.as_ptr() as usize),
        None => nulls[Anum_pg_class_relacl - 1] = true,
    }
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
    reloftype: Oid,
    relowner: Oid,
    relkind: u8,
    relfrozenxid: TransactionId,
    relminmxid: MultiXactId,
    relrewrite: Oid,
    relacl: Option<&[u8]>,
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
        reloftype,
        relrewrite,
        relacl,
        reloptions,
    )
}

#[derive(Clone, Copy)]
#[allow(non_camel_case_types)]
pub struct FormExtraData_pg_attribute {
    pub attstattarget: datum::NullableDatum,
    pub attoptions: datum::NullableDatum,
}

fn form_pg_attribute_tuple<'mcx>(
    mcx: Mcx<'mcx>,
    pg_attribute_rel: &Relation<'mcx>,
    attrs: &FormData_pg_attribute,
    new_rel_oid: Oid,
    extra: Option<&FormExtraData_pg_attribute>,
) -> PgResult<heaptuple::HeapTuple<'mcx>> {
    let mut values = [Datum::null(); Natts_pg_attribute];
    let mut nulls = [false; Natts_pg_attribute];
    values[0] = Datum::from_oid(if new_rel_oid != InvalidOid {
        new_rel_oid
    } else {
        attrs.attrelid
    });
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
    if let Some(extra) = extra {
        values[20] = extra.attstattarget.value;
        nulls[20] = extra.attstattarget.isnull;
        values[22] = extra.attoptions.value;
        nulls[22] = extra.attoptions.isnull;
    }

    heaptuple::heap_form_tuple(mcx, pg_attribute_rel.descr(), &values, &nulls)
}

// InsertPgAttributeTuples (heap.c): batches of Heap2 MULTI_INSERT records.
pub fn InsertPgAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    pg_attribute_rel: &Relation<'mcx>,
    attrs: &[FormData_pg_attribute],
    new_rel_oid: Oid,
    attrs_extra: Option<&[FormExtraData_pg_attribute]>,
    indstate: Option<&mut catalog_indexing::CatalogIndexState<'mcx>>,
) -> PgResult<()> {
    // C sizeof(FormData_pg_attribute) == 100; Rust layout may differ.
    let nslots = attrs
        .len()
        .min(catalog_indexing::MAX_CATALOG_MULTI_INSERT_BYTES / 100)
        .max(1);
    let mut opened = None;
    let indstate = match indstate {
        Some(st) => st,
        None => {
            opened = Some(catalog_indexing::CatalogOpenIndexes(mcx, pg_attribute_rel)?);
            opened.as_mut().unwrap()
        }
    };
    debug_assert!(attrs_extra.is_none_or(|e| e.len() == attrs.len()));
    for (chunk_i, chunk) in attrs.chunks(nslots).enumerate() {
        let mut tuples = std::vec::Vec::with_capacity(chunk.len());
        for (j, att) in chunk.iter().enumerate() {
            let extra = attrs_extra.map(|e| &e[chunk_i * nslots + j]);
            tuples.push(form_pg_attribute_tuple(
                mcx,
                pg_attribute_rel,
                att,
                new_rel_oid,
                extra,
            )?);
        }
        catalog_indexing::CatalogTuplesMultiInsertWithInfo(
            mcx,
            pg_attribute_rel,
            tuples,
            indstate,
        )?;
    }
    if let Some(st) = opened {
        catalog_indexing::CatalogCloseIndexes(st)?;
    }
    Ok(())
}

fn AddNewAttributeTuples<'mcx>(
    mcx: Mcx<'mcx>,
    new_rel_oid: Oid,
    tupdesc: &TupleDescData<'_>,
    relkind: u8,
) -> PgResult<()> {
    let rel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &rel)?;

    InsertPgAttributeTuples(
        mcx,
        &rel,
        &tupdesc.attrs[..tupdesc.natts as usize],
        new_rel_oid,
        None,
        Some(&mut indstate),
    )?;

    for i in 0..tupdesc.natts as usize {
        let att = &tupdesc.attrs[i];
        // upstream f9d5a52da4ca (18.5): Don't try to record dependency on a dropped column's datatype
        if att.attisdropped {
            continue;
        }
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
        InsertPgAttributeTuples(mcx, &rel, &SysAtt, new_rel_oid, None, Some(&mut indstate))?;
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
    pub reloftype: Oid,
    // heap.c shared_relation: true only for a shared catalog's toast table
    // (toasting.c:241 "Toast table is shared if and only if its parent is").
    pub shared: bool,
    // RelationIsMapped(source): CLUSTER/VACUUM FULL transient heaps for
    // mapped catalogs must themselves be mapped (cluster.c make_new_heap).
    pub mapped: bool,
    pub allow_system_table_mods: bool,
    pub reloptions: Option<&'a [u8]>,
    // heap.c:1129 "link to original relation during a table rewrite":
    // pg_class.relrewrite of a transient heap (cluster.c make_new_heap) or of
    // its toast table (toasting.c OIDOldToast); InvalidOid otherwise.
    pub relrewrite: Oid,
    // heap.c use_user_acl: false for toast tables (toasting.c) and rewrite
    // heaps (cluster.c make_new_heap), whose relacl stays NULL.
    pub use_user_acl: bool,
    pub is_internal: bool,
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
            || p.relkind == types_rel::RELKIND_PARTITIONED_TABLE
            || p.relkind == RELKIND_VIEW
            || p.relkind == types_rel::RELKIND_MATVIEW
            || p.relkind == RELKIND_COMPOSITE_TYPE
            || p.relkind == types_rel::RELKIND_FOREIGN_TABLE,
        "only plain/partitioned tables, toast, sequences, views, matviews, composite types and foreign tables ported"
    );
    // C: no rowtype/array pg_type entry where the relation is an
    // implementation detail (toast, sequences, indexes).
    let make_rowtype =
        p.relkind != types_rel::RELKIND_TOASTVALUE && p.relkind != types_rel::RELKIND_SEQUENCE;
    let pg_class_desc = table::table_open(mcx, RELATION_RELATION_ID, RowExclusiveLock)?;

    CheckAttributeNamesTypes(
        mcx,
        tupdesc,
        p.relkind,
        if p.allow_system_table_mods {
            CHKATYPE_ANYARRAY
        } else {
            0
        },
    )?;

    if lsyscache::get_relname_relid(p.relname, p.relnamespace)? != InvalidOid {
        return Err(Box::new(
            PgError::new(ERROR, format!("relation \"{}\" already exists", p.relname))
                .with_sqlstate(ERRCODE_DUPLICATE_TABLE)
                // C heap.c:1196 (18.6)
                .with_location("heap.c", 1196, "heap_create_with_catalog"),
        ));
    }

    let old_type_oid = syscache_seams::lookup_pg_type_oid_by_name::call(p.relname, p.relnamespace)?;
    if old_type_oid != InvalidOid
        && !pg_type::moveArrayTypeName(mcx, old_type_oid, p.relname, p.relnamespace)?
    {
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

    // heap.c:1221-1222: shared relations must be in pg_global (last-ditch check).
    if p.shared && p.reltablespace != GLOBALTABLESPACE_OID {
        return Err(Box::new(PgError::error(
            "shared relations must be placed in pg_global tablespace",
        )));
    }
    // C moveArrayTypeName returns true WITHOUT renaming when the colliding
    // type is a shell: TypeCreate then fills the shell in place, reusing its
    // OID (newTypeOid stays InvalidOid on C's AddNewRelationType path). The
    // composite-OID hoist below must therefore reuse the shell's OID instead
    // of preassigning a fresh one — a preassigned OID trips TypeCreate's
    // "cannot assign new OID to existing shell type" XX000 guard (round-16
    // gramwalk: CREATE MATERIALIZED VIEW over a same-named shell type).
    let shell_row_type_oid =
        if old_type_oid != InvalidOid && !lsyscache::typ::get_typisdefined(old_type_oid)? {
            old_type_oid
        } else {
            InvalidOid
        };

    // Binary-upgrade override for pg_class.oid and relfilenumber (heap.c);
    // indexes use binary_upgrade_next_index_pg_class_oid instead.
    let mut relid = InvalidOid;
    let mut relfilenumber = InvalidRelFileNumber;
    if init_small::globals::IsBinaryUpgrade() {
        debug_assert!(p.relkind != types_rel::RELKIND_INDEX);
        debug_assert!(p.relkind != types_rel::RELKIND_PARTITIONED_INDEX);
        if p.relkind == types_rel::RELKIND_TOASTVALUE {
            // There might be no TOAST table, so we have to test for it.
            if let Some(oid) = take_next_toast_pg_class_oid() {
                relid = oid;
                relfilenumber = take_next_toast_pg_class_relfilenumber().ok_or_else(|| {
                    binary_upgrade_err(
                        "toast relfilenumber value not set when in binary upgrade mode",
                    )
                })?;
            }
        } else {
            relid = take_next_heap_pg_class_oid().ok_or_else(|| {
                binary_upgrade_err("pg_class heap OID value not set when in binary upgrade mode")
            })?;
            if RELKIND_HAS_STORAGE(p.relkind) {
                relfilenumber = take_next_heap_pg_class_relfilenumber().ok_or_else(|| {
                    binary_upgrade_err("relfilenumber value not set when in binary upgrade mode")
                })?;
            }
        }
    }
    if !types_core::OidIsValid(relid) {
        relid = catalog::GetNewRelFileNumber(
            mcx,
            p.reltablespace,
            Some(&pg_class_desc),
            p.relpersistence,
        )?;
    }
    let relid = relid;
    lmgr::LockRelationOid(relid, AccessExclusiveLock)?;
    check_system_namespace_create(
        mcx,
        p.relname,
        p.relnamespace,
        p.relkind,
        p.allow_system_table_mods,
    )?;

    // C allocates the array-type oid after heap_create and the composite oid
    // inside TypeCreate; both are hoisted here so the relcache entry can carry
    // reltype at build time. GetNewObjectId order (relid, array, composite)
    // and both TypeCreate calls' catalog effects are unchanged.
    let (new_array_oid, new_type_oid) = if make_rowtype {
        let array_oid = pg_type::AssignTypeArrayOid(mcx)?;
        if shell_row_type_oid != InvalidOid {
            // Shell reuse: the rowtype IS the surviving shell type; C never
            // draws a composite OID in this case (TypeCreate updates the
            // shell tuple in place).
            (array_oid, shell_row_type_oid)
        } else if init_small::globals::IsBinaryUpgrade() {
            // pg_type.c:472-481 (TypeCreate with reltypeid = InvalidOid,
            // heap.c:1379): the rowtype consumes
            // binary_upgrade_next_pg_type_oid, so pg_class.reltype and
            // pg_type.oid equal pg_upgrade's preset.
            let oid = pg_type::take_next_pg_type_oid().ok_or_else(|| {
                binary_upgrade_err("pg_type OID value not set when in binary upgrade mode")
            })?;
            (array_oid, oid)
        } else {
            let pg_type_rel =
                table::table_open(mcx, types_core::TYPE_RELATION_ID, AccessShareLock)?;
            let oid = catalog::GetNewOidWithIndex(
                mcx,
                &pg_type_rel,
                pg_type::TypeOidIndexId,
                pg_type::Anum_pg_type_oid,
            )?;
            pg_type_rel.close(AccessShareLock)?;
            (array_oid, oid)
        }
    } else {
        (InvalidOid, InvalidOid)
    };

    // heap.c:1297-1311: RELATION, VIEW, MATVIEW, FOREIGN_TABLE and
    // PARTITIONED_TABLE share OBJECT_TABLE.
    let relacl: Option<mcx::PgVec<'mcx, u8>> = match p.relkind {
        _ if !p.use_user_acl => None,
        RELKIND_RELATION
        | RELKIND_VIEW
        | types_rel::RELKIND_MATVIEW
        | types_rel::RELKIND_FOREIGN_TABLE
        | types_rel::RELKIND_PARTITIONED_TABLE => {
            aclchk_seams::get_user_default_acl::call(mcx, b'r', p.ownerid, p.relnamespace)?
        }
        types_rel::RELKIND_SEQUENCE => {
            aclchk_seams::get_user_default_acl::call(mcx, b'S', p.ownerid, p.relnamespace)?
        }
        _ => None,
    };

    let (new_rel_desc, relfrozenxid, relminmxid) = heap_create(
        mcx,
        p.relname,
        p.relnamespace,
        p.reltablespace,
        relid,
        new_type_oid,
        relfilenumber,
        p.accessmtd,
        tupdesc,
        p.relkind,
        p.relpersistence,
        p.shared,
        p.mapped,
        p.allow_system_table_mods,
        // create_storage = true is correct even for binary upgrade: the
        // storage created here is replaced later, but something must exist
        // on disk in the meanwhile.
        true,
    )?;

    if make_rowtype {
        AddNewRelationType(
            mcx,
            p.relname,
            p.relnamespace,
            relid,
            p.relkind,
            p.ownerid,
            // Shell reuse: pass InvalidOid so TypeCreate takes its existing-shell
            // update-in-place branch (which resolves to shell_row_type_oid ==
            // new_type_oid); a preassigned OID there is the XX000 guard.
            if shell_row_type_oid != InvalidOid {
                InvalidOid
            } else {
                new_type_oid
            },
            new_array_oid,
        )?;

        let relarrayname = pg_type::makeArrayTypeName(p.relname, p.relnamespace)?;
        pg_type::TypeCreate(
            mcx,
            &pg_type::TypeCreateParams {
                newTypeOid: new_array_oid,
                typeName: core::str::from_utf8(relarrayname.name_str())
                    .expect("non-UTF-8 array type name"),
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
                defaultValue: None,
                defaultTypeBin: None,
            },
        )?;
    }

    AddNewRelationTuple(
        mcx,
        &pg_class_desc,
        &new_rel_desc,
        relid,
        new_type_oid,
        p.reloftype,
        p.ownerid,
        p.relkind,
        relfrozenxid,
        relminmxid,
        p.relrewrite,
        relacl.as_deref(),
        p.reloptions,
    )?;

    AddNewAttributeTuples(mcx, relid, &new_rel_desc.rd_att, p.relkind)?;

    // Composite types track these dependencies on the pg_type entry instead.
    if p.relkind != types_rel::RELKIND_COMPOSITE_TYPE
        && p.relkind != types_rel::RELKIND_TOASTVALUE
        && !miscinit_seams::is_bootstrap_processing_mode::call()
    {
        let myself = ObjectAddress::set(RELATION_RELATION_ID, relid);
        pg_depend::recordDependencyOnOwner(mcx, RELATION_RELATION_ID, relid, p.ownerid)?;
        if let Some(img) = relacl.as_deref() {
            aclchk_seams::record_dependency_on_new_acl::call(
                mcx,
                RELATION_RELATION_ID,
                relid,
                0,
                p.ownerid,
                img,
            )?;
        }
        pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;
        // C address order: namespace, then reloftype, then access method.
        let mut addrs: [ObjectAddress; 3] =
            [ObjectAddress::set(catalog::NamespaceRelationId, p.relnamespace); 3];
        let mut live = 1;
        if p.reloftype != InvalidOid {
            addrs[live] = ObjectAddress::set(TYPE_RELATION_ID, p.reloftype);
            live += 1;
        }
        if p.accessmtd != InvalidOid {
            addrs[live] = ObjectAddress::set(AccessMethodRelationId, p.accessmtd);
            live += 1;
        }
        pg_depend::record_object_address_dependencies(
            mcx,
            &myself,
            &mut addrs[..live],
            pg_depend::DependencyType::Normal,
        )?;
    }

    objectaccess::InvokeObjectPostCreateHookArg(RELATION_RELATION_ID, relid, 0, p.is_internal)?;

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
            defaultValue: None,
            defaultTypeBin: None,
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
        let mut scan = genam::systable_beginscan(mcx, &attrrel, 2659, true, None, &keys)?;
        let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
            return Err(crate::attribute_lookup_failed(attnum as AttrNumber, relid));
        };
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
        mcx, &attrrel, 2659, // AttributeRelidNumIndexId
        true, None, &keys,
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(crate::attribute_lookup_failed(attnum, rel.rd_id));
    };
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

// SetAttrMissing (heap.c): binary upgrade only; stores a pre-parsed
// attmissingval array literal.
pub fn SetAttrMissing<'mcx>(
    mcx: Mcx<'mcx>,
    relid: Oid,
    attname: &str,
    value: &str,
) -> PgResult<()> {
    let tablerel = table::table_open(mcx, relid, AccessExclusiveLock)?;
    if tablerel.rd_rel.relkind != RELKIND_RELATION {
        return tablerel.close(AccessExclusiveLock);
    }

    let attrrel = table::table_open(mcx, ATTRIBUTE_RELATION_ID, RowExclusiveLock)?;
    let attnum = syscache_seams::lookup_pg_attribute_attnum_by_name::call(relid, attname)?;
    if attnum == 0 {
        return Err(crate::attribute_name_lookup_failed(attname, relid));
    }
    let keys = [
        crate::drop::oid_scankey(1, relid),
        crate::drop::int2_scankey(5, attnum),
    ];
    let mut scan = genam::systable_beginscan(
        mcx, &attrrel, 2659, // AttributeRelidNumIndexId
        true, None, &keys,
    )?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(crate::attribute_name_lookup_failed(attname, relid));
    };
    let desc = attrrel.descr();
    let get = |anum: i32| {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_attribute columns under its descriptor.
        let d = unsafe { types_tuple::heap_getattr(tup, anum, desc, &mut isnull) };
        debug_assert!(!isnull);
        d
    };
    let atttypid = get(3).as_oid();
    let atttypmod = get(6).as_i32();

    let mut cval: mcx::PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, value.len() + 1)?;
    cval.resize(value.len() + 1, 0);
    cval[..value.len()].copy_from_slice(value.as_bytes());
    let mut flinfo = fmgr::FmgrInfo::unresolved();
    fmgr_core::fmgr_info_into(pg_type::F_ARRAY_IN, &mut flinfo)?;
    let missingval = fmgr_core::function_call3_coll_in(
        &mut flinfo,
        InvalidOid,
        mcx,
        Datum::from_usize(cval.as_ptr() as usize),
        Datum::from_oid(atttypid),
        Datum::from_i32(atttypmod),
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
    values[25 - 1] = missingval; // attmissingval
    replace[25 - 1] = true;

    let mut newtup = heaptuple::heap_modify_tuple(mcx, tup, desc, &values, &isnull, &replace)?;
    let otid = tup.t_self;
    genam::systable_endscan(mcx, scan)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &attrrel, &otid, &mut newtup)?;

    attrrel.close(RowExclusiveLock)?;
    tablerel.close(AccessExclusiveLock)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

    // upstream 01db3f0398fd (18.4): CheckAttributeNamesTypes must not run
    // CheckAttributeType() over a dropped column (atttypid == InvalidOid).
    // Pre-fix it did, and only "worked" because every syscache probe with
    // InvalidOid happens to miss; the mocked probe refuses InvalidOid, so the
    // pre-fix loop fails here for exactly that reason.
    #[test]
    fn check_attribute_names_types_skips_dropped_columns() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            syscache_seams::pg_type_typtype::set(|typid| {
                assert!(
                    typid != InvalidOid,
                    "get_typtype(InvalidOid): CheckAttributeType() reached for a dropped column"
                );
                Ok(None)
            });
        });
        let ctx = MemoryContext::new_bump("t");
        let mcx = ctx.mcx();
        let mut dropped = FormData_pg_attribute::default();
        dropped.attname.namestrcpy("........pg.dropped.1........");
        dropped.attnum = 1;
        dropped.atttypid = InvalidOid;
        dropped.attcollation = InvalidOid;
        dropped.attisdropped = true;
        let mut attrs: mcx::PgVec<'_, FormData_pg_attribute> =
            mcx::vec_with_capacity_in(mcx, 1).unwrap();
        attrs.push(dropped);
        let tupdesc = TupleDescData {
            natts: 1,
            tdtypeid: types_core::catalog::RECORDOID,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: mcx::PgVec::new_in(mcx),
            attrs,
        };
        CheckAttributeNamesTypes(mcx, &tupdesc, RELKIND_RELATION, 0).unwrap();
    }
}
