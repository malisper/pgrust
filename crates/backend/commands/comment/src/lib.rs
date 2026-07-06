//! comment.c (CommentObject over the objectaddress seams + CreateComments /
//! CreateSharedComments / GetComment; the DeleteComments consumer lives in
//! catalog_dependency, DeleteSharedComments in dbcommands).

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::{AttrNumber, InvalidOid, Oid, RegProcedure, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_UNDEFINED_DATABASE, ERRCODE_WRONG_OBJECT_TYPE, ERROR, WARNING,
};
use types_nodes::parsenodes::{CommentStmt, ObjectType};
use types_rel::{
    NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const DescriptionRelationId: Oid = 2609;
const DescriptionObjIndexId: Oid = 2675;
const Natts_pg_description: usize = 4;
const Anum_pg_description_description: usize = 4;
const SharedDescriptionRelationId: Oid = 2396;
const SharedDescriptionObjIndexId: Oid = 2397;
const Natts_pg_shdescription: usize = 3;
const Anum_pg_shdescription_description: usize = 3;
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

pub fn CommentObject<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CommentStmt<'mcx>,
) -> PgResult<objectaddress_seams::ObjectAddr> {
    let object = stmt.object.expect("grammar always supplies the object");

    // pg_restore replays COMMENT ON DATABASE under the dump-time name;
    // wrong-name is a WARNING, not an ERROR.
    if stmt.objtype == ObjectType::OBJECT_DATABASE {
        let database = object.as_string().expect("database object is a String node").sval;
        if dbcommands_seams::get_database_oid::call(mcx, database, true)? == InvalidOid {
            elog_seams::ereport::call(
                PgError::new(WARNING, format!("database \"{database}\" does not exist"))
                    .with_sqlstate(ERRCODE_UNDEFINED_DATABASE),
            )?;
            // C: returns the zero-initialized (invalid) address on this arm.
            return Ok(objectaddress_seams::ObjectAddr {
                classId: InvalidOid,
                objectId: InvalidOid,
                objectSubId: 0,
            });
        }
    }

    let (address, relation) = objectaddress_seams::get_object_address::call(
        mcx,
        stmt.objtype,
        object,
        ShareUpdateExclusiveLock,
        false,
    )?;

    objectaddress_seams::check_object_ownership::call(
        mcx,
        miscinit::GetUserId(),
        stmt.objtype,
        address,
        object,
        relation.as_ref(),
    )?;

    if stmt.objtype == ObjectType::OBJECT_COLUMN {
        let rel = relation.as_ref().expect("column comment carries its relation");
        let relkind = rel.rd_rel.relkind;
        if !matches!(
            relkind,
            RELKIND_RELATION
                | RELKIND_VIEW
                | RELKIND_MATVIEW
                | RELKIND_COMPOSITE_TYPE
                | RELKIND_FOREIGN_TABLE
                | RELKIND_PARTITIONED_TABLE
        ) {
            let detail = pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot set comment on relation \"{}\"", rel.name()),
                )
                .with_detail(detail)
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
    }

    if matches!(
        stmt.objtype,
        ObjectType::OBJECT_DATABASE | ObjectType::OBJECT_TABLESPACE | ObjectType::OBJECT_ROLE
    ) {
        CreateSharedComments(mcx, address.objectId, address.classId, stmt.comment)?;
    } else {
        CreateComments(mcx, address.objectId, address.classId, address.objectSubId, stmt.comment)?;
    }

    if let Some(rel) = relation {
        rel.close(NoLock)?;
    }
    Ok(address)
}

pub fn CreateComments<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    subid: i32,
    comment: Option<&str>,
) -> PgResult<()> {
    let comment = match comment {
        Some("") => None,
        c => c,
    };
    let description = table::table_open(mcx, DescriptionRelationId, RowExclusiveLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(oid)),
        eq_key(2, F_OIDEQ, Datum::from_oid(classoid)),
        eq_key(3, F_INT4EQ, Datum::from_i32(subid)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &description, DescriptionObjIndexId, true, None, &keys)?;

    let text = match comment {
        Some(c) => Some(varlena::cstring_to_text(mcx, c.as_bytes())?),
        None => None,
    };
    let text_datum =
        text.as_ref().map(|t| Datum::from_usize(t.as_bytes().as_ptr() as usize));

    let old = genam::systable_getnext(mcx, &mut scan)?;
    match (old, text_datum) {
        (Some(oldtup), None) => {
            let tid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleDelete(&description, &tid)?;
        }
        (Some(oldtup), Some(d)) => {
            let mut values = [Datum::null(); Natts_pg_description];
            let isnull = [false; Natts_pg_description];
            let replace = [true; Natts_pg_description];
            values[0] = Datum::from_oid(oid);
            values[1] = Datum::from_oid(classoid);
            values[2] = Datum::from_i32(subid);
            values[Anum_pg_description_description - 1] = d;
            let mut newtup = heaptuple::heap_modify_tuple(
                mcx,
                oldtup,
                description.descr(),
                &values,
                &isnull,
                &replace,
            )?;
            let otid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &description, &otid, &mut newtup)?;
        }
        (None, Some(d)) => {
            genam::systable_endscan(mcx, scan)?;
            let values = [
                Datum::from_oid(oid),
                Datum::from_oid(classoid),
                Datum::from_i32(subid),
                d,
            ];
            let nulls = [false; Natts_pg_description];
            let mut tup =
                heaptuple::heap_form_tuple(mcx, description.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &description, &mut tup)?;
        }
        (None, None) => genam::systable_endscan(mcx, scan)?,
    }
    description.close(NoLock)
}

pub fn CreateSharedComments<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    comment: Option<&str>,
) -> PgResult<()> {
    let comment = match comment {
        Some("") => None,
        c => c,
    };
    let shdescription =
        table::table_open(mcx, SharedDescriptionRelationId, RowExclusiveLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(oid)),
        eq_key(2, F_OIDEQ, Datum::from_oid(classoid)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &shdescription,
        SharedDescriptionObjIndexId,
        true,
        None,
        &keys,
    )?;

    let text = match comment {
        Some(c) => Some(varlena::cstring_to_text(mcx, c.as_bytes())?),
        None => None,
    };
    let text_datum =
        text.as_ref().map(|t| Datum::from_usize(t.as_bytes().as_ptr() as usize));

    let old = genam::systable_getnext(mcx, &mut scan)?;
    match (old, text_datum) {
        (Some(oldtup), None) => {
            let tid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleDelete(&shdescription, &tid)?;
        }
        (Some(oldtup), Some(d)) => {
            let mut values = [Datum::null(); Natts_pg_shdescription];
            let isnull = [false; Natts_pg_shdescription];
            let replace = [true; Natts_pg_shdescription];
            values[0] = Datum::from_oid(oid);
            values[1] = Datum::from_oid(classoid);
            values[Anum_pg_shdescription_description - 1] = d;
            let mut newtup = heaptuple::heap_modify_tuple(
                mcx,
                oldtup,
                shdescription.descr(),
                &values,
                &isnull,
                &replace,
            )?;
            let otid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &shdescription, &otid, &mut newtup)?;
        }
        (None, Some(d)) => {
            genam::systable_endscan(mcx, scan)?;
            let values = [Datum::from_oid(oid), Datum::from_oid(classoid), d];
            let nulls = [false; Natts_pg_shdescription];
            let mut tup =
                heaptuple::heap_form_tuple(mcx, shdescription.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &shdescription, &mut tup)?;
        }
        (None, None) => genam::systable_endscan(mcx, scan)?,
    }
    shdescription.close(NoLock)
}

// GetComment (comment.c): pg_description text for (oid, classoid, subid).
pub fn GetComment<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    subid: i32,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let description = table::table_open(mcx, DescriptionRelationId, types_rel::AccessShareLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(oid)),
        eq_key(2, F_OIDEQ, Datum::from_oid(classoid)),
        eq_key(3, F_INT4EQ, Datum::from_i32(subid)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &description, DescriptionObjIndexId, true, None, &keys)?;
    let comment = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => {
            let mut isnull = false;
            // SAFETY: pg_description.description under its own descriptor.
            let d = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_description_description as i32,
                    description.descr(),
                    &mut isnull,
                )
            };
            debug_assert!(!isnull);
            let p = d.as_usize() as *const u8;
            // SAFETY: not-null text column: live varlena image through its extent.
            let image =
                unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
            let payload = varlena::open_image(mcx, image)?;
            let s = core::str::from_utf8(payload.as_bytes()).expect("comment UTF-8");
            Some(mcx::PgString::from_str_in(s, mcx)?)
        }
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    description.close(types_rel::AccessShareLock)?;
    Ok(comment)
}
