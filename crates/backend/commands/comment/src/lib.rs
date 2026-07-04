//! comment.c, TABLE/COLUMN lanes (CommentObject + CreateComments; the shared
//! DeleteComments consumer lives in catalog_dependency).

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::Mcx;
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::{AttrNumber, Oid, RegProcedure, RELATION_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_UNDEFINED_COLUMN, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::{CommentStmt, ObjectType};
use types_nodes::NodeList;
use types_rel::{
    NoLock, RowExclusiveLock, ShareUpdateExclusiveLock, RELKIND_PARTITIONED_TABLE,
    RELKIND_RELATION, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use rel_vocab::RangeVar;

const DescriptionRelationId: Oid = 2609;
const DescriptionObjIndexId: Oid = 2675;
const Natts_pg_description: usize = 4;
const Anum_pg_description_description: usize = 4;
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported: comment {what}")
}

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

fn range_var_from_parts<'mcx>(parts: &[&'mcx str]) -> RangeVar<'mcx> {
    let mut rv = RangeVar {
        catalogname: None,
        schemaname: None,
        relname: "",
        inh: true,
        relpersistence: types_core::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    match parts {
        [r] => rv.relname = r,
        [s, r] => {
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        [c, s, r] => {
            rv.catalogname = Some(c);
            rv.schemaname = Some(s);
            rv.relname = r;
        }
        _ => panic!("improper relation name (too many dotted names)"),
    }
    rv
}

fn name_parts<'mcx>(names: &NodeList<'mcx>) -> Vec<&'mcx str> {
    names
        .iter()
        .map(|n| n.as_string().expect("object name component is a String node").sval)
        .collect()
}

pub fn CommentObject<'mcx>(mcx: Mcx<'mcx>, stmt: &CommentStmt<'mcx>) -> PgResult<()> {
    if matches!(
        stmt.objtype,
        ObjectType::OBJECT_PUBLICATION | ObjectType::OBJECT_SUBSCRIPTION
    ) {
        return comment_by_name_object(mcx, stmt);
    }
    let names = stmt
        .object
        .expect("grammar always supplies the object")
        .as_list()
        .expect("TABLE/COLUMN comment object is a name list");
    let parts = name_parts(&names);

    let (rv_parts, attname) = match stmt.objtype {
        ObjectType::OBJECT_TABLE => (parts.as_slice(), None),
        ObjectType::OBJECT_COLUMN => {
            let (last, rel_parts) =
                parts.split_last().expect("column comment object has at least two parts");
            (rel_parts, Some(*last))
        }
        other => unported(&format!("CommentObject: objtype {other:?}")),
    };
    let rv = range_var_from_parts(rv_parts);

    // get_object_address with ShareUpdateExclusiveLock, TABLE/COLUMN arms.
    let relid = catalog_namespace::RangeVarGetRelid(&rv, ShareUpdateExclusiveLock, false)?;
    let rel = table::table_open(mcx, relid, NoLock)?;

    let relkind = rel.rd_rel.relkind;
    if stmt.objtype == ObjectType::OBJECT_TABLE
        && !matches!(relkind, RELKIND_RELATION | RELKIND_PARTITIONED_TABLE)
    {
        return Err(Box::new(
            PgError::new(ERROR, format!("\"{}\" is not a table", rv.relname))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    // check_object_ownership (objectaddress.c): superuser fast path; role
    // ownership walks are the unported remainder.
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        unported("CommentObject: check_object_ownership for non-superusers");
    }

    let objsubid = match attname {
        None => 0,
        Some(attname) => {
            if !matches!(
                relkind,
                RELKIND_RELATION
                    | RELKIND_VIEW
                    | RELKIND_MATVIEW
                    | RELKIND_COMPOSITE_TYPE
                    | RELKIND_FOREIGN_TABLE
                    | RELKIND_PARTITIONED_TABLE
            ) {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "\"{}\" is not a table, view, materialized view, composite type, or foreign table",
                            rel.name()
                        ),
                    )
                    .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            let attnum = parse_relation::attnameAttNum(&rel, attname, false);
            if attnum == 0 {
                return Err(Box::new(
                    PgError::new(
                        ERROR,
                        format!(
                            "column \"{attname}\" of relation \"{}\" does not exist",
                            rel.name()
                        ),
                    )
                    .with_sqlstate(ERRCODE_UNDEFINED_COLUMN),
                ));
            }
            attnum as i32
        }
    };

    let comment = match stmt.comment {
        Some("") => None,
        c => c,
    };
    CreateComments(mcx, relid, RELATION_RELATION_ID, objsubid, comment)?;
    rel.close(NoLock)
}

const PublicationRelationId: Oid = 6104;
const SubscriptionRelationId: Oid = 6100;

// get_object_address unqualified-name arm + CreateComments for the classes
// without a namespace (publication, subscription).
fn comment_by_name_object<'mcx>(mcx: Mcx<'mcx>, stmt: &CommentStmt<'mcx>) -> PgResult<()> {
    let name = stmt
        .object
        .expect("grammar always supplies the object")
        .as_string()
        .expect("publication/subscription comment object is a name")
        .sval;
    let (classid, objid) = match stmt.objtype {
        ObjectType::OBJECT_PUBLICATION => {
            (PublicationRelationId, lsyscache::get_publication_oid(name, false)?)
        }
        ObjectType::OBJECT_SUBSCRIPTION => {
            (SubscriptionRelationId, lsyscache::get_subscription_oid(name, false)?)
        }
        _ => unreachable!(),
    };

    if classid == SubscriptionRelationId {
        lmgr::LockSharedObject(classid, objid, 0, ShareUpdateExclusiveLock)?;
    } else {
        lmgr::LockDatabaseObject(classid, objid, 0, ShareUpdateExclusiveLock)?;
    }

    // check_object_ownership (objectaddress.c): superuser fast path; role
    // ownership walks are the unported remainder.
    if !superuser::superuser_arg(miscinit::GetUserId())? {
        unported("CommentObject: check_object_ownership for non-superusers");
    }

    let comment = match stmt.comment {
        Some("") => None,
        c => c,
    };
    CreateComments(mcx, objid, classid, 0, comment)
}

pub fn CreateComments<'mcx>(
    mcx: Mcx<'mcx>,
    oid: Oid,
    classoid: Oid,
    subid: i32,
    comment: Option<&str>,
) -> PgResult<()> {
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
            let mut isnull = [false; Natts_pg_description];
            let mut replace = [false; Natts_pg_description];
            values[Anum_pg_description_description - 1] = d;
            replace[Anum_pg_description_description - 1] = true;
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
