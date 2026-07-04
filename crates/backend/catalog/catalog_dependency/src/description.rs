// getObjectDescription (objectaddress.c), pg_class + pg_constraint + pg_policy
// arms only; every other object class is loud.
use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{AttrNumber, InvalidOid, Oid, RELATION_RELATION_ID};
use types_error::{PgError, PgResult};
use types_rel::pg_class::{
    RELKIND_COMPOSITE_TYPE, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_TOASTVALUE, RELKIND_VIEW,
};

#[cold]
#[inline(never)]
fn cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for relation {relid}")))
}

pub fn getObjectDescription<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
) -> PgResult<Option<String>> {
    match object.classId {
        RELATION_RELATION_ID => {
            if object.objectSubId == 0 {
                Ok(Some(getRelationDescription(mcx, object.objectId)?))
            } else {
                let attname = lsyscache::attribute::get_attname(
                    mcx,
                    object.objectId,
                    object.objectSubId as AttrNumber,
                    false,
                )?
                .expect("missing_ok=false");
                let rel = getRelationDescription(mcx, object.objectId)?;
                Ok(Some(format!("column {attname} of {rel}")))
            }
        }
        3256 => {
            let (polname, polrelid) = policy_name_rel(mcx, object.objectId)?;
            let rel = getRelationDescription(mcx, polrelid)?;
            Ok(Some(format!("policy {polname} on {rel}")))
        }
        ConstraintRelationId => {
            let (conname, conrelid) = constraint_name_and_rel(mcx, object.objectId)?;
            if conrelid != InvalidOid {
                Ok(Some(format!(
                    "constraint {} on {}",
                    conname,
                    getRelationDescription(mcx, conrelid)?
                )))
            } else {
                Ok(Some(format!("constraint {conname}")))
            }
        }
        other => panic!("unported: objectaddress.c getObjectDescription class {other}"),
    }
}

// getObjectDescription's PolicyRelationId arm (objectaddress.c): polname +
// polrelid off the pg_policy row.
fn policy_name_rel<'mcx>(mcx: Mcx<'mcx>, policy_id: Oid) -> PgResult<(String, Oid)> {
    const POLICY_RELATION_ID: Oid = 3256;
    const POLICY_OID_INDEX_ID: Oid = 3257;
    let rel = table::table_open(mcx, POLICY_RELATION_ID, types_rel::AccessShareLock)?;
    let keys = [crate::oid_key(1, policy_id)];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, POLICY_OID_INDEX_ID, true, None, &keys)?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(Box::new(PgError::error(format!(
            "could not find tuple for policy {policy_id}"
        ))));
    };
    let td = rel.descr();
    let mut isnull = false;
    // SAFETY: pg_policy row read under its relation's descriptor.
    let (name_d, relid_d) = unsafe {
        (
            types_tuple::heap_getattr(tup, 2, td, &mut isnull),
            types_tuple::heap_getattr(tup, 3, td, &mut isnull),
        )
    };
    // SAFETY: a non-null pg_policy name column is a 64-byte NameData image.
    let bytes = unsafe { core::slice::from_raw_parts(name_d.as_usize() as *const u8, 64) };
    let len = bytes.iter().position(|&b| b == 0).unwrap_or(64);
    let polname = String::from_utf8_lossy(&bytes[..len]).into_owned();
    let polrelid = relid_d.as_oid();
    genam::systable_endscan(mcx, scan)?;
    rel.close(types_rel::AccessShareLock)?;
    Ok((polname, polrelid))
}

const ConstraintRelationId: Oid = 2606;

fn constraint_name_and_rel<'mcx>(mcx: Mcx<'mcx>, con_id: Oid) -> PgResult<(String, Oid)> {
    let con_rel = table::table_open(mcx, ConstraintRelationId, types_rel::AccessShareLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = pg_constraint::Anum_pg_constraint_oid;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(oideq) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(con_id);
    let keys = [key];
    let mut scan = genam::systable_beginscan(
        mcx,
        &con_rel,
        types_core::CONSTRAINT_OID_INDEX_ID,
        true,
        None,
        &keys,
    )?;
    let tup = genam::systable_getnext(mcx, &mut scan)?
        .unwrap_or_else(|| panic!("cache lookup failed for constraint {con_id}"));
    let desc = con_rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed NOT NULL pg_constraint columns under its descriptor.
    let name_d = unsafe {
        types_tuple::heap_getattr(
            tup,
            pg_constraint::Anum_pg_constraint_conname as i32,
            desc,
            &mut isnull,
        )
    };
    // SAFETY: conname is an inline NameData (64 NUL-padded bytes).
    let namebytes = unsafe { core::slice::from_raw_parts(name_d.as_usize() as *const u8, 64) };
    let len = namebytes.iter().position(|&b| b == 0).unwrap_or(64);
    let conname = core::str::from_utf8(&namebytes[..len])
        .expect("conname UTF-8")
        .to_string();
    // SAFETY: as above.
    let conrelid = unsafe {
        types_tuple::heap_getattr(
            tup,
            pg_constraint::Anum_pg_constraint_conrelid as i32,
            desc,
            &mut isnull,
        )
    }
    .as_oid();
    genam::systable_endscan(mcx, scan)?;
    con_rel.close(types_rel::AccessShareLock)?;
    Ok((conname, conrelid))
}

fn getRelationDescription<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<String> {
    let Some(relname) = lsyscache::relation::get_rel_name(mcx, relid)? else {
        return Err(cache_lookup_failed(relid));
    };
    let relkind = lsyscache::relation::get_rel_relkind(relid)? as u8;

    // RelationIsVisible: visible iff an unqualified lookup along the active
    // search path resolves to this relation.
    let nspname = if catalog_namespace::RelnameGetRelid(&relname)? == relid {
        None
    } else {
        let nsp = lsyscache::relation::get_rel_namespace(relid)?;
        lsyscache::misc::get_namespace_name(mcx, nsp)?
    };
    let qualified = match &nspname {
        Some(nsp) => format!(
            "{}.{}",
            format_type::quote_identifier(nsp),
            format_type::quote_identifier(&relname)
        ),
        None => format_type::quote_identifier(&relname).into_owned(),
    };

    let noun = match relkind {
        RELKIND_RELATION | RELKIND_PARTITIONED_TABLE => "table",
        RELKIND_INDEX | RELKIND_PARTITIONED_INDEX => "index",
        RELKIND_SEQUENCE => "sequence",
        RELKIND_TOASTVALUE => "toast table",
        RELKIND_VIEW => "view",
        RELKIND_MATVIEW => "materialized view",
        RELKIND_COMPOSITE_TYPE => "composite type",
        RELKIND_FOREIGN_TABLE => "foreign table",
        _ => "relation",
    };
    Ok(format!("{noun} {qualified}"))
}
