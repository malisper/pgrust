// getObjectDescription (objectaddress.c), pg_class arm only; every other
// object class is loud.
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::{AttrNumber, Oid, RELATION_RELATION_ID};
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
        other => panic!("unported: objectaddress.c getObjectDescription class {other}"),
    }
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
