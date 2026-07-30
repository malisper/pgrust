use crate::attribute::name_to_pgstring;
use crate::cache_lookup_error;
use mcx::{Mcx, PgString};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};

// pg_constraint.h
pub const CONSTRAINT_FOREIGN: i8 = b'f' as i8;
pub const CONSTRAINT_PRIMARY: i8 = b'p' as i8;
pub const CONSTRAINT_UNIQUE: i8 = b'u' as i8;
pub const CONSTRAINT_EXCLUSION: i8 = b'x' as i8;

pub fn get_cast_oid(sourcetypeid: Oid, targettypeid: Oid, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::lookup_pg_cast_oid::call(sourcetypeid, targettypeid)?;
    if oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::error(format!(
                "cast from type {} to type {} does not exist",
                format_type::format_type_be(sourcetypeid)?,
                format_type::format_type_be(targettypeid)?
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

pub fn get_collation_name<'mcx>(mcx: Mcx<'mcx>, colloid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_pg_collation_shape::call(colloid)? {
        Some(colltup) => Ok(Some(name_to_pgstring(mcx, &colltup.collname)?)),
        None => Ok(None),
    }
}

pub fn get_collation_isdeterministic(colloid: Oid) -> PgResult<bool> {
    match syscache_seams::lookup_pg_collation_shape::call(colloid)? {
        Some(colltup) => Ok(colltup.collisdeterministic),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for collation {colloid}"
        ))),
    }
}

// collations_agree_on_equality (lsyscache.c): equality compatibility only,
// not ordering. InvalidOid on either side means a non-collation-sensitive
// operation, which cannot conflict; two deterministic collations share the
// byte-wise equality relation.
pub fn collations_agree_on_equality(coll1: Oid, coll2: Oid) -> PgResult<bool> {
    if coll1 == InvalidOid || coll2 == InvalidOid {
        return Ok(true);
    }
    if coll1 == coll2 {
        return Ok(true);
    }
    Ok(get_collation_isdeterministic(coll1)? && get_collation_isdeterministic(coll2)?)
}

pub fn get_constraint_name<'mcx>(mcx: Mcx<'mcx>, conoid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_pg_constraint_shape::call(conoid)? {
        Some(contup) => Ok(Some(name_to_pgstring(mcx, &contup.conname)?)),
        None => Ok(None),
    }
}

pub fn get_constraint_index(conoid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_constraint_shape::call(conoid)? {
        Some(contup)
            if contup.contype == CONSTRAINT_UNIQUE
                || contup.contype == CONSTRAINT_PRIMARY
                || contup.contype == CONSTRAINT_EXCLUSION =>
        {
            contup.conindid
        }
        _ => InvalidOid,
    })
}

pub fn get_constraint_type(conoid: Oid) -> PgResult<i8> {
    match syscache_seams::lookup_pg_constraint_shape::call(conoid)? {
        Some(contup) => Ok(contup.contype),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for constraint {conoid}"
        ))),
    }
}

pub fn get_language_name<'mcx>(
    mcx: Mcx<'mcx>,
    langoid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_pg_language_name::call(langoid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None if missing_ok => Ok(None),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for language {langoid}"
        ))),
    }
}

#[track_caller]
#[cold]
fn opclass_lookup_failed(opclass: Oid) -> Box<PgError> {
    cache_lookup_error(format!("cache lookup failed for opclass {opclass}"))
}

pub fn get_opclass_family(opclass: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_opclass_shape::call(opclass)? {
        Some(cla_tup) => Ok(cla_tup.opcfamily),
        None => Err(opclass_lookup_failed(opclass)),
    }
}

pub fn get_opclass_input_type(opclass: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_opclass_shape::call(opclass)? {
        Some(cla_tup) => Ok(cla_tup.opcintype),
        None => Err(opclass_lookup_failed(opclass)),
    }
}

pub fn get_opclass_opfamily_and_input_type(opclass: Oid) -> PgResult<Option<(Oid, Oid)>> {
    Ok(syscache_seams::lookup_pg_opclass_shape::call(opclass)?
        .map(|cla_tup| (cla_tup.opcfamily, cla_tup.opcintype)))
}

pub fn get_opclass_method(opclass: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_opclass_shape::call(opclass)? {
        Some(cla_tup) => Ok(cla_tup.opcmethod),
        None => Err(opclass_lookup_failed(opclass)),
    }
}

pub fn get_opfamily_method(opfid: Oid) -> PgResult<Oid> {
    match syscache_seams::lookup_pg_opfamily_shape::call(opfid)? {
        Some(opfform) => Ok(opfform.opfmethod),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for operator family {opfid}"
        ))),
    }
}

pub fn get_opfamily_name<'mcx>(
    mcx: Mcx<'mcx>,
    opfid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_pg_opfamily_shape::call(opfid)? {
        Some(opfform) => Ok(Some(name_to_pgstring(mcx, &opfform.opfname)?)),
        None if missing_ok => Ok(None),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for operator family {opfid}"
        ))),
    }
}

pub fn get_transform_fromsql(typid: Oid, langid: Oid, trftypes: &[Oid]) -> PgResult<Oid> {
    if !trftypes.contains(&typid) {
        return Ok(InvalidOid);
    }
    Ok(match syscache_seams::lookup_pg_transform_shape::call(typid, langid)? {
        Some(trf) => trf.trffromsql,
        None => InvalidOid,
    })
}

pub fn get_transform_tosql(typid: Oid, langid: Oid, trftypes: &[Oid]) -> PgResult<Oid> {
    if !trftypes.contains(&typid) {
        return Ok(InvalidOid);
    }
    Ok(match syscache_seams::lookup_pg_transform_shape::call(typid, langid)? {
        Some(trf) => trf.trftosql,
        None => InvalidOid,
    })
}

pub fn get_namespace_name<'mcx>(mcx: Mcx<'mcx>, nspid: Oid) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_namespace_nspname::call(nspid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None => Ok(None),
    }
}

pub fn get_namespace_name_or_temp<'mcx>(
    mcx: Mcx<'mcx>,
    nspid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    if namespace_seams::is_temp_namespace::call(nspid) {
        Ok(Some(PgString::from_str_in("pg_temp", mcx)?))
    } else {
        get_namespace_name(mcx, nspid)
    }
}

pub fn get_range_subtype(range_oid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_range_shape::call(range_oid)? {
        Some(rngtup) => rngtup.rngsubtype,
        None => InvalidOid,
    })
}

pub fn get_range_collation(range_oid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_range_shape::call(range_oid)? {
        Some(rngtup) => rngtup.rngcollation,
        None => InvalidOid,
    })
}

pub fn get_range_multirange(range_oid: Oid) -> PgResult<Oid> {
    Ok(match syscache_seams::lookup_pg_range_shape::call(range_oid)? {
        Some(rngtup) => rngtup.rngmultitypid,
        None => InvalidOid,
    })
}

pub fn get_multirange_range(multirange_oid: Oid) -> PgResult<Oid> {
    Ok(
        match syscache_seams::lookup_pg_range_by_multirange::call(multirange_oid)? {
            Some(rngtypid) => rngtypid,
            None => InvalidOid,
        },
    )
}

pub fn get_publication_oid(pubname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::lookup_pg_publication_oid::call(pubname)?;
    if oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::error(format!("publication \"{pubname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

pub fn get_publication_name<'mcx>(
    mcx: Mcx<'mcx>,
    pubid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_publication_pubname::call(pubid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None if missing_ok => Ok(None),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for publication {pubid}"
        ))),
    }
}

pub fn get_subscription_oid(subname: &str, missing_ok: bool) -> PgResult<Oid> {
    let oid = syscache_seams::lookup_pg_subscription_oid::call(
        init_small::globals::MyDatabaseId(),
        subname,
    )?;
    if oid == InvalidOid && !missing_ok {
        return Err(Box::new(
            PgError::error(format!("subscription \"{subname}\" does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

pub fn get_subscription_name<'mcx>(
    mcx: Mcx<'mcx>,
    subid: Oid,
    missing_ok: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::pg_subscription_subname::call(subid)? {
        Some(name) => Ok(Some(name_to_pgstring(mcx, &name)?)),
        None if missing_ok => Ok(None),
        None => Err(cache_lookup_error(format!(
            "cache lookup failed for subscription {subid}"
        ))),
    }
}
