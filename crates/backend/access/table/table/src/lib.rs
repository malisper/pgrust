#![allow(non_snake_case)]

use mcx::Mcx;
use rel_vocab::RangeVar;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_WRONG_OBJECT_TYPE};
use types_rel::{
    Relation, RelationData, LOCKMODE, RELKIND_COMPOSITE_TYPE, RELKIND_INDEX,
    RELKIND_PARTITIONED_INDEX,
};

#[cfg(test)]
mod tests;

pub fn init_seams() {
    table_seams::table_open::set(table_open);
    table_seams::try_table_open::set(try_table_open);
}

pub fn table_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = relation_seams::relation_open::call(mcx, relationId, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

pub fn try_table_open<'mcx>(
    mcx: Mcx<'mcx>,
    relationId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<Option<Relation<'mcx>>> {
    let Some(r) = relation_seams::try_relation_open::call(mcx, relationId, lockmode)? else {
        return Ok(None);
    };

    validate_relation_kind(&r)?;

    Ok(Some(r))
}

pub fn table_openrv<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
) -> PgResult<Relation<'mcx>> {
    let r = relation_seams::relation_openrv::call(mcx, relation, lockmode)?;

    validate_relation_kind(&r)?;

    Ok(r)
}

pub fn table_openrv_extended<'mcx>(
    mcx: Mcx<'mcx>,
    relation: &RangeVar,
    lockmode: LOCKMODE,
    missing_ok: bool,
) -> PgResult<Option<Relation<'mcx>>> {
    let r = relation_seams::relation_openrv_extended::call(mcx, relation, lockmode, missing_ok)?;

    if let Some(r) = &r {
        validate_relation_kind(r)?;
    }

    Ok(r)
}

/// Consumes the handle; a lock held past close is released at xact end, as in C.
pub fn table_close(relation: Relation<'_>, lockmode: LOCKMODE) -> PgResult<()> {
    relation.close(lockmode)
}

fn validate_relation_kind(r: &Relation<'_>) -> PgResult<()> {
    let relkind = r.rd_rel.relkind;

    if relkind == RELKIND_INDEX
        || relkind == RELKIND_PARTITIONED_INDEX
        || relkind == RELKIND_COMPOSITE_TYPE
    {
        return Err(cannot_open_relation(r));
    }

    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_open_relation(r: &RelationData<'_>) -> Box<PgError> {
    let detail = match pg_class_seams::errdetail_relkind_not_supported::call(r.rd_rel.relkind) {
        Ok(detail) => detail,
        Err(e) => return e,
    };
    Box::new(
        PgError::error(format!("cannot open relation \"{}\"", r.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
            .with_detail(detail),
    )
}
