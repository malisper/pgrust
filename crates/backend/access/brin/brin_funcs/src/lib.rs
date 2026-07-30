//! brin.c SQL maintenance entry points: brin_summarize_new_values,
//! brin_summarize_range, brin_desummarize_range.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::types_brin::BRIN_ALL_BLOCKRANGES;
use ::types_core::{
    BlockNumber, InvalidOid, MaxBlockNumber, Oid, BRIN_AM_OID, RELATION_RELATION_ID,
};
use ::types_error::{
    PgError, PgResult, DEBUG1, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_TABLE, ERRCODE_WRONG_OBJECT_TYPE,
};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use ::types_nodes::parsenodes::ObjectType;
use ::types_rel::{Relation, ShareUpdateExclusiveLock, RELKIND_INDEX};

#[track_caller]
#[cold]
#[inline(never)]
fn recovery_in_progress_error() -> Box<PgError> {
    Box::new(
        PgError::error("recovery is in progress")
            .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .with_hint("BRIN control functions cannot be executed during recovery."),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn block_number_out_of_range(heapBlk64: i64) -> Box<PgError> {
    Box::new(
        PgError::error(format!("block number out of range: {heapBlk64}"))
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_a_brin_index(indexRel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{}\" is not a BRIN index", indexRel.name()))
            .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn no_parent_table(indexRel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "could not open parent table of index \"{}\"",
            indexRel.name()
        ))
        .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
    )
}

fn is_brin_index(indexRel: &Relation<'_>) -> bool {
    indexRel.rd_rel.relkind == RELKIND_INDEX && indexRel.rd_rel.relam == BRIN_AM_OID
}

// brin_summarize_range (brin.c). Shared by brin_summarize_new_values, whose C
// body is DirectFunctionCall2 with BRIN_ALL_BLOCKRANGES.
fn brin_summarize_range_impl(
    fcinfo: &mut Fcinfo,
    indexoid: Oid,
    heapBlk64: i64,
) -> PgResult<Datum> {
    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    if heapBlk64 > BRIN_ALL_BLOCKRANGES as i64 || heapBlk64 < 0 {
        return Err(block_number_out_of_range(heapBlk64));
    }
    let heapBlk = heapBlk64 as BlockNumber;

    let mcx = fcinfo.result_mcx();

    // Lock table before index to avoid deadlocks; if indexoid isn't an index,
    // postpone complaining until the is-it-an-index test below.
    let heapoid = catalog_index::IndexGetRelation(mcx, indexoid, true)?;
    let (heapRel, saved) = if heapoid != InvalidOid {
        let heapRel = table::table_open(mcx, heapoid, ShareUpdateExclusiveLock)?;
        // Autovacuum calls us: run index functions as the table owner, under
        // a security-restricted operation with command-local GUC changes.
        let guard = miscinit::SecContextGuard::security_restricted(heapRel.rd_rel.relowner);
        let save_nestlevel = guc::NewGUCNestLevel();
        guc::RestrictSearchPath()?;
        (Some(heapRel), Some((guard, save_nestlevel)))
    } else {
        (None, None)
    };

    let indexRel = indexam::index_open(mcx, indexoid, ShareUpdateExclusiveLock)?;

    if !is_brin_index(&indexRel) {
        return Err(not_a_brin_index(&indexRel));
    }

    // User must own the index (comparable to privileges needed for VACUUM).
    if let Some((guard, _)) = &saved {
        let save_userid = guard.saved().0;
        if !aclchk::object_ownercheck(RELATION_RELATION_ID, indexoid, save_userid)? {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                ObjectType::OBJECT_INDEX,
                indexRel.name(),
            )?;
        }
    }

    // The unlocked IndexGetRelation above could have raced an index
    // drop/recreation onto the wrong table: recheck.
    if heapRel.is_none() || heapoid != catalog_index::IndexGetRelation(mcx, indexoid, false)? {
        return Err(no_parent_table(&indexRel));
    }
    let heapRel = heapRel.expect("checked above");

    let mut numSummarized = 0.0f64;
    if indexRel.rd_index.as_ref().expect("index").indisvalid {
        brin_build::brinsummarize(
            mcx,
            &indexRel,
            &heapRel,
            heapBlk,
            true,
            Some(&mut numSummarized),
            None,
        )?;
    } else {
        let _ = elog::elog(
            DEBUG1,
            format!("index \"{}\" is not valid", indexRel.name()),
        );
    }

    let (guard, save_nestlevel) = saved.expect("heapRel was opened");
    guc::AtEOXact_GUC(false, save_nestlevel);
    guard.restore();

    indexRel.close(ShareUpdateExclusiveLock)?;
    heapRel.close(ShareUpdateExclusiveLock)?;

    Ok(Datum::from_i32(numSummarized as i32))
}

pub fn fc_brin_summarize_new_values(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let indexoid = fcinfo.arg(0).as_oid();
    brin_summarize_range_impl(fcinfo, indexoid, BRIN_ALL_BLOCKRANGES as i64)
}

pub fn fc_brin_summarize_range(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let indexoid = fcinfo.arg(0).as_oid();
    let heapBlk64 = fcinfo.arg(1).as_i64();
    brin_summarize_range_impl(fcinfo, indexoid, heapBlk64)
}

/// brin_desummarize_range (brin.c).
pub fn fc_brin_desummarize_range(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let indexoid = fcinfo.arg(0).as_oid();
    let heapBlk64 = fcinfo.arg(1).as_i64();

    if transam_xlog::RecoveryInProgress() {
        return Err(recovery_in_progress_error());
    }

    if heapBlk64 > MaxBlockNumber as i64 || heapBlk64 < 0 {
        return Err(block_number_out_of_range(heapBlk64));
    }
    let heapBlk = heapBlk64 as BlockNumber;

    let mcx = fcinfo.result_mcx();

    // Lock table before index, as above. Autovacuum never calls this, so no
    // userid switch.
    let heapoid = catalog_index::IndexGetRelation(mcx, indexoid, true)?;
    let heapRel = if heapoid != InvalidOid {
        Some(table::table_open(mcx, heapoid, ShareUpdateExclusiveLock)?)
    } else {
        None
    };

    let indexRel = indexam::index_open(mcx, indexoid, ShareUpdateExclusiveLock)?;

    if !is_brin_index(&indexRel) {
        return Err(not_a_brin_index(&indexRel));
    }

    if !aclchk::object_ownercheck(RELATION_RELATION_ID, indexoid, miscinit::GetUserId())? {
        aclchk::aclcheck_error(
            aclchk::ACLCHECK_NOT_OWNER,
            ObjectType::OBJECT_INDEX,
            indexRel.name(),
        )?;
    }

    if heapRel.is_none() || heapoid != catalog_index::IndexGetRelation(mcx, indexoid, false)? {
        return Err(no_parent_table(&indexRel));
    }
    let heapRel = heapRel.expect("checked above");

    if indexRel.rd_index.as_ref().expect("index").indisvalid {
        loop {
            if brin_pageops::brinRevmapDesummarizeRange(&indexRel, heapBlk)? {
                break;
            }
        }
    } else {
        let _ = elog::elog(
            DEBUG1,
            format!("index \"{}\" is not valid", indexRel.name()),
        );
    }

    indexRel.close(ShareUpdateExclusiveLock)?;
    heapRel.close(ShareUpdateExclusiveLock)?;

    Ok(Datum::null())
}

#[cfg(test)]
mod tests {
    use super::BRIN_FUNCS_BUILTINS;

    // Rows must carry pg_proc.dat's metadata: fmgr resolves fn_nargs/strict
    // from the registered row when the canonical entry is a stub.
    #[test]
    fn builtin_rows_match_pg_proc() {
        let expect = [
            (3952, "brin_summarize_new_values", 1),
            (3999, "brin_summarize_range", 2),
            (4014, "brin_desummarize_range", 2),
        ];
        assert_eq!(BRIN_FUNCS_BUILTINS.len(), expect.len());
        for (b, (foid, name, nargs)) in BRIN_FUNCS_BUILTINS.iter().zip(expect) {
            assert_eq!(b.foid, foid);
            assert_eq!(b.name, name);
            assert_eq!(b.nargs, nargs);
            assert!(b.strict);
            assert!(!b.retset);
        }
    }
}

pub static BRIN_FUNCS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3952,
        name: "brin_summarize_new_values",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_brin_summarize_new_values as PGFunction,
    },
    FmgrBuiltin {
        foid: 3999,
        name: "brin_summarize_range",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_brin_summarize_range as PGFunction,
    },
    FmgrBuiltin {
        foid: 4014,
        name: "brin_desummarize_range",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_brin_desummarize_range as PGFunction,
    },
];
