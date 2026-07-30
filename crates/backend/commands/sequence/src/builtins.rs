use datum::Datum;
use mcx::Mcx;
use types_core::{Oid, BOOLOID, INT8OID, RECORDOID};
use types_error::{PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_rel::{AccessShareLock, NoLock, RELKIND_SEQUENCE};

use adt_acl::{ACL_SELECT, ACL_UPDATE, ACL_USAGE};

use crate::{err, fc_mcx, init_sequence, pgs_form, read_seq_tuple};

pub(crate) fn register_builtins() {
    fmgr_core::register_late_builtins(SEQUENCE_INTROSPECT_BUILTINS);
}

static SEQUENCE_INTROSPECT_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3078,
        name: "pg_sequence_parameters",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_sequence_parameters,
    },
    FmgrBuiltin {
        foid: 4032,
        name: "pg_sequence_last_value",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_sequence_last_value,
    },
    FmgrBuiltin {
        foid: 6427,
        name: "pg_get_sequence_data",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_get_sequence_data,
    },
];

fn composite_datum(
    mcx: Mcx<'_>,
    desc: &types_tuple::TupleDescData<'_>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Datum> {
    let tup = heaptuple::heap_form_tuple(mcx, desc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

#[track_caller]
#[cold]
fn permission_denied(relid: Oid) -> Box<PgError> {
    let name = lsyscache::relation::get_rel_name(fc_mcx(), relid)
        .ok()
        .flatten()
        .map(|n| n.as_str().to_string())
        .unwrap_or_else(|| format!("{relid}"));
    err(
        format!("permission denied for sequence {name}"),
        ERRCODE_INSUFFICIENT_PRIVILEGE,
    )
}

fn fc_pg_sequence_parameters(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_sequence_parameters: resolved FmgrInfo required");
    let relid = fcinfo.arg_oid(0);

    if aclchk::pg_class_aclcheck(
        relid,
        miscinit::GetUserId(),
        ACL_SELECT | ACL_UPDATE | ACL_USAGE,
    )? != aclchk::ACLCHECK_OK
    {
        return Err(permission_denied(relid));
    }

    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let desc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let form = pgs_form(relid)?;
    let values = [
        Datum::from_i64(form.seqstart),
        Datum::from_i64(form.seqmin),
        Datum::from_i64(form.seqmax),
        Datum::from_i64(form.seqincrement),
        Datum::from_bool(form.seqcycle),
        Datum::from_i64(form.seqcache),
        Datum::from_oid(form.seqtypid),
    ];
    composite_datum(mcx, &desc, &values, &[false; 7])
}

fn fc_pg_sequence_last_value(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    let seqrel = init_sequence(fc_mcx(), relid)?;

    let mut is_called = false;
    let mut result = 0i64;
    // sequence.c:1866-1869. The other-temp conjunct is why C returns NULL here
    // rather than erroring: it is C's stated defense against a direct call on
    // another session's temp sequence, whose pages we cannot read.
    if aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT | ACL_USAGE)?
        == aclchk::ACLCHECK_OK
        && !seqrel.is_other_temp()
        && (seqrel.is_permanent() || !transam_xlog_seams::recovery_in_progress::call())
    {
        let (buf, seq) = read_seq_tuple(&seqrel)?;
        is_called = seq.is_called();
        result = seq.last_value();
        bufmgr::UnlockReleaseBuffer(buf)?;
    }
    seqrel.close(NoLock)?;

    if is_called {
        Ok(Datum::from_i64(result))
    } else {
        Ok(fcinfo.return_null())
    }
}

fn fc_pg_get_sequence_data(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let relid = fcinfo.arg_oid(0);
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("last_value"), INT8OID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("is_called"), BOOLOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut values = [Datum::from_i64(0), Datum::from_bool(false)];
    let mut isnull = [false; 2];

    let seqrel = relation::try_relation_open(mcx, relid, AccessShareLock)?;
    let mut filled = false;
    // sequence.c:1811-1814: all-NULLs for missing sequences, ones we lack
    // privileges on, OTHER SESSIONS' temporary sequences, and unlogged
    // sequences on standbys.
    if let Some(rel) = &seqrel {
        if rel.rd_rel.relkind == RELKIND_SEQUENCE
            && aclchk::pg_class_aclcheck(relid, miscinit::GetUserId(), ACL_SELECT)?
                == aclchk::ACLCHECK_OK
            && !rel.is_other_temp()
            && (rel.is_permanent() || !transam_xlog_seams::recovery_in_progress::call())
        {
            let (buf, seq) = read_seq_tuple(rel)?;
            values[0] = Datum::from_i64(seq.last_value());
            values[1] = Datum::from_bool(seq.is_called());
            bufmgr::UnlockReleaseBuffer(buf)?;
            filled = true;
        }
    }
    if !filled {
        isnull = [true, true];
    }
    if let Some(rel) = seqrel {
        rel.close(AccessShareLock)?;
    }

    composite_datum(mcx, &desc, &values, &isnull)
}
