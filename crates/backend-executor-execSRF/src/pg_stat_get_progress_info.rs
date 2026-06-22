//! `pg_stat_get_progress_info(text)` (OID 3318) — the backing materialize-mode
//! SRF for the `pg_stat_progress_*` views (pgstatfuncs.c:255).
//!
//! Translates the command-name text argument ("VACUUM"/"ANALYZE"/"CLUSTER"/
//! "CREATE INDEX"/"BASEBACKUP"/"COPY") into a `ProgressCommandType`, then walks
//! the per-backend `localBackendStatusTable` snapshot and emits one row per
//! backend currently running that command. Each row is `pid, datid,
//! relid + PGSTAT_NUM_PROGRESS_PARAM` parameter columns (`PG_STAT_GET_PROGRESS_COLS
//! = PGSTAT_NUM_PROGRESS_PARAM + 3`). The relid + parameter columns are emitted
//! only to role members / `pg_read_all_stats` (`HAS_PGSTAT_PERMISSIONS`);
//! otherwise they are NULL.

extern crate alloc;
use alloc::vec::Vec;

use types_core::Oid;
use types_datum::varlena::VARHDRSZ;
use types_error::error::ERRCODE_INVALID_PARAMETER_VALUE;
use types_error::{PgError, PgResult};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_pgstat::backend_progress::{ProgressCommandType, PGSTAT_NUM_PROGRESS_PARAM};
use types_tuple::backend_access_common_heaptuple::Datum;

use backend_utils_activity_status as status;
use backend_utils_activity_status::{LocalBackendStatusFields, LocalPgBackendStatus};
use backend_utils_fmgr_funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_stat_get_progress_info(text)` (OID 3318).
const PG_STAT_GET_PROGRESS_INFO: Oid = 3318;

/// `#define PG_STAT_GET_PROGRESS_COLS PGSTAT_NUM_PROGRESS_PARAM + 3`.
const PG_STAT_GET_PROGRESS_COLS: usize = PGSTAT_NUM_PROGRESS_PARAM + 3;

/// `ROLE_PG_READ_ALL_STATS` (pg_authid.dat) — predefined role OID 3375.
const ROLE_PG_READ_ALL_STATS: Oid = 3375;

/// Register `pg_stat_get_progress_info` in the executor-frame SRF table.
pub(crate) fn register_pg_stat_get_progress_info() {
    register_srf(PG_STAT_GET_PROGRESS_INFO, pg_stat_get_progress_info);
}

/// `HAS_PGSTAT_PERMISSIONS(role)` (pgstatfuncs.c:37).
fn has_pgstat_permissions(role: Oid) -> PgResult<bool> {
    let user = backend_utils_init_miscinit::GetUserId();
    if backend_utils_adt_acl_seams::has_privs_of_role::call(user, ROLE_PG_READ_ALL_STATS)? {
        return Ok(true);
    }
    backend_utils_adt_acl_seams::has_privs_of_role::call(user, role)
}

/// `text_to_cstring(PG_GETARG_TEXT_PP(0))` — the command-name text arg's
/// `VARDATA_ANY` payload on the by-ref lane (header-ful image; skip the 4-byte
/// varlena header), decoded as UTF-8.
fn arg_text<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("pg_stat_get_progress_info: text arg missing from by-ref lane");
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set)
    // header, else `VARHDRSZ`. No-op while `SHORT_VARLENA_PACKING` is off.
    let bytes: &[u8] = match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    };
    core::str::from_utf8(bytes).expect("pg_stat_get_progress_info: text arg not valid UTF-8")
}

/// `pg_stat_get_progress_info(PG_FUNCTION_ARGS)` (pgstatfuncs.c:255) over the
/// executor frame.
fn pg_stat_get_progress_info<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    // int num_backends = pgstat_fetch_stat_numbackends();
    let num_backends = status::pgstat_fetch_stat_numbackends();

    // char *cmd = text_to_cstring(PG_GETARG_TEXT_PP(0));
    let cmd = arg_text(fcinfo, 0);

    // Translate command name into command type code (pg_strcasecmp).
    let cmdtype = if cmd.eq_ignore_ascii_case("VACUUM") {
        ProgressCommandType::Vacuum
    } else if cmd.eq_ignore_ascii_case("ANALYZE") {
        ProgressCommandType::Analyze
    } else if cmd.eq_ignore_ascii_case("CLUSTER") {
        ProgressCommandType::Cluster
    } else if cmd.eq_ignore_ascii_case("CREATE INDEX") {
        ProgressCommandType::CreateIndex
    } else if cmd.eq_ignore_ascii_case("BASEBACKUP") {
        ProgressCommandType::Basebackup
    } else if cmd.eq_ignore_ascii_case("COPY") {
        ProgressCommandType::Copy
    } else {
        return Err(
            PgError::error(alloc::format!("invalid command name: \"{cmd}\""))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    };

    // InitMaterializedSRF(fcinfo, 0). The C flag is 0 (build the descriptor from
    // the function's pg_proc OUT params via get_call_result_type); this port
    // resolves the polymorphic-RECORD descriptor from the executor's resolved
    // `expectedDesc` instead (the same adaptation pg_stat_get_activity makes),
    // since get_call_result_type does not reconstruct the OUT-param row type here.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let mut rows: Vec<(
        [Datum<'mcx>; PG_STAT_GET_PROGRESS_COLS],
        [bool; PG_STAT_GET_PROGRESS_COLS],
    )> = Vec::new();

    // for (curr_backend = 1; curr_backend <= num_backends; curr_backend++)
    for curr_backend in 1..=num_backends {
        let mut values: [Datum<'mcx>; PG_STAT_GET_PROGRESS_COLS] =
            core::array::from_fn(|_| Datum::null());
        let mut nulls = [false; PG_STAT_GET_PROGRESS_COLS];

        // local_beentry = pgstat_get_local_beentry_by_index(curr_backend);
        // beentry = &local_beentry->backendStatus;
        let local_beentry: LocalPgBackendStatus =
            match status::pgstat_get_local_beentry_by_index(curr_backend) {
                Some(l) => l,
                None => continue,
            };
        let beentry: &LocalBackendStatusFields = &local_beentry.backend_status;

        // Report values for only those backends which are running the given
        // command.
        if beentry.st_progress_command != cmdtype {
            continue;
        }

        // Value available to all callers.
        // values[0] = Int32GetDatum(beentry->st_procpid);
        values[0] = Datum::from_i32(beentry.st_procpid);
        // values[1] = ObjectIdGetDatum(beentry->st_databaseid);
        values[1] = Datum::from_oid(beentry.st_databaseid);

        // show rest of the values including relid only to role members.
        if has_pgstat_permissions(beentry.st_userid)? {
            values[2] = Datum::from_oid(beentry.st_progress_command_target);
            for i in 0..PGSTAT_NUM_PROGRESS_PARAM {
                values[i + 3] = Datum::from_i64(beentry.st_progress_param[i]);
            }
        } else {
            nulls[2] = true;
            for i in 0..PGSTAT_NUM_PROGRESS_PARAM {
                nulls[i + 3] = true;
            }
        }

        rows.push((values, nulls));
    }

    let rsinfo = fcinfo.resultinfo.as_mut().expect(
        "pg_stat_get_progress_info: InitMaterializedSRF establishes fcinfo->resultinfo",
    );
    for (values, nulls) in &rows {
        materialized_srf_putvalues(rsinfo, &values[..], &nulls[..])?;
    }

    // return (Datum) 0.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
