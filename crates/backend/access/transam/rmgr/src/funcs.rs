use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::{RmgrIdExists, RmgrTable};

pub(crate) fn register_builtins() {
    fmgr_core::register_late_builtins(RMGR_BUILTINS);
}

static RMGR_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 6224,
    name: "pg_get_wal_resource_managers",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_get_wal_resource_managers,
}];

const RM_MAX_ID: usize = u8::MAX as usize;

pub fn fc_pg_get_wal_resource_managers(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_resource_managers: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for rmid in 0..=RM_MAX_ID {
        if !RmgrIdExists(rmid as u8) {
            continue;
        }
        // No-dlopen carve: only the builtin table is populated, so
        // RmgrIdIsBuiltin(rmid) is always true for an existing id.
        let name = varlena_result(varlena::cstring_to_text(
            mcx,
            RmgrTable[rmid].rm_name.as_bytes(),
        )?);
        let values = [Datum::from_i32(rmid as i32), name, Datum::from_bool(true)];
        srf.putvalues(&values, &[false; 3])?;
    }

    Ok(srf.finish(fcinfo))
}

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::RMGR_BUILTINS);
    }

    #[test]
    fn only_builtin_ids_exist() {
        let n = (0..=super::RM_MAX_ID).filter(|&id| crate::RmgrIdExists(id as u8)).count();
        assert_eq!(n, crate::RM_N_BUILTIN_IDS);
    }
}
