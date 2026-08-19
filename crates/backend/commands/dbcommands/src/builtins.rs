use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_tuple::NameData;

std::thread_local! {
    static NAME_SCRATCH: core::cell::UnsafeCell<NameData> =
        core::cell::UnsafeCell::new(NameData::default());
}

pub fn fc_current_database(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let dbid = init_small::globals::MyDatabaseId();
    let Some(name) = crate::get_database_name(dbid)? else {
        panic!("current_database: no pg_database row for {dbid}");
    };
    NAME_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let nd = unsafe { &mut *c.get() };
        *nd = NameData::default();
        nd.namestrcpy(&name);
        Ok(Datum::from_usize(nd.data.as_ptr() as usize))
    })
}

pub fn fc_pg_database_collation_actual_version(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    use pg_locale::COLLPROVIDER_LIBC;
    use types_error::{PgError, ERRCODE_UNDEFINED_OBJECT};

    let dbid = fcinfo.arg(0).as_oid();
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let row = pg_database_seams::search_database_syscache::call(mcx, dbid)?.ok_or_else(
        || -> Box<PgError> {
            PgError::error(format!("database with OID {dbid} does not exist"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
                .into()
        },
    )?;
    let locale = if row.datlocprovider == COLLPROVIDER_LIBC {
        row.datcollate.as_str().to_owned()
    } else {
        row.datlocale
            .as_ref()
            .ok_or_else(|| PgError::error("unexpected null datlocale in pg_database"))?
            .as_str()
            .to_owned()
    };
    match pg_locale::get_collation_actual_version(row.datlocprovider, &locale)? {
        Some(v) => Ok(types_fmgr::varlena_result(varlena::cstring_to_text(mcx, v.as_bytes())?)),
        None => Ok(fcinfo.return_null()),
    }
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const DBCOMMANDS_BUILTINS: &[FmgrBuiltin] = &[
    b(861, "current_database", 0, fc_current_database),
    b(6249, "pg_database_collation_actual_version", 1, fc_pg_database_collation_actual_version),
];

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::DBCOMMANDS_BUILTINS);
    }
}
