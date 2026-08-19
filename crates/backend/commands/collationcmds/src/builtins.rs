use datum::Datum;
use pg_locale::COLLPROVIDER_LIBC;
use types_core::DEFAULT_COLLATION_OID;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_OBJECT};
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub const COLLATIONCMDS_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 3448,
    name: "pg_collation_actual_version",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_pg_collation_actual_version,
}];

pub fn fc_pg_collation_actual_version(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let collid = fcinfo.arg(0).as_oid();
    let mcx = fcinfo.result_mcx();

    let (provider, locale) = if collid == DEFAULT_COLLATION_OID {
        let dboid = init_small::globals::MyDatabaseId();
        let row = pg_database_seams::search_database_syscache::call(mcx, dboid)?.ok_or_else(
            || -> Box<PgError> {
                PgError::error(format!("database with OID {dboid} does not exist"))
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
        (row.datlocprovider, locale)
    } else {
        let row = syscache_seams::lookup_pg_collation_locale_row::call(mcx, collid)?.ok_or_else(
            || -> Box<PgError> {
                PgError::error(format!("collation with OID {collid} does not exist"))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
                    .into()
            },
        )?;
        let field = if row.collprovider == COLLPROVIDER_LIBC {
            &row.collcollate
        } else {
            &row.colllocale
        };
        let locale = field
            .as_ref()
            .ok_or_else(|| PgError::error("unexpected null locale in pg_collation"))?
            .as_str()
            .to_owned();
        (row.collprovider, locale)
    };

    match pg_locale::get_collation_actual_version(provider, &locale)? {
        Some(v) => Ok(varlena_result(varlena::cstring_to_text(mcx, v.as_bytes())?)),
        None => Ok(fcinfo.return_null()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::COLLATIONCMDS_BUILTINS);
    }
}
