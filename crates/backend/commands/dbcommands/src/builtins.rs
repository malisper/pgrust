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

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const DBCOMMANDS_BUILTINS: &[FmgrBuiltin] = &[b(861, "current_database", 0, fc_current_database)];
