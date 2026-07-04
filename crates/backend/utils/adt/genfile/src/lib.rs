// genfile.c slice: pg_stat_file / pg_ls_dir / pg_ls_waldir. Unported (loud via
// the canonical table): pg_read_file* / pg_read_binary_file* / pg_ls_logdir /
// pg_ls_tmpdir / pg_ls_archive_statusdir / pg_ls_summariesdir / pg_ls_logicalsnapdir /
// pg_ls_logicalmapdir / pg_ls_replslotdir / pg_ls_waldir on nonstandard XLOGDIR.

use std::os::unix::fs::MetadataExt;

use datum::Datum;
use types_core::{Oid, BOOLOID, INT8OID, RECORDOID, TIMESTAMPTZOID};
use types_error::{PgResult, ERROR};
use types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
};

const ROLE_PG_READ_SERVER_FILES: Oid = 4569;

pub static GENFILE_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 2623,
        name: "pg_stat_file_1arg",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_stat_file_1arg,
    },
    FmgrBuiltin {
        foid: 2625,
        name: "pg_ls_dir_1arg",
        nargs: 1,
        strict: true,
        retset: true,
        func: fc_pg_ls_dir_1arg,
    },
    FmgrBuiltin {
        foid: 3297,
        name: "pg_ls_dir",
        nargs: 3,
        strict: true,
        retset: true,
        func: fc_pg_ls_dir,
    },
    FmgrBuiltin {
        foid: 3307,
        name: "pg_stat_file",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_stat_file,
    },
    FmgrBuiltin {
        foid: 3354,
        name: "pg_ls_waldir",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_ls_waldir,
    },
];

fn convert_and_check_filename(fcinfo: &Fcinfo) -> PgResult<String> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fns only here).
    let arg = unsafe { fcinfo.arg_varlena_packed(0)? };
    let filename = pg_path::canonicalize_path(&String::from_utf8_lossy(arg.data()));

    if acl_seams::has_privs_of_role::call(miscinit::GetUserId(), ROLE_PG_READ_SERVER_FILES)? {
        return Ok(filename);
    }

    if pg_path::is_absolute_path(&filename) {
        let datadir = init_small::globals::DataDir().unwrap_or_default();
        let logdir = guc_tables::vars::Log_directory.read().unwrap_or_default();
        if !pg_path::path_is_prefix_of_path(datadir, &filename)
            && (!pg_path::is_absolute_path(&logdir)
                || !pg_path::path_is_prefix_of_path(&logdir, &filename))
        {
            return Err(elog::ereport(ERROR)
                .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("absolute path not allowed")
                .into_error()
                .into());
        }
    } else if !pg_path::path_is_relative_and_below_cwd(&filename) {
        return Err(elog::ereport(ERROR)
            .errcode(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("path must be in or below the data directory")
            .into_error()
            .into());
    }

    Ok(filename)
}

// C: timestamp.c time_t_to_timestamptz — (POSTGRES_EPOCH_JDATE -
// UNIX_EPOCH_JDATE) * SECS_PER_DAY = 946684800.
fn time_t_to_timestamptz(t: i64) -> Datum {
    Datum::from_i64((t - 946_684_800) * 1_000_000)
}

fn stat_file(fcinfo: &mut Fcinfo, missing_ok: bool) -> PgResult<Datum> {
    let filename = convert_and_check_filename(fcinfo)?;
    // SAFETY: executor arms the per-tuple context pre-call; it outlives this
    // frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let fst = match std::fs::metadata(&filename) {
        Ok(m) => m,
        Err(e) => {
            if missing_ok && e.kind() == std::io::ErrorKind::NotFound {
                return Ok(fcinfo.return_null());
            }
            return Err(elog::ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not stat file \"{filename}\": %m"))
                .into_error()
                .into());
        }
    };

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 6)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("size"), INT8OID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("access"), TIMESTAMPTZOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 3, Some("modification"), TIMESTAMPTZOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 4, Some("change"), TIMESTAMPTZOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 5, Some("creation"), TIMESTAMPTZOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 6, Some("isdir"), BOOLOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;

    let values = [
        Datum::from_i64(fst.size() as i64),
        time_t_to_timestamptz(fst.atime()),
        time_t_to_timestamptz(fst.mtime()),
        time_t_to_timestamptz(fst.ctime()),
        Datum::null(),
        Datum::from_bool(fst.is_dir()),
    ];
    let nulls = [false, false, false, false, true, false];
    let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &nulls)?;
    byref_result(mcx, tuple.image())
}

pub fn fc_pg_stat_file(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let missing_ok = fcinfo.arg_bool(1);
    stat_file(fcinfo, missing_ok)
}

pub fn fc_pg_stat_file_1arg(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    stat_file(fcinfo, false)
}

fn ls_dir(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    missing_ok: bool,
    include_dot_dirs: bool,
) -> PgResult<Datum> {
    let location = convert_and_check_filename(fcinfo)?;
    // SAFETY: as in stat_file.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut mat =
        funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_USE_EXPECTED_DESC)?;

    let dirdesc = fd::AllocateDir(&location)?;
    if dirdesc.is_none() && missing_ok && elog::errno::current_errno() == libc::ENOENT {
        return Ok(mat.finish(fcinfo));
    }

    // std read_dir omits "." and ".." which C's readdir reports; emitted
    // up front instead — set membership matches C, order within a dir does
    // not (readdir order is unspecified anyway).
    if dirdesc.is_some() && include_dot_dirs {
        for dot in [".", ".."] {
            let d = varlena_result(varlena::cstring_to_text(mcx, dot.as_bytes())?);
            mat.putvalues(&[d], &[false])?;
        }
    }

    while let Some(de) = fd::ReadDir(dirdesc, &location)? {
        if !include_dot_dirs && (de.d_name == "." || de.d_name == "..") {
            continue;
        }
        let d = varlena_result(varlena::cstring_to_text(mcx, de.d_name.as_bytes())?);
        mat.putvalues(&[d], &[false])?;
    }

    fd::FreeDir(dirdesc)?;
    Ok(mat.finish(fcinfo))
}

pub fn fc_pg_ls_dir(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_ls_dir: NULL flinfo");
    let missing_ok = fcinfo.arg_bool(1);
    let include_dot_dirs = fcinfo.arg_bool(2);
    ls_dir(flinfo, fcinfo, missing_ok, include_dot_dirs)
}

pub fn fc_pg_ls_dir_1arg(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_ls_dir: NULL flinfo");
    ls_dir(flinfo, fcinfo, false, false)
}

fn ls_dir_files(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    dir: &str,
    missing_ok: bool,
) -> PgResult<Datum> {
    // SAFETY: as in stat_file.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut mat = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let dirdesc = fd::AllocateDir(dir)?;
    if dirdesc.is_none() && missing_ok && elog::errno::current_errno() == libc::ENOENT {
        return Ok(mat.finish(fcinfo));
    }

    while let Some(de) = fd::ReadDir(dirdesc, dir)? {
        if de.d_name.starts_with('.') {
            continue;
        }
        let path = format!("{dir}/{}", de.d_name);
        let attrib = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(elog::ereport(ERROR)
                    .with_saved_errno(e.raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not stat file \"{path}\": %m"))
                    .into_error()
                    .into());
            }
        };
        if !attrib.is_file() {
            continue;
        }
        let values = [
            varlena_result(varlena::cstring_to_text(mcx, de.d_name.as_bytes())?),
            Datum::from_i64(attrib.size() as i64),
            time_t_to_timestamptz(attrib.mtime()),
        ];
        mat.putvalues(&values, &[false; 3])?;
    }

    fd::FreeDir(dirdesc)?;
    Ok(mat.finish(fcinfo))
}

pub fn fc_pg_ls_waldir(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_ls_waldir: NULL flinfo");
    ls_dir_files(flinfo, fcinfo, "pg_wal", false)
}
