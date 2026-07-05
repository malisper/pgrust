//! utils/misc/pg_config.c + common/config_info.c — configure-time constants
//! as an SRF.

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// config_info.c VERSION: "PostgreSQL " PG_VERSION (the version() string's base).
const VERSION: &str = "PostgreSQL 18.3";
// C reports the build's VAL_* flags; a Rust build records none, which is C's
// own fallback text for an undefined VAL_* macro.
const NOT_RECORDED: &str = "not recorded";

// get_configdata (config_info.c): 23 entries, C order. Paths are relocated
// against my_exec_path exactly as pg_path's get_*_path do.
pub fn get_configdata() -> [(&'static str, String); 23] {
    let exec = init_small::globals::my_exec_path();
    let len = exec.iter().position(|&b| b == 0).unwrap_or(exec.len());
    let my_exec_path = String::from_utf8_lossy(&exec[..len]).into_owned();

    let bindir = match pg_path::last_dir_separator(&my_exec_path) {
        Some(i) => my_exec_path[..i].to_string(),
        None => my_exec_path.clone(),
    };
    let clean = |p: String| pg_path::canonicalize_path(&p);

    [
        ("BINDIR", clean(bindir)),
        ("DOCDIR", clean(pg_path::get_doc_path(&my_exec_path))),
        ("HTMLDIR", clean(pg_path::get_html_path(&my_exec_path))),
        ("INCLUDEDIR", clean(pg_path::get_include_path(&my_exec_path))),
        ("PKGINCLUDEDIR", clean(pg_path::get_pkginclude_path(&my_exec_path))),
        ("INCLUDEDIR-SERVER", clean(pg_path::get_includeserver_path(&my_exec_path))),
        ("LIBDIR", clean(pg_path::get_lib_path(&my_exec_path))),
        ("PKGLIBDIR", clean(pg_path::get_pkglib_path(&my_exec_path))),
        ("LOCALEDIR", clean(pg_path::get_locale_path(&my_exec_path))),
        ("MANDIR", clean(pg_path::get_man_path(&my_exec_path))),
        ("SHAREDIR", clean(pg_path::get_share_path(&my_exec_path))),
        ("SYSCONFDIR", clean(pg_path::get_etc_path(&my_exec_path))),
        (
            "PGXS",
            clean(pg_path::get_pkglib_path(&my_exec_path) + "/pgxs/src/makefiles/pgxs.mk"),
        ),
        ("CONFIGURE", String::new()),
        ("CC", NOT_RECORDED.to_string()),
        ("CPPFLAGS", NOT_RECORDED.to_string()),
        ("CFLAGS", NOT_RECORDED.to_string()),
        ("CFLAGS_SL", NOT_RECORDED.to_string()),
        ("LDFLAGS", NOT_RECORDED.to_string()),
        ("LDFLAGS_EX", NOT_RECORDED.to_string()),
        ("LDFLAGS_SL", NOT_RECORDED.to_string()),
        ("LIBS", NOT_RECORDED.to_string()),
        ("VERSION", VERSION.to_string()),
    ]
}

pub fn fc_pg_config(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_config: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    for (name, setting) in get_configdata() {
        let values = [
            varlena_result(varlena::cstring_to_text(mcx, name.as_bytes())?),
            varlena_result(varlena::cstring_to_text(mcx, setting.as_bytes())?),
        ];
        srf.putvalues(&values, &[false; 2])?;
    }

    Ok(srf.finish(fcinfo))
}

pub const PG_CONFIG_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 3400,
    name: "pg_config",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_config,
}];

#[cfg(test)]
mod tests {
    #[test]
    fn configdata_matches_c_shape() {
        init_small::globals::set_my_exec_path({
            let mut buf = [0u8; types_core::MAXPGPATH];
            buf[..28].copy_from_slice(b"/usr/local/pgsql/bin/pgrust\0");
            buf
        });
        let data = super::get_configdata();
        assert_eq!(data.len(), 23);
        assert_eq!(data[0], ("BINDIR", "/usr/local/pgsql/bin".to_string()));
        assert_eq!(data[10].0, "SHAREDIR");
        assert_eq!(data[22], ("VERSION", "PostgreSQL 18.3".to_string()));
    }
}
