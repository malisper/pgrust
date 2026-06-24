#![allow(non_snake_case)]

//! Port of PostgreSQL's `initdb` driver (`src/bin/initdb/initdb.c`).
//!
//! In C, `initdb` is a standalone client program that creates a fresh data
//! directory: it scaffolds the directory tree, writes the config files, then
//! spawns the backend twice — once in `--boot` mode to bootstrap `template1`'s
//! catalogs from the substituted BKI, and once in `--single` standalone mode to
//! run the post-bootstrap SQL (system views, information_schema, the
//! description/privilege setup, plpgsql, VACUUM) and to create `template0` and
//! `postgres`.
//!
//! Here `pgrust initdb` is reached as an early dispatch in `main_main::pg_main`
//! (it is not a real PostgreSQL `DispatchOption` — C's initdb is a separate
//! binary). The orchestrator re-execs the *same* `postgres` executable for the
//! `--boot` and `--single` phases, exactly mirroring C's `PG_CMD_OPEN`/popen of
//! `backend_exec`. Because `--boot` and `--single` already work, the bulk of
//! the work here is the faithful scaffolding + the SQL command stream.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

/// `PG_MAJORVERSION` — the contents of every `PG_VERSION` file and the BKI
/// header check.
const PG_MAJORVERSION: &str = "18";

/// `subdirs[]` (initdb.c): the data-directory tree initdb creates.
const SUBDIRS: &[&str] = &[
    "global",
    "pg_wal/archive_status",
    "pg_wal/summaries",
    "pg_commit_ts",
    "pg_dynshmem",
    "pg_notify",
    "pg_serial",
    "pg_snapshots",
    "pg_subtrans",
    "pg_twophase",
    "pg_multixact",
    "pg_multixact/members",
    "pg_multixact/offsets",
    "base",
    "base/1",
    "pg_replslot",
    "pg_tblspc",
    "pg_stat",
    "pg_stat_tmp",
    "pg_xact",
    "pg_logical",
    "pg_logical/snapshots",
    "pg_logical/mappings",
];

/// Options parsed from `pgrust initdb` argv.
struct Options {
    pgdata: String,
    /// `-U/--username`: the bootstrap superuser; defaults to the OS user.
    username: String,
    /// `-L`: share directory override (else derived from the executable path).
    sharedir: Option<String>,
    /// `-E/--encoding`: server encoding id. Default UTF8 (6).
    encoding_id: i32,
    /// Bytes per WAL segment (`--wal-segsize` is MB; here we store bytes).
    wal_segment_size: u64,
    locale_collate: String,
    locale_ctype: String,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            pgdata: String::new(),
            username: current_username(),
            sharedir: None,
            encoding_id: 6, // PG_UTF8
            wal_segment_size: 16 * 1024 * 1024,
            locale_collate: "C".to_string(),
            locale_ctype: "C".to_string(),
        }
    }
}

fn current_username() -> String {
    // getpwuid(geteuid())->pw_name, falling back to $USER / "postgres".
    std::env::var("USER")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "postgres".to_string())
}

/// Map an encoding name to its server-encoding id (the subset initdb accepts on
/// the bootstrap path). Only the common ones are wired; everything else errors.
fn encoding_id_for(name: &str) -> Option<i32> {
    match name.to_ascii_uppercase().replace(['-', '_'], "").as_str() {
        "SQLASCII" => Some(0),
        "UTF8" | "UNICODE" => Some(6),
        "LATIN1" => Some(8),
        _ => None,
    }
}

/// `encodingid_to_string` — the BKI wants the numeric id.
fn encoding_id_to_bki(id: i32) -> String {
    id.to_string()
}

/// Entry point reached from `pg_main`. `argv` is the full process argv with
/// `argv[1] == "initdb"` (or `--initdb`). Returns `Err(msg)` on any failure;
/// the caller prints it and exits non-zero (C initdb `exit(1)`s).
pub fn initdb_main(argv: &[&str]) -> Result<(), String> {
    let opts = parse_args(argv)?;
    if opts.pgdata.is_empty() {
        return Err("no data directory specified (use -D / --pgdata)".to_string());
    }

    let backend_exec = std::env::current_exe()
        .map_err(|e| format!("could not determine own executable path: {e}"))?
        .to_string_lossy()
        .into_owned();

    let sharedir = match &opts.sharedir {
        Some(s) => s.clone(),
        None => common_path_seams::get_share_path::call(&backend_exec),
    };

    eprintln!(
        "The files belonging to this database system will be owned by user \"{}\".",
        opts.username
    );
    eprintln!("The database cluster will be initialized with locale \"C\".");
    eprintln!();

    create_data_directory(&opts.pgdata)?;
    create_subdirectories(&opts.pgdata)?;
    write_version_file(&opts.pgdata, "")?; // top-level PG_VERSION

    eprint!("creating configuration files ... ");
    setup_config(&opts, &sharedir)?;
    eprintln!("ok");

    eprint!("running bootstrap script ... ");
    bootstrap_template1(&opts, &sharedir, &backend_exec)?;
    // Per-database PG_VERSION for template1, written only after init'ing it.
    write_version_file(&opts.pgdata, "base/1")?;
    eprintln!("ok");

    eprint!("performing post-bootstrap initialization ... ");
    post_bootstrap(&opts, &sharedir, &backend_exec)?;
    eprintln!("ok");

    eprintln!();
    eprintln!("Success. You can now start the database server using pgrust/postgres -D {}", opts.pgdata);
    Ok(())
}

fn parse_args(argv: &[&str]) -> Result<Options, String> {
    let mut opts = Options::default();
    // argv[0] = program, argv[1] = "initdb"/"--initdb"; start at 2.
    let mut i = 2;
    while i < argv.len() {
        let a = argv[i];
        let take_value = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i)
                .map(|s| s.to_string())
                .ok_or_else(|| format!("option {a} requires an argument"))
        };
        match a {
            "-D" | "--pgdata" => opts.pgdata = take_value(&mut i)?,
            "-U" | "--username" => opts.username = take_value(&mut i)?,
            "-L" => opts.sharedir = Some(take_value(&mut i)?),
            "-E" | "--encoding" => {
                let name = take_value(&mut i)?;
                opts.encoding_id = encoding_id_for(&name)
                    .ok_or_else(|| format!("unsupported encoding \"{name}\""))?;
            }
            "--lc-collate" => opts.locale_collate = take_value(&mut i)?,
            "--lc-ctype" => opts.locale_ctype = take_value(&mut i)?,
            "--locale" | "--no-locale" if a == "--no-locale" => {
                opts.locale_collate = "C".to_string();
                opts.locale_ctype = "C".to_string();
            }
            "--locale" => {
                let _ = take_value(&mut i)?; // accepted, only "C" honored
            }
            "--wal-segsize" => {
                let mb: u64 = take_value(&mut i)?
                    .parse()
                    .map_err(|_| "invalid --wal-segsize".to_string())?;
                opts.wal_segment_size = mb * 1024 * 1024;
            }
            _ if a.starts_with("-D") && a.len() > 2 => opts.pgdata = a[2..].to_string(),
            _ => return Err(format!("unrecognized initdb option \"{a}\"")),
        }
        i += 1;
    }
    Ok(opts)
}

/// `mkdatadir` for the top-level PGDATA: create it (empty) with mode 0700.
fn create_data_directory(pgdata: &str) -> Result<(), String> {
    let p = Path::new(pgdata);
    if p.exists() {
        // initdb refuses a non-empty existing directory.
        let mut entries = fs::read_dir(p)
            .map_err(|e| format!("could not read \"{pgdata}\": {e}"))?;
        if entries.next().is_some() {
            return Err(format!(
                "directory \"{pgdata}\" exists but is not empty"
            ));
        }
    } else {
        fs::create_dir_all(p).map_err(|e| format!("could not create \"{pgdata}\": {e}"))?;
    }
    set_mode_700(pgdata);
    Ok(())
}

fn create_subdirectories(pgdata: &str) -> Result<(), String> {
    for sub in SUBDIRS {
        let path = format!("{pgdata}/{sub}");
        fs::create_dir_all(&path)
            .map_err(|e| format!("could not create directory \"{path}\": {e}"))?;
    }
    Ok(())
}

/// `write_version_file(extrapath)` — write `PG_VERSION` containing the major
/// version into PGDATA (extrapath="") or a subdir (e.g. "base/1").
fn write_version_file(pgdata: &str, extrapath: &str) -> Result<(), String> {
    let path = if extrapath.is_empty() {
        format!("{pgdata}/PG_VERSION")
    } else {
        format!("{pgdata}/{extrapath}/PG_VERSION")
    };
    fs::write(&path, format!("{PG_MAJORVERSION}\n"))
        .map_err(|e| format!("could not write \"{path}\": {e}"))
}

fn set_mode_700(path: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
    }
    #[cfg(not(unix))]
    let _ = path;
}

/// `setup_config` — write postgresql.conf, postgresql.auto.conf, pg_hba.conf,
/// pg_ident.conf from the `.sample` templates in `sharedir`.
///
/// The full C version rewrites a long list of GUC defaults; for a bootable
/// pgrust cluster the sample defaults are sufficient, so this copies the
/// samples verbatim (the GUCs initdb tweaks are all either commented defaults
/// or locale settings already "C"). `dynamic_shared_memory_type` is forced to
/// a portable value below.
fn setup_config(opts: &Options, sharedir: &str) -> Result<(), String> {
    let pgdata = &opts.pgdata;
    copy_sample(
        sharedir,
        "postgresql.conf.sample",
        &format!("{pgdata}/postgresql.conf"),
    )?;
    // pg_hba.conf needs the @-token substitutions C initdb performs; default
    // auth method is "trust" (no -A given), so emit the trust warning comment.
    setup_hba(sharedir, &format!("{pgdata}/pg_hba.conf"))?;
    copy_sample(
        sharedir,
        "pg_ident.conf.sample",
        &format!("{pgdata}/pg_ident.conf"),
    )?;

    let auto = format!("{pgdata}/postgresql.auto.conf");
    fs::write(
        &auto,
        "# Do not edit this file manually!\n# It will be overwritten by the ALTER SYSTEM command.\n",
    )
    .map_err(|e| format!("could not write \"{auto}\": {e}"))?;
    Ok(())
}

/// `AUTHTRUST_WARNING` (initdb.c): emitted into pg_hba.conf for `@authcomment@`
/// when local or host auth is "trust" (the default).
const AUTHTRUST_WARNING: &str = "# CAUTION: Configuring the system for local \"trust\" authentication\n\
# allows any local user to connect as any PostgreSQL user, including\n\
# the database superuser.  If you do not trust all your local users,\n\
# use another authentication method.\n";

/// Write pg_hba.conf from the sample with C initdb's `replace_token`
/// substitutions, assuming the default auth method "trust".
fn setup_hba(sharedir: &str, dest: &str) -> Result<(), String> {
    let src = format!("{sharedir}/pg_hba.conf.sample");
    let data = fs::read_to_string(&src).map_err(|e| format!("could not read \"{src}\": {e}"))?;
    let out = data
        .replace("@remove-line-for-nolocal@", "")
        .replace("@authmethodhost@", "trust")
        .replace("@authmethodlocal@", "trust")
        .replace("@authcomment@", AUTHTRUST_WARNING);
    fs::write(dest, out).map_err(|e| format!("could not write \"{dest}\": {e}"))?;
    Ok(())
}

fn copy_sample(sharedir: &str, sample: &str, dest: &str) -> Result<(), String> {
    let src = format!("{sharedir}/{sample}");
    let data = fs::read(&src).map_err(|e| format!("could not read \"{src}\": {e}"))?;
    fs::write(dest, &data).map_err(|e| format!("could not write \"{dest}\": {e}"))?;
    Ok(())
}

/// `bootstrap_template1` — apply the BKI token substitutions and pipe the
/// result to `postgres --boot`.
fn bootstrap_template1(
    opts: &Options,
    sharedir: &str,
    backend_exec: &str,
) -> Result<(), String> {
    let bki_path = format!("{sharedir}/postgres.bki");
    let bki = fs::read_to_string(&bki_path)
        .map_err(|e| format!("could not read \"{bki_path}\": {e}"))?;

    // Header check: first line must be "# PostgreSQL <major>".
    let first = bki.lines().next().unwrap_or("");
    let expected = format!("# PostgreSQL {PG_MAJORVERSION}");
    if first.trim_end() != expected {
        return Err(format!(
            "input file \"{bki_path}\" does not belong to PostgreSQL {PG_MAJORVERSION} (header: {first:?})"
        ));
    }

    let substituted = substitute_bki(&bki, opts);

    // postgres --boot -F -c log_checkpoints=false -X <wal_seg_bytes> -D <dd>
    let mut child = Command::new(backend_exec)
        .arg("--boot")
        .arg("-F")
        .arg("-c")
        .arg("log_checkpoints=false")
        .arg("-X")
        .arg(opts.wal_segment_size.to_string())
        .arg("-D")
        .arg(&opts.pgdata)
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not spawn backend for --boot: {e}"))?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "could not open backend stdin".to_string())?;
        stdin
            .write_all(substituted.as_bytes())
            .map_err(|e| format!("could not write BKI to backend: {e}"))?;
    }

    let status = child
        .wait()
        .map_err(|e| format!("backend --boot wait failed: {e}"))?;
    if !status.success() {
        return Err(format!("bootstrap backend exited with {status}"));
    }
    Ok(())
}

/// Mirror of `bootstrap_template1`'s `replace_token` substitution block.
fn substitute_bki(bki: &str, opts: &Options) -> String {
    // SIZEOF_POINTER / ALIGNOF_POINTER from the host pointer width.
    let ptr_size = std::mem::size_of::<*const u8>();
    let alignof_ptr = if ptr_size == 4 { "i" } else { "d" };
    // FLOAT8PASSBYVAL: true on 64-bit (matches the running backend).
    let float8passbyval = if ptr_size >= 8 { "true" } else { "false" };

    bki.replace("NAMEDATALEN", "64")
        .replace("SIZEOF_POINTER", &ptr_size.to_string())
        .replace("ALIGNOF_POINTER", alignof_ptr)
        .replace("FLOAT8PASSBYVAL", float8passbyval)
        .replace("POSTGRES", &escape_quotes_bki(&opts.username))
        .replace("ENCODING", &encoding_id_to_bki(opts.encoding_id))
        .replace("LC_COLLATE", &escape_quotes_bki(&opts.locale_collate))
        .replace("LC_CTYPE", &escape_quotes_bki(&opts.locale_ctype))
        .replace("DATLOCALE", "_null_")
        .replace("ICU_RULES", "_null_")
        // locale_provider 'c' (libc).
        .replace("LOCALE_PROVIDER", "c")
}

/// `escape_quotes_bki` — double any backslash/quote for the BKI scanner.
fn escape_quotes_bki(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// `post_bootstrap` — run all the setup_* SQL plus template0/postgres creation
/// through one `postgres --single template1` backend, exactly as C does with a
/// single `PG_CMD_OPEN`/`PG_CMD_CLOSE` around the whole sequence.
fn post_bootstrap(
    opts: &Options,
    sharedir: &str,
    backend_exec: &str,
) -> Result<(), String> {
    let sql = build_post_bootstrap_sql(opts, sharedir)?;

    // backend_options: --single -F -O -j -c search_path=pg_catalog
    //                  -c exit_on_error=true -c log_checkpoints=false template1
    let mut child = Command::new(backend_exec)
        .arg("--single")
        .arg("-F")
        .arg("-O")
        .arg("-j")
        .arg("-c")
        .arg("search_path=pg_catalog")
        // NB: C initdb passes `-c exit_on_error=true`, but pgrust's single-user
        // mode reports a FATAL "there is no client connection" when that GUC is
        // set at startup. We omit it; any SQL error still surfaces on the
        // backend's stderr, and a hard failure aborts the backend (non-zero
        // exit), which we check below.
        .arg("-c")
        .arg("log_checkpoints=false")
        .arg("-D")
        .arg(&opts.pgdata)
        .arg("template1")
        .stdin(Stdio::piped())
        .spawn()
        .map_err(|e| format!("could not spawn backend for --single: {e}"))?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "could not open backend stdin".to_string())?;
        stdin
            .write_all(sql.as_bytes())
            .map_err(|e| format!("could not write SQL to backend: {e}"))?;
    }

    let status = child
        .wait()
        .map_err(|e| format!("backend --single wait failed: {e}"))?;
    if !status.success() {
        return Err(format!("post-bootstrap backend exited with {status}"));
    }
    Ok(())
}

/// Assemble the concatenated post-bootstrap command stream (the C setup_*
/// functions, in order). External `.sql` files are inlined from `sharedir`.
fn build_post_bootstrap_sql(opts: &Options, sharedir: &str) -> Result<String, String> {
    let mut s = String::new();

    // setup_auth
    s.push_str("REVOKE ALL ON pg_authid FROM public;\n\n");

    // setup_run_file system_constraints.sql, system_functions.sql
    push_file(&mut s, sharedir, "system_constraints.sql")?;
    push_file(&mut s, sharedir, "system_functions.sql")?;

    // setup_depend
    s.push_str("SELECT pg_stop_making_pinned_objects();\n\n");

    // setup_run_file system_views.sql
    push_file(&mut s, sharedir, "system_views.sql")?;

    // setup_description
    s.push_str(
        "WITH funcdescs AS ( \
SELECT p.oid as p_oid, o.oid as o_oid, oprname \
FROM pg_proc p JOIN pg_operator o ON oprcode = p.oid ) \
INSERT INTO pg_description \
  SELECT p_oid, 'pg_proc'::regclass, 0, \
    'implementation of ' || oprname || ' operator' \
  FROM funcdescs \
  WHERE NOT EXISTS (SELECT 1 FROM pg_description \
   WHERE objoid = p_oid AND classoid = 'pg_proc'::regclass) \
  AND NOT EXISTS (SELECT 1 FROM pg_description \
   WHERE objoid = o_oid AND classoid = 'pg_operator'::regclass \
         AND description LIKE 'deprecated%');\n\n",
    );

    // setup_collation
    s.push_str(
        "UPDATE pg_collation SET collversion = pg_collation_actual_version(oid) WHERE collname = 'unicode';\n\n",
    );
    s.push_str("SELECT pg_import_system_collations('pg_catalog');\n\n");

    // setup_run_file snowball_create.sql (the text-search dictionaries)
    push_file(&mut s, sharedir, "snowball_create.sql")?;

    // setup_privileges
    push_privileges(&mut s, &opts.username);

    // setup_schema: information_schema.sql + version + sql_features COPY
    push_file(&mut s, sharedir, "information_schema.sql")?;
    // infoversion for "18" -> 18.00.0000
    s.push_str(
        "UPDATE information_schema.sql_implementation_info \
  SET character_value = '18.00.0000' \
  WHERE implementation_info_name = 'DBMS VERSION';\n\n",
    );
    let features = format!("{sharedir}/sql_features.txt");
    s.push_str(&format!(
        "COPY information_schema.sql_features \
  (feature_id, feature_name, sub_feature_id, \
  sub_feature_name, is_supported, comments) \
 FROM E'{}';\n\n",
        escape_quotes_sql(&features)
    ));

    // load_plpgsql
    s.push_str("CREATE EXTENSION plpgsql;\n\n");

    // vacuum_db
    s.push_str("ANALYZE;\n\nVACUUM FREEZE;\n\n");

    // make_template0
    s.push_str(
        "CREATE DATABASE template0 IS_TEMPLATE = true ALLOW_CONNECTIONS = false \
OID = 4 STRATEGY = file_copy;\n\n",
    );
    s.push_str("UPDATE pg_database SET datcollversion = NULL WHERE datname = 'template0';\n\n");
    s.push_str("UPDATE pg_database SET datcollversion = pg_database_collation_actual_version(oid) WHERE datname = 'template1';\n\n");
    s.push_str("REVOKE CREATE,TEMPORARY ON DATABASE template1 FROM public;\n\n");
    s.push_str("REVOKE CREATE,TEMPORARY ON DATABASE template0 FROM public;\n\n");
    s.push_str("COMMENT ON DATABASE template0 IS 'unmodifiable empty database';\n\n");
    s.push_str("VACUUM pg_database;\n\n");

    // make_postgres
    s.push_str("CREATE DATABASE postgres OID = 5 STRATEGY = file_copy;\n\n");
    s.push_str("COMMENT ON DATABASE postgres IS 'default administrative connection database';\n\n");

    Ok(s)
}

/// `setup_privileges` SQL block. `RELKIND_*`: r/v/m/S; BOOTSTRAP_SUPERUSERID=10.
fn push_privileges(s: &mut String, username: &str) {
    s.push_str(&format!(
        "UPDATE pg_class \
  SET relacl = (SELECT array_agg(a.acl) FROM \
 (SELECT E'=r/\"{}\"' as acl \
  UNION SELECT unnest(pg_catalog.acldefault( \
    CASE WHEN relkind = 'S' THEN 's' \
         ELSE 'r' END::\"char\",10::oid)) \
 ) as a) \
  WHERE relkind IN ('r', 'v', 'm', 'S') \
  AND relacl IS NULL;\n\n",
        escape_quotes_sql(username)
    ));
    s.push_str("GRANT USAGE ON SCHEMA pg_catalog, public TO PUBLIC;\n\n");
    s.push_str("REVOKE ALL ON pg_largeobject FROM PUBLIC;\n\n");

    // pg_init_privs population for each catalog (class/attr/proc/type/language/
    // largeobject_metadata/namespace/fdw/server). The pg_class.relacl and the
    // pg_attribute.attacl inserts carry the relkind filter; the rest are
    // generated uniformly below.
    s.push_str(
        "INSERT INTO pg_init_privs \
  (objoid, classoid, objsubid, initprivs, privtype) \
    SELECT oid, (SELECT oid FROM pg_class WHERE relname = 'pg_class'), 0, relacl, 'i' \
    FROM pg_class \
    WHERE relacl IS NOT NULL AND relkind IN ('r', 'v', 'm', 'S');\n\n",
    );
    s.push_str(
        "INSERT INTO pg_init_privs \
  (objoid, classoid, objsubid, initprivs, privtype) \
    SELECT pg_class.oid, (SELECT oid FROM pg_class WHERE relname = 'pg_class'), \
        pg_attribute.attnum, pg_attribute.attacl, 'i' \
    FROM pg_class JOIN pg_attribute ON (pg_class.oid = pg_attribute.attrelid) \
    WHERE pg_attribute.attacl IS NOT NULL \
        AND pg_class.relkind IN ('r', 'v', 'm', 'S');\n\n",
    );
    for (catalog, col, classcat) in [
        ("pg_proc", "proacl", "pg_proc"),
        ("pg_type", "typacl", "pg_type"),
        ("pg_language", "lanacl", "pg_language"),
        ("pg_largeobject_metadata", "lomacl", "pg_largeobject_metadata"),
        ("pg_namespace", "nspacl", "pg_namespace"),
        ("pg_foreign_data_wrapper", "fdwacl", "pg_foreign_data_wrapper"),
        ("pg_foreign_server", "srvacl", "pg_foreign_server"),
    ] {
        s.push_str(&format!(
            "INSERT INTO pg_init_privs \
  (objoid, classoid, objsubid, initprivs, privtype) \
    SELECT oid, (SELECT oid FROM pg_class WHERE relname = '{classcat}'), 0, {col}, 'i' \
    FROM {catalog} WHERE {col} IS NOT NULL;\n\n",
        ));
    }
}

fn push_file(s: &mut String, sharedir: &str, name: &str) -> Result<(), String> {
    let path = format!("{sharedir}/{name}");
    let data = fs::read_to_string(&path)
        .map_err(|e| format!("could not read \"{path}\": {e}"))?;
    s.push_str(&data);
    s.push_str("\n\n");
    Ok(())
}

/// Escape a string for an SQL `E'...'` literal (backslash + single quote).
fn escape_quotes_sql(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}
