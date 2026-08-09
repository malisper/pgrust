//! C: src/bin/pg_combinebackup/pg_combinebackup.c (18.3) — combine
//! incremental backups with prior backups into a synthetic full backup.
//!
//! Ported whole: option parsing (getopt_long-compatible, GNU permutation),
//! PG_VERSION / control-file / backup_label chain validation, manifest
//! loading + system-identifier cross-check, tablespace scanning and -T
//! remapping (in-place tablespaces included), recursive copy/reconstruct
//! walk, output backup_label + backup_manifest, --dry-run, --no-sync, and
//! the copy strategies (see copy_file.rs for platform coverage).
//! Error message identity and exit codes pinned to C.

use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use manifest::{
    pg_checksum_parse_type, PgChecksumContext, PgChecksumType, PG_CHECKSUM_MAX_LENGTH,
};

mod backup_label;
mod copy_file;
mod flog;
mod fsutil;
mod load_manifest;
mod reconstruct;
mod write_manifest;

use copy_file::CopyMethod;
use flog::{
    errno_message, log_debug, log_error, log_error_hint, log_warning, log_warning_hint, pg_fatal,
};
use load_manifest::ManifestData;
use write_manifest::ManifestWriter;

/// The pg_combinebackup version whose behavior this port tracks.
pub const PG_COMBINEBACKUP_VERSION: &str = "18.3";

pub const BLCKSZ: usize = 8192;
pub const RELSEG_SIZE: u32 = 131072;

const PG_TBLSPC_DIR: &str = "pg_tblspc";
const PG_CONTROL_VERSION: u32 = controldata_utils::PG_CONTROL_VERSION;
const XLOG_CONTROL_FILE: &str = controldata_utils::XLOG_CONTROL_FILE;

/* Incremental file naming convention. */
const INCREMENTAL_PREFIX: &str = "INCREMENTAL.";

/// C: cb_tablespace_mapping.
struct TablespaceMapping {
    old_dir: String,
    new_dir: String,
}

/// C: cb_options.
struct CbOptions {
    debug: bool,
    output: Option<String>,
    dry_run: bool,
    no_sync: bool,
    tsmappings: Vec<TablespaceMapping>,
    manifest_checksums: PgChecksumType,
    no_manifest: bool,
    copy_method: CopyMethod,
}

/// C: cb_tablespace.
struct CbTablespace {
    oid: u32,
    in_place: bool,
    old_dir: String,
    new_dir: String,
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    /* C: handle_help_version_opts — only looks at argv[1]. */
    if args.len() > 1 {
        if args[1] == "--help" || args[1] == "-?" {
            help();
            std::process::exit(0);
        }
        if args[1] == "--version" || args[1] == "-V" {
            println!("pg_combinebackup (PostgreSQL) {PG_COMBINEBACKUP_VERSION} (pgrust)");
            std::process::exit(0);
        }
    }

    let mut opt = CbOptions {
        debug: false,
        output: None,
        dry_run: false,
        no_sync: false,
        tsmappings: Vec::new(),
        manifest_checksums: PgChecksumType::Crc32c,
        no_manifest: false,
        copy_method: CopyMethod::Copy,
    };

    /* process command-line options */
    let operands = parse_options(&args, &mut opt);

    if operands.is_empty() {
        log_error("no input directories specified");
        log_error_hint(&format!(
            "Try \"{} --help\" for more information.",
            flog::PROGNAME
        ));
        std::process::exit(1);
    }

    let Some(output_dir) = opt.output.clone() else {
        pg_fatal!("no output directory specified");
    };

    /* If no manifest is needed, no checksums are needed, either. */
    if opt.no_manifest {
        opt.manifest_checksums = PgChecksumType::None;
    }

    /* Check that the platform supports the requested copy method. */
    if opt.copy_method == CopyMethod::Clone {
        if cfg!(any(target_os = "macos", target_os = "linux")) {
            if opt.dry_run {
                log_debug("would use cloning to copy files");
            } else {
                log_debug("will use cloning to copy files");
            }
        } else {
            pg_fatal!("file cloning not supported on this platform");
        }
    } else if opt.copy_method == CopyMethod::CopyFileRange {
        if cfg!(target_os = "linux") {
            if opt.dry_run {
                log_debug("would use copy_file_range to copy blocks");
            } else {
                log_debug("will use copy_file_range to copy blocks");
            }
        } else {
            pg_fatal!("copy_file_range not supported on this platform");
        }
    }

    /* Read the server version from the final backup. */
    let version = read_pg_version_file(operands.last().unwrap());
    let _ = version;

    /* Sanity-check control files. */
    let n_backups = operands.len();
    let system_identifier = check_control_files(&operands);

    /* Sanity-check backup_label files, and get the contents of the last one. */
    let last_backup_label = check_backup_label_files(&operands);

    /*
     * We'll need the pathnames to the prior backups. By "prior" we mean all
     * but the last one listed on the command line.
     */
    let n_prior_backups = n_backups - 1;
    let prior_backup_dirs: &[String] = &operands[..n_prior_backups];

    /* Load backup manifests. */
    let manifests = load_manifest::load_backup_manifests(&operands);

    /*
     * Validate the manifest system identifier against the backup system
     * identifier.
     */
    for (i, m) in manifests.iter().enumerate() {
        if let Some(m) = m {
            if m.system_identifier != system_identifier {
                pg_fatal!(
                    "{}/{}: manifest system identifier is {}, but control file has {}",
                    operands[i],
                    XLOG_CONTROL_FILE,
                    m.system_identifier,
                    system_identifier
                );
            }
        }
    }

    /* Figure out which tablespaces are going to be included in the output. */
    let last_input_dir = operands.last().unwrap();
    check_input_dir_permissions(Path::new(last_input_dir));
    let tablespaces = scan_for_existing_tablespaces(Path::new(last_input_dir), &opt, &output_dir);

    /*
     * Create output directories. (The equivalent of C's atexit registration
     * is flog::exit_program, which runs the cleanup list on failure exits.)
     */
    create_output_directory(Path::new(&output_dir), &opt);
    for ts in &tablespaces {
        if !ts.in_place {
            create_output_directory(Path::new(&ts.new_dir), &opt);
        }
    }

    /* If we need to write a backup_manifest, prepare to do so. */
    let mut mwriter: Option<ManifestWriter> = if !opt.dry_run && !opt.no_manifest {
        let w = write_manifest::create_manifest_writer(Path::new(&output_dir), system_identifier);
        /*
         * Verify that we have a backup manifest for the final backup; else
         * we won't have the WAL ranges for the resulting manifest.
         */
        if manifests[n_prior_backups].is_none() {
            pg_fatal!("cannot generate a manifest because no manifest is available for the final input backup");
        }
        Some(w)
    } else {
        None
    };

    /* Write backup label into output directory. */
    if opt.dry_run {
        log_debug(&format!("would generate \"{output_dir}/backup_label\""));
    } else {
        log_debug(&format!("generating \"{output_dir}/backup_label\""));
        backup_label::write_backup_label(
            Path::new(&output_dir),
            &last_backup_label,
            opt.manifest_checksums,
            mwriter.as_mut(),
        );
    }

    /* Process everything that's not part of a user-defined tablespace. */
    log_debug(&format!("processing backup directory \"{last_input_dir}\""));
    process_directory_recursively(
        None,
        Path::new(last_input_dir),
        Path::new(&output_dir),
        None,
        prior_backup_dirs,
        &manifests,
        &mut mwriter,
        &opt,
    );

    /* Process user-defined tablespaces. */
    for ts in &tablespaces {
        log_debug(&format!("processing tablespace directory \"{}\"", ts.old_dir));

        /*
         * Normal tablespace: symlink pg_tblspc/${OID} -> target; in-place
         * tablespace: directory at pg_tblspc/${OID}.
         */
        if !ts.in_place {
            let linkpath = format!("{}/{}/{}", output_dir, PG_TBLSPC_DIR, ts.oid);

            if opt.dry_run {
                log_debug(&format!(
                    "would create symbolic link from \"{linkpath}\" to \"{}\"",
                    ts.new_dir
                ));
            } else {
                log_debug(&format!(
                    "creating symbolic link from \"{linkpath}\" to \"{}\"",
                    ts.new_dir
                ));
                if let Err(e) = std::os::unix::fs::symlink(&ts.new_dir, &linkpath) {
                    pg_fatal!(
                        "could not create symbolic link from \"{}\" to \"{}\": {}",
                        linkpath,
                        ts.new_dir,
                        errno_message(&e)
                    );
                }
            }
        } else if opt.dry_run {
            log_debug(&format!("would create directory \"{}\"", ts.new_dir));
        } else {
            log_debug(&format!("creating directory \"{}\"", ts.new_dir));
            if fsutil::pg_mkdir_p(Path::new(&ts.new_dir)).is_err() {
                let e = std::io::Error::last_os_error();
                pg_fatal!(
                    "could not create directory \"{}\": {}",
                    ts.new_dir,
                    errno_message(&e)
                );
            }
        }

        /* OK, now handle the directory contents. */
        process_directory_recursively(
            Some(ts.oid),
            Path::new(&ts.old_dir),
            Path::new(&ts.new_dir),
            None,
            prior_backup_dirs,
            &manifests,
            &mut mwriter,
            &opt,
        );
    }

    /* Finalize the backup_manifest, if we're generating one. */
    if let Some(mwriter) = mwriter.as_mut() {
        mwriter.finalize(&manifests[n_prior_backups].as_ref().unwrap().wal_ranges);
    }

    /* fsync that output directory unless we've been told not to do so */
    if !opt.no_sync {
        if opt.dry_run {
            log_debug(&format!("would recursively fsync \"{output_dir}\""));
        } else {
            log_debug(&format!("recursively fsyncing \"{output_dir}\""));
            fsutil::fsync_dir_recurse(Path::new(&output_dir));
        }
    }

    /* Warn about the possibility of compromising the backups, when link mode */
    if opt.copy_method == CopyMethod::Link {
        log_warning(
            "--link mode was used; any modifications to the output \
             directory might destructively modify input directories",
        );
    }

    /* It's a success, so don't remove the output directories. */
    flog::reset_directory_cleanup_list();
    std::process::exit(0);
}

/// getopt_long(argc, argv, "dknNo:T:", long_options) with GNU permutation:
/// returns the operands (non-option arguments). Exits like C on bad options.
fn parse_options(args: &[String], opt: &mut CbOptions) -> Vec<String> {
    let progname = flog::PROGNAME;
    let usage_hint = || {
        log_error_hint(&format!("Try \"{progname} --help\" for more information."));
        std::process::exit(1);
    };

    let mut operands: Vec<String> = Vec::new();
    let mut i = 1usize;
    let mut no_more_options = false;
    while i < args.len() {
        let arg = &args[i];
        if no_more_options || arg == "-" || !arg.starts_with('-') {
            operands.push(arg.clone());
            i += 1;
            continue;
        }
        if arg == "--" {
            no_more_options = true;
            i += 1;
            continue;
        }

        if let Some(long) = arg.strip_prefix("--") {
            let (name, inline_val) = match long.split_once('=') {
                Some((n, v)) => (n, Some(v.to_string())),
                None => (long, None),
            };
            let take_arg = |i: &mut usize| -> String {
                if let Some(v) = inline_val.clone() {
                    return v;
                }
                *i += 1;
                if *i >= args.len() {
                    eprintln!("{progname}: option '--{name}' requires an argument");
                    usage_hint();
                }
                args[*i].clone()
            };
            match name {
                "debug" => {
                    opt.debug = true;
                    flog::increase_verbosity();
                }
                "dry-run" => opt.dry_run = true,
                "no-sync" => opt.no_sync = true,
                "output" => opt.output = Some(take_arg(&mut i)),
                "tablespace-mapping" => {
                    let v = take_arg(&mut i);
                    add_tablespace_mapping(opt, &v);
                }
                "link" => opt.copy_method = CopyMethod::Link,
                "manifest-checksums" => {
                    let v = take_arg(&mut i);
                    match pg_checksum_parse_type(v.as_bytes()) {
                        Some(t) => opt.manifest_checksums = t,
                        None => pg_fatal!("unrecognized checksum algorithm: \"{v}\""),
                    }
                }
                "no-manifest" => opt.no_manifest = true,
                "sync-method" => {
                    let v = take_arg(&mut i);
                    if !parse_sync_method(&v) {
                        std::process::exit(1);
                    }
                }
                "clone" => opt.copy_method = CopyMethod::Clone,
                "copy" => opt.copy_method = CopyMethod::Copy,
                "copy-file-range" => opt.copy_method = CopyMethod::CopyFileRange,
                _ => {
                    eprintln!("{progname}: unrecognized option '--{long}'");
                    usage_hint();
                }
            }
            i += 1;
            continue;
        }

        /* Short options, possibly bundled. */
        let bytes = arg.as_bytes();
        let mut j = 1usize;
        while j < bytes.len() {
            let c = bytes[j] as char;
            match c {
                'd' => {
                    opt.debug = true;
                    flog::increase_verbosity();
                }
                'k' => opt.copy_method = CopyMethod::Link,
                'n' => opt.dry_run = true,
                'N' => opt.no_sync = true,
                'o' | 'T' => {
                    let val: String = if j + 1 < bytes.len() {
                        arg[j + 1..].to_string()
                    } else {
                        i += 1;
                        if i >= args.len() {
                            eprintln!("{progname}: option requires an argument -- '{c}'");
                            usage_hint();
                        }
                        args[i].clone()
                    };
                    if c == 'o' {
                        opt.output = Some(val);
                    } else {
                        add_tablespace_mapping(opt, &val);
                    }
                    j = bytes.len();
                    continue;
                }
                _ => {
                    eprintln!("{progname}: invalid option -- '{c}'");
                    usage_hint();
                }
            }
            j += 1;
        }
        i += 1;
    }
    operands
}

/// C: parse_sync_method (fe_utils/option_utils.c). We only implement the
/// fsync method; syncfs is Linux-only in C and reported the same way C
/// reports it in a build without HAVE_SYNCFS.
fn parse_sync_method(arg: &str) -> bool {
    if arg == "fsync" {
        true
    } else if arg == "syncfs" {
        log_error(&format!(
            "this build does not support sync method \"{}\"",
            "syncfs"
        ));
        false
    } else {
        log_error(&format!("unrecognized sync method: {arg}"));
        false
    }
}

/// C: help.
fn help() {
    let progname = flog::PROGNAME;
    print!(
        "{progname} reconstructs full backups from incrementals.\n\n\
Usage:\n\
  {progname} [OPTION]... DIRECTORY...\n\
\nOptions:\n\
  -d, --debug               generate lots of debugging output\n\
  -k, --link                link files instead of copying\n\
  -n, --dry-run             do not actually do anything\n\
  -N, --no-sync             do not wait for changes to be written safely to disk\n\
  -o, --output=DIRECTORY    output directory\n\
  -T, --tablespace-mapping=OLDDIR=NEWDIR\n\
                            relocate tablespace in OLDDIR to NEWDIR\n\
      --clone               clone (reflink) files instead of copying\n\
      --copy                copy files (default)\n\
      --copy-file-range     copy using copy_file_range() system call\n\
      --manifest-checksums=SHA{{224,256,384,512}}|CRC32C|NONE\n\
                            use algorithm for manifest checksums\n\
      --no-manifest         suppress generation of backup manifest\n\
      --sync-method=METHOD  set method for syncing files to disk\n\
  -V, --version             output version information, then exit\n\
  -?, --help                show this help, then exit\n\
\nReport bugs to <pgsql-bugs@lists.postgresql.org>.\n\
PostgreSQL home page: <https://www.postgresql.org/>\n"
    );
}

/// C: add_tablespace_mapping.
fn add_tablespace_mapping(opt: &mut CbOptions, arg: &str) {
    let mut old_dir = String::new();
    let mut new_dir = String::new();
    {
        /*
         * Copy everything before the equals sign to old_dir and everything
         * afterwards to new_dir; "\=" is a literal equals sign.
         */
        let bytes = arg.as_bytes();
        let mut in_new = false;
        let mut k = 0usize;
        while k < bytes.len() {
            let c = bytes[k] as char;
            let dst = if in_new { &mut new_dir } else { &mut old_dir };
            if dst.len() >= 1024 {
                pg_fatal!("directory name too long");
            }
            if c == '\\' && bytes.get(k + 1) == Some(&b'=') {
                /* skip backslash escaping = */
            } else if c == '=' && (k == 0 || bytes[k - 1] != b'\\') {
                if !new_dir.is_empty() {
                    pg_fatal!("multiple \"=\" signs in tablespace mapping");
                }
                in_new = true;
            } else {
                dst.push(c);
            }
            k += 1;
        }
    }
    if old_dir.is_empty() || new_dir.is_empty() {
        pg_fatal!("invalid tablespace mapping format \"{arg}\", must be \"OLDDIR=NEWDIR\"");
    }

    /*
     * All tablespaces are created with absolute directories; both old and
     * new are on the local machine.
     */
    if !old_dir.starts_with('/') {
        pg_fatal!("old directory is not an absolute path in tablespace mapping: {old_dir}");
    }
    if !new_dir.starts_with('/') {
        pg_fatal!("new directory is not an absolute path in tablespace mapping: {new_dir}");
    }

    /* Canonicalize paths to avoid spurious failures when comparing. */
    let old_dir = fsutil::canonicalize_path(&old_dir);
    let new_dir = fsutil::canonicalize_path(&new_dir);

    opt.tsmappings.push(TablespaceMapping { old_dir, new_dir });
}

/// C: check_backup_label_files — verify the chain, return the final label.
fn check_backup_label_files(backup_dirs: &[String]) -> Vec<u8> {
    let n_backups = backup_dirs.len();
    let mut check_tli: u32 = 0;
    let mut check_lsn: u64 = 0;
    let mut lastbuf: Vec<u8> = Vec::new();

    /* Try to read each backup_label file in turn, last to first. */
    for i in (0..n_backups).rev() {
        let pathbuf = format!("{}/backup_label", backup_dirs[i]);
        log_debug(&format!("reading \"{pathbuf}\""));

        /* Slurp the whole file into memory (limit like C: 10000+MAXPGPATH). */
        let buf = fsutil::slurp_file(Path::new(&pathbuf), 10000 + 1024);

        /* Parse the file contents. */
        let (start_tli, start_lsn, previous_tli, previous_lsn) =
            backup_label::parse_backup_label(&pathbuf, &buf);

        /* Sanity checks. */
        if i > 0 && previous_tli == 0 {
            pg_fatal!(
                "backup at \"{}\" is a full backup, but only the first backup should be a full backup",
                backup_dirs[i]
            );
        }
        if i == 0 && previous_tli != 0 {
            pg_fatal!(
                "backup at \"{}\" is an incremental backup, but the first backup should be a full backup",
                backup_dirs[i]
            );
        }
        if i < n_backups - 1 && start_tli != check_tli {
            pg_fatal!(
                "backup at \"{}\" starts on timeline {}, but expected {}",
                backup_dirs[i],
                start_tli,
                check_tli
            );
        }
        if i < n_backups - 1 && start_lsn != check_lsn {
            pg_fatal!(
                "backup at \"{}\" starts at LSN {:X}/{:X}, but expected {:X}/{:X}",
                backup_dirs[i],
                (start_lsn >> 32) as u32,
                start_lsn as u32,
                (check_lsn >> 32) as u32,
                check_lsn as u32
            );
        }
        check_tli = previous_tli;
        check_lsn = previous_lsn;

        /* The last label in the chain is saved for later use. */
        if i == n_backups - 1 {
            lastbuf = buf;
        }
    }

    lastbuf
}

/// C: check_control_files — sanity check and return system_identifier.
fn check_control_files(backup_dirs: &[String]) -> u64 {
    let n_backups = backup_dirs.len();
    let mut system_identifier: u64 = 0;
    let mut data_checksum_version: u32 = 0;
    let mut data_checksum_mismatch = false;

    /* Try to read each control file in turn, last to first. */
    for i in (0..n_backups).rev() {
        let controlpath = format!("{}/{}", backup_dirs[i], XLOG_CONTROL_FILE);
        log_debug(&format!("reading \"{controlpath}\""));

        /*
         * C's frontend get_controlfile pg_fatals with "could not open file
         * \"%s\" for reading: %m"; probe the open first so the message (and
         * %m rendering) match, then delegate the read + CRC to the shared
         * crate.
         */
        if let Err(e) = std::fs::File::open(&controlpath) {
            pg_fatal!(
                "could not open file \"{}\" for reading: {}",
                controlpath,
                errno_message(&e)
            );
        }
        let (control_file, crc_ok) =
            match controldata_utils::get_controlfile_by_exact_path(&controlpath) {
                Ok(v) => v,
                Err(e) => {
                    log_error(e.message());
                    flog::exit_program(1);
                }
            };

        /* Control file contents not meaningful if CRC is bad. */
        if !crc_ok {
            pg_fatal!("{}: CRC is incorrect", controlpath);
        }

        /* Can't interpret control file if not current version. */
        if control_file.pg_control_version != PG_CONTROL_VERSION {
            pg_fatal!("{}: unexpected control file version", controlpath);
        }

        /* System identifiers should all match. */
        if i == n_backups - 1 {
            system_identifier = control_file.system_identifier;
        } else if system_identifier != control_file.system_identifier {
            pg_fatal!(
                "{}: expected system identifier {}, but found {}",
                controlpath,
                system_identifier,
                control_file.system_identifier
            );
        }

        /*
         * Detect checksum mismatches, but only if the last backup in the
         * chain has checksums enabled.
         */
        if i == n_backups - 1 {
            data_checksum_version = control_file.data_checksum_version;
        } else if data_checksum_version != 0
            && data_checksum_version != control_file.data_checksum_version
        {
            data_checksum_mismatch = true;
        }
    }

    log_debug(&format!("system identifier is {system_identifier}"));

    /*
     * Warn the user if not all backups are in the same state with regards
     * to checksums.
     */
    if data_checksum_mismatch {
        log_warning("only some backups have checksums enabled");
        log_warning_hint(
            "Disable, and optionally reenable, checksums on the output directory to avoid failures.",
        );
    }

    system_identifier
}

/// C: check_input_dir_permissions.
fn check_input_dir_permissions(dir: &Path) {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(dir) {
        Ok(st) => fsutil::set_data_directory_create_perm(st.mode()),
        Err(e) => pg_fatal!(
            "could not stat file \"{}\": {}",
            dir.display(),
            errno_message(&e)
        ),
    }
}

/// C: create_output_directory.
fn create_output_directory(dirname: &Path, opt: &CbOptions) {
    match fsutil::pg_check_dir(dirname) {
        0 => {
            if opt.dry_run {
                log_debug(&format!("would create directory \"{}\"", dirname.display()));
                return;
            }
            log_debug(&format!("creating directory \"{}\"", dirname.display()));
            if let Err(e) = fsutil::pg_mkdir_p(dirname) {
                pg_fatal!(
                    "could not create directory \"{}\": {}",
                    dirname.display(),
                    errno_message(&e)
                );
            }
            flog::remember_to_cleanup_directory(dirname, true);
        }
        1 => {
            log_debug(&format!("using existing directory \"{}\"", dirname.display()));
            flog::remember_to_cleanup_directory(dirname, false);
        }
        -1 => {
            let e = std::io::Error::last_os_error();
            pg_fatal!(
                "could not access directory \"{}\": {}",
                dirname.display(),
                errno_message(&e)
            );
        }
        _ => pg_fatal!("directory \"{}\" exists but is not empty", dirname.display()),
    }
}

/// C: parse_oid — non-zero OID without garbage. (C strtoul accepts leading
/// whitespace/+/hex, but directory entries here are plain digit strings; we
/// accept exactly the strings whose strtoul round-trip C accepts.)
fn parse_oid(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let v: u64 = s.parse().ok()?;
    if v < 1 || v > u32::MAX as u64 {
        return None;
    }
    Some(v as u32)
}

/// C: scan_for_existing_tablespaces.
fn scan_for_existing_tablespaces(
    pathname: &Path,
    opt: &CbOptions,
    output_dir: &str,
) -> Vec<CbTablespace> {
    let pg_tblspc = pathname.join(PG_TBLSPC_DIR);
    log_debug(&format!("scanning \"{}\"", pg_tblspc.display()));

    let dir = match std::fs::read_dir(&pg_tblspc) {
        Ok(d) => d,
        Err(e) => pg_fatal!(
            "could not open directory \"{}\": {}",
            pg_tblspc.display(),
            errno_message(&e)
        ),
    };

    let mut tslist: Vec<CbTablespace> = Vec::new();
    for de in dir {
        let de = match de {
            Ok(de) => de,
            Err(e) => pg_fatal!(
                "could not read directory \"{}\": {}",
                pg_tblspc.display(),
                errno_message(&e)
            ),
        };
        let name = de.file_name();
        let name_str = name.to_string_lossy().into_owned();
        let tblspcdir = pg_tblspc.join(&name);

        /* Ignore any file name that doesn't look like a proper OID. */
        let Some(oid) = parse_oid(&name_str) else {
            log_debug(&format!(
                "skipping \"{}\" because the filename is not a legal tablespace OID",
                tblspcdir.display()
            ));
            continue;
        };

        /* Only symbolic links and directories are tablespaces. */
        let ftype = fsutil::get_dirent_type(&tblspcdir);
        if ftype != fsutil::PgFileType::Lnk && ftype != fsutil::PgFileType::Dir {
            log_debug(&format!(
                "skipping \"{}\" because it is neither a symbolic link nor a directory",
                tblspcdir.display()
            ));
            continue;
        }

        let ts = if ftype == fsutil::PgFileType::Lnk {
            /* Read the link target. */
            let link_target = match std::fs::read_link(&tblspcdir) {
                Ok(t) => t,
                Err(e) => pg_fatal!(
                    "could not read symbolic link \"{}\": {}",
                    tblspcdir.display(),
                    errno_message(&e)
                ),
            };
            let link_target = link_target.to_string_lossy().into_owned();
            if !link_target.starts_with('/') {
                pg_fatal!("target of symbolic link \"{}\" is relative", tblspcdir.display());
            }

            /* Canonicalize the link target. */
            let link_target = fsutil::canonicalize_path(&link_target);

            /*
             * Find the corresponding tablespace mapping and copy the
             * relevant details into the new tablespace entry.
             */
            let Some(tsmap) = opt.tsmappings.iter().find(|m| m.old_dir == link_target) else {
                /* Every non-in-place tablespace must be mapped. */
                pg_fatal!("tablespace at \"{link_target}\" has no tablespace mapping");
            };
            CbTablespace {
                oid,
                in_place: false,
                old_dir: tsmap.old_dir.clone(),
                new_dir: tsmap.new_dir.clone(),
            }
        } else {
            /*
             * In-place tablespace: record the paths within the data
             * directories.
             */
            CbTablespace {
                oid,
                in_place: true,
                old_dir: format!("{}/{}", pg_tblspc.display(), name_str),
                new_dir: format!("{output_dir}/{PG_TBLSPC_DIR}/{name_str}"),
            }
        };

        /* Tablespaces should not share a directory. */
        for otherts in &tslist {
            if otherts.new_dir == ts.new_dir {
                pg_fatal!(
                    "tablespaces with OIDs {} and {} both point at directory \"{}\"",
                    otherts.oid,
                    oid,
                    ts.new_dir
                );
            }
        }

        tslist.push(ts);
    }

    tslist
}

/// C: read_pg_version_file.
fn read_pg_version_file(directory: &str) -> i32 {
    let filename = format!("{directory}/PG_VERSION");

    /* Read into memory. Length limit of 128 should be more than generous. */
    let buf = fsutil::slurp_file(Path::new(&filename), 128);

    /* Convert to integer (strtoul semantics: leading digits). */
    let mut i = 0usize;
    while i < buf.len() && buf[i].is_ascii_whitespace() && buf[i] != b'\n' {
        i += 1;
    }
    let start = i;
    let mut version: u64 = 0;
    while i < buf.len() && buf[i].is_ascii_digit() {
        version = version.saturating_mul(10).saturating_add(u64::from(buf[i] - b'0'));
        i += 1;
    }
    let parsed_some = i > start;
    let next = buf.get(i).copied();
    if !parsed_some || next != Some(b'\n') {
        /*
         * Multi-part version numbers (9.6, 8.4) mean a server too old for
         * incremental backup.
         */
        if parsed_some && version < 10 && next == Some(b'.') {
            pg_fatal!("{}: server version too old", filename);
        }
        pg_fatal!("{}: could not parse version number", filename);
    }

    log_debug(&format!("read server version {version} from file \"{filename}\""));
    (version as i32) * 10000
}

/// C: process_directory_recursively — copy files from the input directory to
/// the output directory, reconstructing full files from incremental files as
/// required.
#[allow(clippy::too_many_arguments)]
fn process_directory_recursively(
    tsoid: Option<u32>,
    input_directory: &Path,
    output_directory: &Path,
    relative_path: Option<&str>,
    prior_backup_dirs: &[String],
    manifests: &[Option<ManifestData>],
    mwriter: &mut Option<ManifestWriter>,
    opt: &CbOptions,
) {
    let n_prior_backups = prior_backup_dirs.len();
    let latest_manifest = manifests[n_prior_backups].as_ref();

    /*
     * Classify this directory: pg_tblspc toplevel, pg_wal subtree, and the
     * directories that can contain incremental files needing reconstruction.
     */
    let mut is_pg_tblspc = false;
    let mut is_pg_wal = false;
    let mut is_incremental_dir = false;
    if tsoid.is_some() {
        is_incremental_dir = true;
    } else if let Some(rp) = relative_path {
        is_pg_tblspc = rp == PG_TBLSPC_DIR;
        is_pg_wal = rp == "pg_wal" || rp.starts_with("pg_wal/");
        is_incremental_dir =
            rp.starts_with("base/") || rp == "global" || rp.starts_with("pg_tblspc/");
    }

    /*
     * Files under pg_wal are not in the backup manifest, so no checksums.
     */
    let checksum_type = if !is_pg_wal {
        opt.manifest_checksums
    } else {
        PgChecksumType::None
    };

    /*
     * Append the relative path to the input and output directories, and
     * figure out the manifest prefix.
     */
    let (ifulldir, ofulldir, manifest_prefix) = match relative_path {
        None => (
            input_directory.to_path_buf(),
            output_directory.to_path_buf(),
            match tsoid {
                Some(oid) => format!("{PG_TBLSPC_DIR}/{oid}/"),
                None => String::new(),
            },
        ),
        Some(rp) => (
            input_directory.join(rp),
            output_directory.join(rp),
            match tsoid {
                Some(oid) => format!("{PG_TBLSPC_DIR}/{oid}/{rp}/"),
                None => format!("{rp}/"),
            },
        ),
    };

    /*
     * Toplevel output directories already exist; subdirectories are our
     * responsibility.
     */
    if relative_path.is_some() {
        if opt.dry_run {
            log_debug(&format!("would create directory \"{}\"", ofulldir.display()));
        } else {
            log_debug(&format!("creating directory \"{}\"", ofulldir.display()));
            if let Err(e) = fsutil::mkdir_mode(&ofulldir) {
                pg_fatal!(
                    "could not create directory \"{}\": {}",
                    ofulldir.display(),
                    errno_message(&e)
                );
            }
        }
    }

    /* It's time to scan the directory. */
    let dir = match std::fs::read_dir(&ifulldir) {
        Ok(d) => d,
        Err(e) => pg_fatal!(
            "could not open directory \"{}\": {}",
            ifulldir.display(),
            errno_message(&e)
        ),
    };

    for de in dir {
        let de = match de {
            Ok(de) => de,
            Err(e) => pg_fatal!(
                "could not read directory \"{}\": {}",
                ifulldir.display(),
                errno_message(&e)
            ),
        };
        let dname = de.file_name();
        let dname_str = dname.to_string_lossy().into_owned();

        /* Construct input path. */
        let ifullpath = ifulldir.join(&dname);

        /* Figure out what kind of directory entry this is. */
        let ftype = fsutil::get_dirent_type(&ifullpath);

        /*
         * In pg_tblspc, skip anything scan_for_existing_tablespaces would
         * have considered a tablespace.
         */
        if is_pg_tblspc
            && parse_oid(&dname_str).is_some()
            && (ftype == fsutil::PgFileType::Lnk || ftype == fsutil::PgFileType::Dir)
        {
            continue;
        }

        /* If it's a directory, recurse. */
        if ftype == fsutil::PgFileType::Dir {
            let new_relative_path = match relative_path {
                None => dname_str.clone(),
                Some(rp) => format!("{rp}/{dname_str}"),
            };
            process_directory_recursively(
                tsoid,
                input_directory,
                output_directory,
                Some(&new_relative_path),
                prior_backup_dirs,
                manifests,
                mwriter,
                opt,
            );
            continue;
        }

        /* Skip anything that's not a regular file. */
        if ftype != fsutil::PgFileType::Reg {
            if ftype == fsutil::PgFileType::Lnk {
                log_warning(&format!("skipping symbolic link \"{}\"", ifullpath.display()));
            } else {
                log_warning(&format!("skipping special file \"{}\"", ifullpath.display()));
            }
            continue;
        }

        /*
         * Skip the backup_label and backup_manifest files; they require
         * special handling and are handled elsewhere.
         */
        if relative_path.is_none()
            && (dname_str == "backup_label" || dname_str == "backup_manifest")
        {
            continue;
        }

        /*
         * If it's an incremental file, hand it off to the reconstruction
         * code, which will figure out what to do.
         */
        let mut checksum_payload: Option<Vec<u8>> = None;
        let ofullpath: PathBuf;
        let manifest_path: Vec<u8>;
        if is_incremental_dir && dname_str.starts_with(INCREMENTAL_PREFIX) {
            let bare_name = &dname_str[INCREMENTAL_PREFIX.len()..];

            /* Output path should not include "INCREMENTAL." prefix. */
            ofullpath = ofulldir.join(bare_name);

            /* Manifest path likewise omits incremental prefix. */
            let mut mp = manifest_prefix.clone().into_bytes();
            mp.extend_from_slice(bare_name.as_bytes());
            manifest_path = mp;

            /* Reconstruction logic will do the rest. */
            checksum_payload = reconstruct::reconstruct_from_incremental_file(
                &ifullpath,
                &ofullpath,
                &manifest_prefix,
                bare_name,
                prior_backup_dirs,
                manifests,
                &manifest_path,
                checksum_type,
                opt.copy_method,
                opt.debug,
                opt.dry_run,
            );
        } else {
            /* Construct the path that the backup_manifest will use. */
            let mut mp = manifest_prefix.clone().into_bytes();
            mp.extend_from_slice(dname.as_os_str().as_bytes());
            manifest_path = mp;

            /*
             * Not an incremental file: copy the entire file, reusing the
             * final input manifest's checksum when possible.
             */
            if checksum_type != PgChecksumType::None {
                if let Some(latest_manifest) = latest_manifest {
                    match latest_manifest.files.get(&manifest_path) {
                        None => {
                            /*
                             * The directory is out of sync with the
                             * backup_manifest, so emit a warning.
                             */
                            log_warning(&format!(
                                "manifest file \"{}/backup_manifest\" contains no entry for file \"{}\"",
                                input_directory.display(),
                                String::from_utf8_lossy(&manifest_path)
                            ));
                        }
                        Some(mfile) => {
                            if mfile.checksum_type == checksum_type {
                                checksum_payload =
                                    Some(mfile.checksum_payload.clone().unwrap_or_default());
                            }
                        }
                    }
                }
            }

            /*
             * If we're reusing a checksum, copy_file doesn't need to compute
             * one.
             */
            let mut checksum_ctx = if checksum_payload.is_some() {
                PgChecksumContext::init(PgChecksumType::None)
            } else {
                PgChecksumContext::init(checksum_type)
            };

            /* Actually copy the file. */
            ofullpath = ofulldir.join(&dname);
            copy_file::copy_file(
                &ifullpath,
                &ofullpath,
                &mut checksum_ctx,
                opt.copy_method,
                opt.dry_run,
            );

            /*
             * If copy_file() performed a checksum calculation for us, then
             * save the results (except in dry-run mode, when there's no
             * point).
             */
            if checksum_ctx.checksum_type() != PgChecksumType::None && !opt.dry_run {
                let mut payload = [0u8; PG_CHECKSUM_MAX_LENGTH];
                let len = checksum_ctx.finalize(&mut payload);
                checksum_payload = Some(payload[..len].to_vec());
            }
        }

        /* Generate manifest entry, if needed. */
        if let Some(mwriter) = mwriter.as_mut() {
            /*
             * We need the file size and mtime for the manifest entry, and
             * only stat() can tell us the mtime.
             */
            let (size, mtime) = match std::fs::metadata(&ofullpath) {
                Ok(md) => (md.len(), write_manifest::mtime_of(&md)),
                Err(e) => pg_fatal!(
                    "could not stat file \"{}\": {}",
                    ofullpath.display(),
                    errno_message(&e)
                ),
            };
            mwriter.add_file(
                &manifest_path,
                size,
                mtime,
                checksum_type,
                checksum_payload.as_deref().unwrap_or(&[]),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_backup_label_full_and_incremental() {
        let full = b"START WAL LOCATION: 0/D00028 (file 00000001000000000000000D)\n\
CHECKPOINT LOCATION: 0/D00080\n\
START TIMELINE: 1\n";
        let (tli, lsn, ptli, plsn) = backup_label::parse_backup_label("x", full);
        assert_eq!((tli, lsn, ptli, plsn), (1, 0x0000_0000_00D0_0028, 0, 0));

        let incr = b"START WAL LOCATION: 1/AB00028 (file 000000020000000100000ab0)\n\
START TIMELINE: 2\n\
INCREMENTAL FROM LSN: 0/D00028\n\
INCREMENTAL FROM TLI: 1\n";
        let (tli, lsn, ptli, plsn) = backup_label::parse_backup_label("x", incr);
        assert_eq!(tli, 2);
        assert_eq!(lsn, 0x0000_0001_0AB0_0028);
        assert_eq!(ptli, 1);
        assert_eq!(plsn, 0x0000_0000_00D0_0028);
    }

    #[test]
    fn parse_lsn_sscanf_semantics() {
        /* trailing junk is fine; consumed count reported */
        let (lsn, n) = backup_label::parse_lsn(b"12/34 rest").unwrap();
        assert_eq!(lsn, 0x12_0000_0034);
        assert_eq!(n, 5);
        /* whitespace before a number is skipped, not before '/' */
        assert!(backup_label::parse_lsn(b"12 /34").is_none());
        assert!(backup_label::parse_lsn(b"zz/34").is_none());
    }

    #[test]
    fn canonicalize_path_cases() {
        assert_eq!(fsutil::canonicalize_path("/a/b/"), "/a/b");
        assert_eq!(fsutil::canonicalize_path("/a//b/./c"), "/a/b/c");
        assert_eq!(fsutil::canonicalize_path("/a/b/../c"), "/a/c");
        assert_eq!(fsutil::canonicalize_path("/"), "/");
        assert_eq!(fsutil::canonicalize_path("/.."), "/");
    }

    #[test]
    fn parse_oid_matches_c() {
        assert_eq!(parse_oid("16384"), Some(16384));
        assert_eq!(parse_oid("1"), Some(1));
        assert_eq!(parse_oid("0"), None); /* oid < 1 */
        assert_eq!(parse_oid(""), None);
        assert_eq!(parse_oid("4294967295"), Some(u32::MAX));
        assert_eq!(parse_oid("4294967296"), None);
        assert_eq!(parse_oid("12x"), None);
        assert_eq!(parse_oid("-1"), None);
    }

    #[test]
    fn format_gmtime_matches_strftime() {
        /* date -u -r 1786247466 => 2026-08-09 03:51:06 UTC (glibc %Z of gmtime prints GMT) */
        assert_eq!(write_manifest::format_gmtime(1786247466), "2026-08-09 03:51:06 GMT");
        assert_eq!(write_manifest::format_gmtime(0), "1970-01-01 00:00:00 GMT");
        assert_eq!(write_manifest::format_gmtime(951827696), "2000-02-29 12:34:56 GMT");
    }

    #[test]
    fn escape_json_matches_c() {
        let mut buf = Vec::new();
        write_manifest::escape_json(&mut buf, "a\"b\\c\nd\x01e");
        assert_eq!(buf, b"\"a\\\"b\\\\c\\nd\\u0001e\"");
    }

    #[test]
    fn sync_method_parsing() {
        assert!(parse_sync_method("fsync"));
        assert!(!parse_sync_method("nosuch"));
        assert!(!parse_sync_method("syncfs"));
    }
}
