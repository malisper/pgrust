//! C: src/bin/pg_walsummary/pg_walsummary.c (18.3) — prints the contents of
//! WAL summary files.
//!
//! Ported whole: option parsing (getopt_long-compatible, GNU permutation,
//! -i/--individual and -q/--quiet), the per-file read loop over the
//! blkreftable reader, and dump_one_relation's limit/block/range output.
//! Output bytes, error message identity, and exit codes pinned to C (the
//! blkreftable reader's own error texts — wrong magic number, wrong
//! checksum, ends unexpectedly — already match C's blkreftable.c).

use std::io::Read;

use types_core::{BlockNumber, ForkNumber, InvalidBlockNumber};
use types_storage::RelFileLocator;

/// The pg_walsummary version whose behavior this port tracks.
pub const PG_WALSUMMARY_VERSION: &str = "18.6";

const PROGNAME: &str = "pg_walsummary";

/// C: ws_options.
#[derive(Default)]
struct WsOptions {
    individual: bool,
    quiet: bool,
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
            println!("pg_walsummary (PostgreSQL) {PG_WALSUMMARY_VERSION} (pgrust)");
            std::process::exit(0);
        }
    }

    let mut opt = WsOptions::default();
    let files = parse_options(&args, &mut opt);

    if files.is_empty() {
        log_error("no input files specified");
        usage_hint();
        std::process::exit(1);
    }

    let root = mcx::MemoryContext::new("pg_walsummary");
    /* C: the static block_buffer, retained across files and relations. */
    let mut block_buffer: Vec<BlockNumber> = Vec::new();

    for filename in &files {
        let mut file = match std::fs::File::open(filename) {
            Ok(f) => f,
            Err(e) => pg_fatal(&format!(
                "could not open file \"{filename}\": {}",
                errno_message(&e)
            )),
        };

        /* C: walsummary_read_callback — read(2), pg_fatal on error. */
        let read_cb = |buf: &mut [u8]| -> types_error::PgResult<usize> {
            match file.read(buf) {
                Ok(n) => Ok(n),
                Err(e) => pg_fatal(&format!(
                    "could not read file \"{filename}\": {}",
                    errno_message(&e)
                )),
            }
        };

        let mut reader = blkreftable::BlockRefTableReader::new(root.mcx(), read_cb, filename)
            .unwrap_or_else(|e| error_exit(&e));
        loop {
            match reader.next_relation() {
                Ok(Some((rlocator, forknum, limit_block))) => {
                    dump_one_relation(
                        &opt,
                        rlocator,
                        forknum,
                        limit_block,
                        &mut reader,
                        &mut block_buffer,
                    );
                }
                Ok(None) => break,
                Err(e) => error_exit(&e),
            }
        }
    }

    std::process::exit(0);
}

/// C: dump_one_relation — the limit-block line, then the (sorted) modified
/// blocks as ranges, or one line per block under --individual.
fn dump_one_relation<R: FnMut(&mut [u8]) -> types_error::PgResult<usize>>(
    opt: &WsOptions,
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    limit_block: BlockNumber,
    reader: &mut blkreftable::BlockRefTableReader<'_, '_, R>,
    block_buffer: &mut Vec<BlockNumber>,
) {
    let ts = rlocator.spcOid;
    let db = rlocator.dbOid;
    let rel = rlocator.relNumber;
    let fork = fork_name(forknum);

    /* Dump limit block, if any. */
    if limit_block != InvalidBlockNumber {
        println!("TS {ts}, DB {db}, REL {rel}, FORK {fork}: limit {limit_block}");
    }

    /* Collect every modified block for this relation fork (C grows its
     * static buffer, doubling, until one GetBlocks call comes back short;
     * looping to 0 collects the identical set). */
    block_buffer.clear();
    let mut chunk = [0 as BlockNumber; 512];
    loop {
        let nblocks = match reader.get_blocks(&mut chunk) {
            Ok(n) => n,
            Err(e) => error_exit(&e),
        };
        if nblocks == 0 {
            break;
        }
        block_buffer.extend_from_slice(&chunk[..nblocks]);
    }

    /* If we don't need to produce any output, skip the rest of this. */
    if opt.quiet {
        return;
    }

    /* C: qsort with compare_block_numbers (pg_cmp_u32) — u32 keys have no
     * distinguishable ties, so an unstable sort is order-identical. */
    block_buffer.sort_unstable();

    /* Dump block references. */
    let mut i = 0usize;
    while i < block_buffer.len() {
        /* Find the next range of blocks to print, but if --individual was
         * specified, then consider each block a separate range. */
        let startblock = block_buffer[i];
        let mut endblock = startblock;
        i += 1;
        if !opt.individual {
            while i < block_buffer.len() && block_buffer[i] == endblock + 1 {
                endblock += 1;
                i += 1;
            }
        }

        if startblock == endblock {
            println!("TS {ts}, DB {db}, REL {rel}, FORK {fork}: block {startblock}");
        } else {
            println!("TS {ts}, DB {db}, REL {rel}, FORK {fork}: blocks {startblock}..{endblock}");
        }
    }
}

/// C: forkNames[forknum] (relpath.c). The reader collapses out-of-range fork
/// numbers (possible only in corrupt files, where C indexes off the end of
/// forkNames) to InvalidForkNumber; fail cleanly instead.
fn fork_name(forknum: ForkNumber) -> &'static str {
    match forknum {
        ForkNumber::MAIN_FORKNUM => "main",
        ForkNumber::FSM_FORKNUM => "fsm",
        ForkNumber::VISIBILITYMAP_FORKNUM => "vm",
        ForkNumber::INIT_FORKNUM => "init",
        ForkNumber::InvalidForkNumber => {
            log_error("invalid fork number");
            std::process::exit(1);
        }
    }
}

/// C: walsummary_error_callback — pg_log_generic(PG_LOG_ERROR, ...) + exit(1),
/// fed here by the blkreftable reader's PgError (message text already C's).
fn error_exit(e: &types_error::PgError) -> ! {
    log_error(e.message());
    std::process::exit(1);
}

/// getopt_long(argc, argv, "iq", long_options) with GNU permutation:
/// returns the operands (the FILE... arguments). Exits like C on bad options.
fn parse_options(args: &[String], opt: &mut WsOptions) -> Vec<String> {
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
            match long {
                "individual" => opt.individual = true,
                "quiet" => opt.quiet = true,
                _ => {
                    eprintln!("{PROGNAME}: unrecognized option '--{long}'");
                    usage_hint();
                    std::process::exit(1);
                }
            }
            i += 1;
            continue;
        }

        /* Short options, possibly bundled. */
        for &b in &arg.as_bytes()[1..] {
            match b {
                b'i' => opt.individual = true,
                b'q' => opt.quiet = true,
                c => {
                    eprintln!("{PROGNAME}: invalid option -- '{}'", c as char);
                    usage_hint();
                    std::process::exit(1);
                }
            }
        }
        i += 1;
    }
    operands
}

/// C: help.
fn help() {
    print!(
        "{PROGNAME} prints the contents of a WAL summary file.\n\n\
Usage:\n\
  {PROGNAME} [OPTION]... FILE...\n\
\nOptions:\n\
  -i, --individual          list block numbers individually, not as ranges\n\
  -q, --quiet               don't print anything, just parse the files\n\
  -V, --version             output version information, then exit\n\
  -?, --help                show this help, then exit\n\
\nReport bugs to <pgsql-bugs@lists.postgresql.org>.\n\
PostgreSQL home page: <https://www.postgresql.org/>\n"
    );
}

fn log_error(msg: &str) {
    eprintln!("{PROGNAME}: error: {msg}");
}

fn usage_hint() {
    eprintln!("{PROGNAME}: hint: Try \"{PROGNAME} --help\" for more information.");
}

/// C: pg_fatal — log the error and exit(1).
fn pg_fatal(msg: &str) -> ! {
    log_error(msg);
    std::process::exit(1);
}

/// C: %m — strerror(errno) for a std::io::Error, without Rust's
/// " (os error N)" suffix.
fn errno_message(e: &std::io::Error) -> String {
    match e.raw_os_error() {
        Some(errnum) => {
            // SAFETY: strerror returns a NUL-terminated static string.
            let s = unsafe { std::ffi::CStr::from_ptr(libc::strerror(errnum)) };
            s.to_string_lossy().into_owned()
        }
        None => e.to_string(),
    }
}
