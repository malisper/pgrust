//! The `postgres` executable entry shell — the C-ABI `main()` over
//! [`::main_main::pg_main`].
//!
//! Mirrors `src/backend/main/main.c`'s `main(argc, argv)` flow:
//!
//! 1. Create the top-level memory context (C's very early `MemoryContextInit`
//!    creates `TopMemoryContext`; here the binary owns the long-lived context
//!    and hands a `'static` handle to `pg_main`). It is leaked deliberately — it
//!    lives for the whole process, exactly like C's `TopMemoryContext`.
//! 2. Install every ported crate's inward seams (`init::init_all`). C has
//!    no analog (its functions are directly linked); in this tree the seam
//!    registry must be populated before any cross-crate call.
//! 3. Build the `argv` slice and dispatch through `pg_main`. The `--help`/
//!    `--version`/`--describe-config` fast paths come back as
//!    [`MainOutcome::PrintAndExit`] (C writes stdout and `exit(0)`s); a fatal
//!    startup error comes back as `Err` (C `exit(1)`s).
//!
//! Note: the dispatch targets (postmaster / single-user / bootstrap) are mostly
//! still seam-and-panic until their owners land, so running this today reaches a
//! loud panic once it dispatches — but the entry shell itself is complete and
//! compiles into a real `postgres` binary.

use ::main_main::{pg_main, MainOutcome};

fn main() {
    // On wasm64-unknown-unknown std's panic message goes to std stderr, which is
    // a no-op — so a panic (e.g. an `.expect()` on a startup PgError) aborts
    // invisibly. Install a panic hook that routes the panic location + message to
    // the host stderr so boot failures are diagnosable.
    #[cfg(target_family = "wasm")]
    std::panic::set_hook(Box::new(|info| {
        let msg = format!("PANIC: {info}\n");
        wasm_libc_shim::stderr_write(msg.as_bytes());
    }));

    // Collect process arguments as owned strings, then borrow them as the
    // `&[&str]` slice `pg_main` expects (argv[0] is the executable name).
    //
    // On `wasm64-unknown-unknown` there is no WASI, so `std::env::args()` is
    // empty; the host harness provides `argv` through a `pgvfs` import instead.
    #[cfg(not(target_family = "wasm"))]
    let owned: Vec<String> = std::env::args().collect();
    #[cfg(target_family = "wasm")]
    let owned: Vec<String> = {
        let a = wasm_libc_shim::host_args();
        if a.is_empty() {
            // Fall back to a bare program name so dispatch still runs.
            vec!["postgres".to_string()]
        } else {
            a
        }
    };
    let argv: Vec<&str> = owned.iter().map(String::as_str).collect();

    // C's `MemoryContextInit()` builds `TopMemoryContext`; this tree's mcx model
    // has no global context, so the binary owns the top context and leaks it so
    // its `'static` handle is valid for the whole process.
    let top_ctx: &'static mcx::MemoryContext =
        Box::leak(Box::new(mcx::MemoryContext::new("TopMemoryContext")));
    let mcx = top_ctx.mcx();

    // Populate the seam registry before any cross-crate call.
    init::init_all();

    match pg_main(mcx, &argv) {
        Ok(MainOutcome::PrintAndExit(text)) => {
            // C writes the banner/help to stdout and exit(0).
            out_stdout(&text);
            exit_process(0);
        }
        Ok(MainOutcome::Dispatched) => {
            // The dispatch arms never return in C (the trailing abort() is
            // unreachable); reaching here means a subprogram returned, which is
            // a logic error.
            exit_process(0);
        }
        Ok(MainOutcome::Initdb) => {
            // `pgrust initdb` driver — re-execs this binary for --boot/--single.
            // Not built on wasm (no subprocesses there).
            #[cfg(not(target_family = "wasm"))]
            match initdb::initdb_main(&argv) {
                Ok(()) => exit_process(0),
                Err(msg) => {
                    out_stderr(&format!("initdb: error: {msg}\n"));
                    exit_process(1);
                }
            }
            #[cfg(target_family = "wasm")]
            exit_process(1);
        }
        Err(err) => {
            // C's FATAL startup failures print to stderr and exit(1).
            out_stderr(&format!(
                "{}: {}\n",
                argv.first().copied().unwrap_or("postgres"),
                err
            ));
            exit_process(1);
        }
    }
}

/// Write to stdout. std's stdout is a no-op on wasm64-unknown-unknown, so route
/// to the host import there; natively use `print!`.
fn out_stdout(s: &str) {
    #[cfg(not(target_family = "wasm"))]
    {
        use std::io::Write;
        print!("{s}");
        let _ = std::io::stdout().flush();
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::stdout_write(s.as_bytes());
    }
}

/// Write to stderr (host import on wasm64, `eprint!` natively).
fn out_stderr(s: &str) {
    #[cfg(not(target_family = "wasm"))]
    {
        eprint!("{s}");
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::stderr_write(s.as_bytes());
    }
}

/// Exit the process. `std::process::exit` traps (`unreachable`) on
/// wasm64-unknown-unknown because there is no exit syscall; route to the host
/// `proc_exit` import, which the harness turns into a clean shutdown.
fn exit_process(code: i32) -> ! {
    #[cfg(not(target_family = "wasm"))]
    {
        std::process::exit(code);
    }
    #[cfg(target_family = "wasm")]
    {
        wasm_libc_shim::proc_exit(code)
    }
}
