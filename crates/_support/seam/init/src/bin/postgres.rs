//! The `postgres` executable entry shell — the C-ABI `main()` over
//! [`main_main::pg_main`].
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

use main_main::{pg_main, MainOutcome};

fn main() {
    // Collect process arguments as owned strings, then borrow them as the
    // `&[&str]` slice `pg_main` expects (argv[0] is the executable name).
    let owned: Vec<String> = std::env::args().collect();
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
            print!("{text}");
            std::process::exit(0);
        }
        Ok(MainOutcome::Dispatched) => {
            // The dispatch arms never return in C (the trailing abort() is
            // unreachable); reaching here means a subprogram returned, which is
            // a logic error.
            std::process::exit(0);
        }
        Err(err) => {
            // C's FATAL startup failures print to stderr and exit(1).
            eprintln!("{}: {}", argv.first().copied().unwrap_or("postgres"), err);
            std::process::exit(1);
        }
    }
}
