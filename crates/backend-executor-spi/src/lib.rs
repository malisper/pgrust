//! `backend-executor-spi` — the Server Programming Interface (`executor/spi.c`).
//!
//! # Port status: NEEDS_DECOMP (scaffold + grounded backbone + residue)
//!
//! `spi.c` (3404 LOC) is the SPI layer PL/pgSQL, the RI triggers, and many
//! C extensions use to run SQL from C. It is deeply coupled to *unported*
//! owners:
//!
//! * the executor driver `ExecutorStart/Run/Finish/End` + `CreateQueryDesc`
//!   (`backend-executor-execMain`, **needs-decomp**; #167 wired only a plain
//!   `DestNone`/`DestRemote` SELECT and guard-panics on every other path);
//! * the parser/analyzer `raw_parser` / `pg_analyze_and_rewrite_*`
//!   (`backend-tcop-postgres` + `backend-parser-analyze`, **todo**);
//! * the portal driver `PortalStart` / `PortalRunFetch`
//!   (`backend-tcop-pquery`, **todo**);
//! * the `CreateDestReceiver(DestSPI)` router (`backend-tcop-dest`, **todo** —
//!   the receiver-value keystone, #166-F0b / #168 / #169; the
//!   `create_dest_receiver` seam is declared but **installed by nobody**, so
//!   the `DestSPI` vtable cannot be registered with a router that does not
//!   exist).
//!
//! What is **grounded** here (the pieces with merged consumers on the
//! transaction hot path, all mirror-PG-and-panic-free):
//!
//! * the connection / nesting machinery: [`SPI_connect`], [`SPI_connect_ext`],
//!   [`SPI_finish`], [`AtEOXact_SPI`], [`AtEOSubXact_SPI`],
//!   [`SPI_inside_nonatomic_context`], the `_SPI_stack` / `_SPI_current` /
//!   `_SPI_connected` backend-globals (as `thread_local!`), and the
//!   `_SPI_begin_call` / `_SPI_end_call` / `_SPI_execmem` / `_SPI_procmem`
//!   helpers;
//! * the SPI result-code constants and [`SPI_result_code_string`].
//!
//! What is **seam-and-panic** (faithful structure, honest decomp-stub bodies
//! that `panic!` into the genuinely-unported owner, never `todo!`): the
//! execute / prepare / cursor legs ([`spi::spi_execute_snapshot`],
//! [`spi::spi_prepare`], cursor open/fetch) and the `DestSPI` install. These
//! cannot be filled until the executor-driver / parser / portal / dest-router
//! owners above land. See the per-fn doc comments for the exact prerequisite.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

mod accessors;
mod backbone;
mod exec;
mod result_code;

pub use accessors::*;
pub use backbone::*;
pub use result_code::*;

/// Install SPI's inward seams (the ones `backend-executor-spi-seams` declares
/// and other crates already consume). Wired into `seams-init`.
pub fn init_seams() {
    use backend_executor_spi_seams as seams;

    // --- grounded backbone (real bodies) ---
    seams::at_eoxact_spi::set(backbone::AtEOXact_SPI);
    seams::at_eosubxact_spi::set(backbone::AtEOSubXact_SPI);
    seams::spi_inside_nonatomic_context::set(backbone::SPI_inside_nonatomic_context);
    seams::spi_connect::set(backbone::spi_connect_seam);
    seams::spi_finish::set(backbone::spi_finish_seam);
    seams::spi_result_code_string::set(result_code::spi_result_code_string_seam);

    // --- seam-and-panic execution/prepare/cursor legs (honest decomp-stubs) ---
    seams::spi_prepare::set(exec::spi_prepare_seam);
    seams::spi_keepplan::set(exec::spi_keepplan_seam);
    seams::spi_freeplan::set(exec::spi_freeplan_seam);
    seams::spi_plan_is_valid::set(exec::spi_plan_is_valid_seam);
    seams::spi_execute_snapshot::set(exec::spi_execute_snapshot_seam);
    seams::spi_first_row_columns::set(exec::spi_first_row_columns_seam);
    seams::tsquery_rewrite_run::set(exec::tsquery_rewrite_run_seam);

    // matview.c's refresh_by_match_merge drives SPI through its own outward
    // frontier seam crate; spi.c owns the bodies. The execute/exec/getvalue/
    // processed legs stay on the SPI executor-driver keystone (exec.rs panics).
    {
        use backend_commands_matview_deps_seams as m;
        m::spi_connect::set(backbone::spi_connect_seam);
        m::spi_finish::set(backbone::spi_finish_seam);
    }
}
