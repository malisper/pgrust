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
//! What is now **landed** (the consumer-facing SELECT/cursor core, since the
//! executor driver, plancache, portal, parser and dest-router substrate landed):
//!
//! * the `DestSPI` receiver ([`mod@dest_spi`]: `spi_dest_startup` /
//!   `spi_printtup`), registered into the `backend-tcop-dest` router via the
//!   `create_spi_dest_receiver` seam (the router's `CreateDestReceiver(Spi)` arm
//!   calls it, mirroring printtup/copyto);
//! * the value-returning SELECT path ([`mod@select`]: `spi_execute_select` /
//!   `spi_query_tupdesc`) — `SPI_connect` → one-shot `CreateOneShotCachedPlan` +
//!   parse-analyze + `CompleteCachedPlan` → `GetCachedPlan` →
//!   `CreateQueryDesc` + `ExecutorStart/Run/Finish/End` to the DestSPI receiver
//!   → collected rows → `SPI_finish`;
//! * the forward cursor fetch ([`mod@cursor`]: `spi_cursor_fetch` /
//!   `spi_cursor_tupdesc`) — `GetPortalByName` + `PortalRunFetch` into DestSPI;
//! * the tuple accessors ([`mod@accessors`]: `SPI_getbinval`, `SPI_fnumber`,
//!   `SPI_gettypeid`, `SPI_getvalue`).
//!
//! These unblock the xml `table/query/cursor_to_xml(schema)` family and the
//! tsvector trigger / `ts_rewrite` SPI reads.
//!
//! What remains **seam-and-panic** (honest decomp-stub bodies that `panic!`,
//! never `todo!`): the prepared-plan / `SpiPlanPtr` legs the RI triggers use
//! ([`exec`]: `spi_prepare` / `spi_keepplan` / `spi_execute_snapshot` /
//! `spi_first_row_columns`). `SpiPlanPtr` is still the opaque `u64` handle in
//! `types-ri-triggers` (not the real `_SPI_plan` carrier holding
//! `plancache_list`/`argtypes`/`nargs`/`saved`); re-modelling it to the real
//! struct + wiring `SPI_prepare`/`SPI_execute_plan` over a saved cached plan is
//! a separate keystone, and the non-SELECT executor (DML / utility /
//! `FOR UPDATE` / parallel) is the execMain `#167 F0d` boundary. Only the plain
//! read-only SELECT the xml/tsvector consumers issue is wired here.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

mod accessors;
mod backbone;
mod cursor;
mod dest_spi;
mod exec;
mod prepare;
mod result_code;
mod select;

pub use accessors::*;
pub use backbone::*;
pub use cursor::{spi_cursor_fetch, spi_cursor_tupdesc};
pub use dest_spi::create_spi_dest_receiver;
pub use result_code::*;
pub use select::{spi_execute_select, spi_query_tupdesc};

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

    // --- DestSPI receiver registration (called by the tcop-dest router's
    //     CreateDestReceiver(Spi) arm) ---
    seams::create_spi_dest_receiver::set(dest_spi::create_spi_dest_receiver);

    // --- consumer-facing high-level SPI seams (declared in the consumers'
    //     seam crates; the SPI owner installs them) ---
    // xml: table/query/cursor-to-xml SELECT + descriptor reads.
    backend_utils_adt_xml_libxml_seams::spi_execute_select::set(select::spi_execute_select);
    backend_utils_adt_xml_libxml_seams::spi_query_tupdesc::set(select::spi_query_tupdesc);
    backend_utils_adt_xml_libxml_seams::spi_cursor_fetch::set(cursor::spi_cursor_fetch);
    backend_utils_adt_xml_libxml_seams::spi_cursor_tupdesc::set(cursor::spi_cursor_tupdesc);

    // --- prepared-plan execution legs (real bodies: the RI / plpgsql path) ---
    seams::spi_prepare::set(prepare::spi_prepare);
    seams::spi_keepplan::set(prepare::spi_keepplan);
    seams::spi_freeplan::set(prepare::spi_freeplan);
    seams::spi_plan_is_valid::set(prepare::spi_plan_is_valid);
    seams::spi_execute_snapshot::set(prepare::spi_execute_snapshot);
    seams::spi_first_row_columns::set(prepare::spi_first_row_columns);

    // --- seam-and-panic cursor leg (ts_rewrite SPI cursor; honest decomp-stub) ---
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
