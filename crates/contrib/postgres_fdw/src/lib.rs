//! `contrib/postgres_fdw`: option validation, shippability, deparser, the
//! planner arms (rel size/paths/plan, remote estimates, join and
//! grouped-aggregate pushdown), the connection layer (connection.c over
//! `crates/interfaces/pgclient`), the scan executor (cursor-batched, async),
//! and DML (per-row prepared statements, batch insert, direct modify).
//! Unported: sort/LIMIT (ORDERED/FINAL) pushdown, pathkey paths
//! (add_paths_with_pathkeys_for_rel), EPQ-capable pushed join paths under
//! UPDATE/DELETE/row locks, row triggers on foreign tables, COPY into
//! foreign tables (BeginForeignInsert).
#![allow(non_snake_case)]

pub mod connection;
pub mod deparse;
pub mod exec;
pub mod import;
pub mod modify;
pub mod option;
pub mod plan;
pub mod relinfo;
pub mod shippable;
pub mod transmission;

use types_error::ErrorLocation;

pub(crate) const LIBRARY: &str = "postgres_fdw";

#[track_caller]
pub(crate) fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

pub fn init_seams() {
    plan::install();
    foreigncmds::install_fdw_import_routine(
        types_nodes::FdwKind::PostgresFdw,
        import::postgresImportForeignSchema,
    );
}
