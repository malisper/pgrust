//! `TableSpaceOpts` (`commands/tablespace.h`).

/// Parsed `pg_tablespace.spcoptions` (`tablespace_reloptions`). A negative
/// value means "not set, use the GUC default" (the reloptions defaults are
/// `-1`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableSpaceOpts {
    /// `random_page_cost` (`float8`).
    pub random_page_cost: f64,
    /// `seq_page_cost` (`float8`).
    pub seq_page_cost: f64,
    /// `effective_io_concurrency` (`int`).
    pub effective_io_concurrency: i32,
    /// `maintenance_io_concurrency` (`int`).
    pub maintenance_io_concurrency: i32,
}
