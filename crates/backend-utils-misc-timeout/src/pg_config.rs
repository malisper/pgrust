//! Port of `src/backend/utils/misc/pg_config.c` — the `pg_config()`
//! set-returning function that exposes the same `name`/`setting` pairs as the
//! `pg_config` program.
//!
//! The C function is a thin SRF shell: `InitMaterializedSRF`, then
//! `get_configdata(my_exec_path, &len)` (`common/config_info.c`), and
//! `tuplestore_putvalues` each `(name, setting)` row before returning
//! `(Datum) 0`. The only computation it owns is iterating the config rows; both
//! `get_configdata` and the tuplestore/`CStringGetTextDatum` materialization are
//! external subsystems. `get_config_rows` returns the owned rows so an executor
//! SRF wrapper can materialize them. `get_configdata` (config_info.c is not yet
//! ported) is reached through the `common-config-info-seams::get_configdata`
//! seam.

use backend_utils_error::PgResult;
use common_config_info_seams::get_configdata;
use types_misc_more2::ConfigDataRow;

/// `pg_config` — produce the configuration rows that the SRF materializes.
///
/// The C function discards its return value (`return (Datum) 0`) after pushing
/// every row into the result tuplestore; the meaningful output is the row set,
/// returned directly for the executor wrapper to put into its tuplestore.
/// `get_configdata` does not `ereport`, so the `PgResult` shape is preserved by
/// wrapping `Ok`.
pub fn get_config_rows() -> PgResult<Vec<ConfigDataRow>> {
    Ok(get_configdata::call())
}
