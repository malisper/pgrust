//! Seam declaration for the owner `src/common/config_info.c`.
//!
//! `pg_config()` (`utils/misc/pg_config.c`) calls
//! `get_configdata(my_exec_path, &len)` to obtain the same `name`/`setting`
//! rows the `pg_config` program prints. The owner is not yet ported; calls
//! panic until it lands.

seam_core::seam!(
    /// `get_configdata(my_exec_path, &configdata_len)` (`common/config_info.c`):
    /// return the configuration rows for the installation hosting
    /// `my_exec_path`. Does not `ereport`; infallible in C.
    pub fn get_configdata() -> Vec<types_misc_more2::ConfigDataRow>
);
