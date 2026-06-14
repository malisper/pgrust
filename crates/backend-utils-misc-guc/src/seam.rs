//! Crate-local re-exports of the genuinely-external seams `guc.c`'s core calls.
//!
//! The GUC *core* is ported in-crate; a handful of predicates and helpers that
//! `guc.c` invokes from `set_config_with_handle()` (the access-restriction
//! switch) and the `GUC_IS_NAME` truncation branch live in *other* subsystems
//! and are reached through those owners' per-crate seam crates. Re-exported here
//! so the in-crate `crate::seam::<name>::{call}` sites resolve to one place.
//!
//! These crates own + install these seams; this unit only *calls* them.

/// `IsUnderPostmaster` (`miscinit.c`).
pub use backend_utils_init_small_seams::is_under_postmaster;

/// `IsInParallelMode()` (`access/xact.c`).
pub use backend_access_transam_xact_seams::is_in_parallel_mode;

/// `InLocalUserIdChange()` (`utils/init/miscinit.c`).
pub use backend_utils_init_miscinit_seams::in_local_user_id_change;

/// `InSecurityRestrictedOperation()` (`utils/init/miscinit.c`).
pub use backend_utils_init_miscinit_seams::in_security_restricted_operation;

/// `pg_parameter_aclcheck(name, role, ACL_SET)` (`catalog/aclchk.c`).
pub use backend_catalog_aclchk_seams::pg_parameter_aclcheck;

/// `truncate_identifier(str, strlen(str), true)` (`parser/scansup.c`).
pub use backend_parser_scansup_seams::truncate_identifier;

/// `pq_putmessage` (`libpq/pqcomm.c`) — the byte sink `ReportGUCOption` writes
/// its `ParameterStatus` ('S') frame through.
pub use backend_libpq_pqcomm_seams::pq_putmessage;
