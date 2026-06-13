#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the project-wide `PgResult` (== `Result<_,
// PgError>`); `PgError` is a large owned struct, so the un-boxed `Err` variant
// trips `clippy::result_large_err`. The un-boxed return is the project's error
// contract, so accept the lint crate-wide.
#![allow(clippy::result_large_err)]
// Some projected-row helpers and imports are exercised only on certain code
// paths; accept the lint crate-wide rather than gate each one.
#![allow(dead_code)]

//! `backend-utils-cache-lsyscache` — port of
//! `src/backend/utils/cache/lsyscache.c` (`src/include/utils/lsyscache.h`).
//!
//! `lsyscache.c` is the "convenience catalog lookups" layer: a large flat set
//! of `get_*` helpers that wrap `SearchSysCache` / `GetSysCacheOid` /
//! `GetSysCacheHashValue` probes over the system caches and project out one or
//! a few fields of a catalog row. With a fan-in of 24, almost every consumer
//! reaches it through the `backend-utils-cache-lsyscache-seams` crate (owned
//! here), so this unit *installs* every one of those seams.
//!
//! The lookups themselves bottom out in the syscache / catcache layer. Until
//! `syscache` lands, the `SearchSysCache*` / `GetSysCacheOid*` calls route
//! through that owner's per-owner seam (loud panic until it lands); this unit's
//! own `get_*` logic is ported in full. `init_seams()` wires every owned seam
//! to its family adapter.
//!
//! Decomposition-track family modules (one group of `get_*` helpers each):
//!
//!  * [`opfamily_operator`] — `pg_operator` / `pg_amop` operator metadata and
//!    opfamily membership (`get_commutator`, `op_input_types`, `op_strict`,
//!    `get_opcode`, `get_op_opfamily_properties`, `get_ordering_op_properties`,
//!    `get_op_hash_functions`, `get_opfamily_member`).
//!  * [`opclass`] — `pg_opclass` / `pg_opfamily` / `pg_amproc`
//!    (`get_opclass_input_type`, `get_opclass_family`, `get_opfamily_method`,
//!    `get_opfamily_proc`, `get_opfamily_name`, `get_default_opclass`).
//!  * [`attribute`] — `pg_attribute` (`get_attname`, `get_attnum`).
//!  * [`collation_constraint_language_cast`] — `pg_collation` / `pg_constraint`
//!    / `pg_language` / `pg_cast` helpers (no seam decls yet).
//!  * [`function`] — `pg_proc` (`get_func_rettype`, `get_func_signature`).
//!  * [`relation`] — `pg_class` / `pg_index` (`get_rel_name`,
//!    `get_rel_relkind`, `get_rel_relispartition`, `get_rel_namespace`,
//!    `get_relname_relid`, `get_index_isclustered`).
//!  * [`type_`] — `pg_type` / `pg_range` and the type-I/O helpers
//!    (`get_typlenbyvalalign`, `get_type_io_data`, `get_type_output_info`,
//!    `get_type_input_info`, `get_type_binary_output_info`, `get_base_type`,
//!    `get_base_type_and_typmod`, `get_base_element_type`, `get_element_type`,
//!    `get_array_type`, `get_array_element_io_data`, `get_multirange_range`,
//!    `lookup_pg_range`, `lookup_pg_type`, `syscache_hash_value_typeoid`).
//!  * [`statistics`] — `pg_statistic` (`get_attstatsslot`,
//!    `get_attstatsslot_mcv`).
//!  * [`namespace_range_index_pubsub`] — `pg_namespace` / `pg_am`
//!    (`get_namespace_name`, `get_namespace_name_or_temp`, `get_am_name`).

pub mod attribute;
pub mod collation_constraint_language_cast;
pub mod function;
pub mod namespace_range_index_pubsub;
pub mod opclass;
pub mod opfamily_operator;
pub mod relation;
pub mod statistics;
pub mod type_;

/// Install every seam this unit owns.
///
/// The unit owns the single seam crate `backend-utils-cache-lsyscache-seams`.
/// Every declaration in it is installed here, exactly once, as a thin
/// marshal+delegate over the family-module logic.
pub fn init_seams() {
    use backend_utils_cache_lsyscache_seams as seams;

    // -- opfamily_operator --------------------------------------------------
    seams::get_commutator::set(opfamily_operator::get_commutator);
    seams::op_input_types::set(opfamily_operator::op_input_types);
    seams::op_strict::set(opfamily_operator::op_strict);
    seams::get_opcode::set(opfamily_operator::get_opcode);
    seams::get_op_opfamily_properties::set(opfamily_operator::get_op_opfamily_properties);
    seams::get_ordering_op_properties::set(opfamily_operator::get_ordering_op_properties);
    seams::get_op_hash_functions::set(opfamily_operator::get_op_hash_functions);
    seams::get_opfamily_member::set(opfamily_operator::get_opfamily_member);

    // -- opclass ------------------------------------------------------------
    seams::get_opclass_input_type::set(opclass::get_opclass_input_type);
    seams::get_opclass_family::set(opclass::get_opclass_family);
    seams::get_opfamily_method::set(opclass::get_opfamily_method);
    seams::get_opfamily_proc::set(opclass::get_opfamily_proc);
    seams::get_opfamily_name::set(opclass::get_opfamily_name);
    seams::get_default_opclass::set(opclass::get_default_opclass);

    // -- attribute ----------------------------------------------------------
    seams::get_attname::set(attribute::get_attname);
    seams::get_attnum::set(attribute::get_attnum);

    // -- function -----------------------------------------------------------
    seams::get_func_rettype::set(function::get_func_rettype);
    seams::get_func_signature::set(function::get_func_signature);

    // -- relation -----------------------------------------------------------
    seams::get_rel_name::set(relation::get_rel_name);
    seams::get_rel_relkind::set(relation::get_rel_relkind);
    seams::get_rel_relispartition::set(relation::get_rel_relispartition);
    seams::get_rel_namespace::set(relation::get_rel_namespace);
    seams::get_relname_relid::set(relation::get_relname_relid);
    seams::get_index_isclustered::set(relation::get_index_isclustered);

    // -- type ---------------------------------------------------------------
    seams::get_typlenbyvalalign::set(type_::get_typlenbyvalalign);
    seams::get_type_io_data::set(type_::get_type_io_data);
    seams::get_type_output_info::set(type_::get_type_output_info);
    seams::get_type_input_info::set(type_::get_type_input_info);
    seams::get_type_binary_output_info::set(type_::get_type_binary_output_info);
    seams::get_base_type::set(type_::get_base_type);
    seams::get_base_type_and_typmod::set(type_::get_base_type_and_typmod);
    seams::get_base_element_type::set(type_::get_base_element_type);
    seams::get_element_type::set(type_::get_element_type);
    seams::get_array_type::set(type_::get_array_type);
    seams::get_array_element_io_data::set(type_::get_array_element_io_data);
    seams::get_multirange_range::set(type_::get_multirange_range);
    seams::lookup_pg_range::set(type_::lookup_pg_range);
    seams::lookup_pg_type::set(type_::lookup_pg_type);
    seams::syscache_hash_value_typeoid::set(type_::syscache_hash_value_typeoid);

    // -- statistics ---------------------------------------------------------
    seams::get_attstatsslot::set(statistics::get_attstatsslot);
    seams::get_attstatsslot_mcv::set(statistics::get_attstatsslot_mcv);

    // -- namespace_range_index_pubsub ---------------------------------------
    seams::get_namespace_name::set(namespace_range_index_pubsub::get_namespace_name);
    seams::get_namespace_name_or_temp::set(namespace_range_index_pubsub::get_namespace_name_or_temp);
    seams::get_am_name::set(namespace_range_index_pubsub::get_am_name);
}
