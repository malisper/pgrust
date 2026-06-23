#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Shared PostgreSQL FFI vocabulary for hand-written pgrust crates.
//!
//! Keep this crate small. Add ABI types and wrappers only when a rewritten
//! crate needs them and the type is shared enough to avoid local duplication.

pub mod access;
pub mod acl;
pub mod aclchk;
pub mod adt;
pub mod algorithms;
pub mod array;
pub mod async_notify;
pub mod bitmapset;
pub mod brin;
pub mod cache;
pub mod cache_remainder;
pub mod catalog;
pub mod catalog_dependency;
pub mod catalog_relcreate;
pub mod commands;
pub mod commands_ddl_parsenodes;
pub mod commands_parsenodes;
pub mod commands_vacuum_stats;
pub mod control;
// `commands_vacuumparallel` is deliberately NOT glob-re-exported below: it
// carries `Relation` / `ParallelContext` / `TidStore` / `dsa_handle` /
// `dsa_pointer` aliases and a `VACUUM_OPTION_*` set that overlap other modules,
// so it is named explicitly as `pg_ffi_fgram::commands_vacuumparallel::*` to
// avoid the ambiguous-glob trap.
pub mod commands_vacuumparallel;
// Canonical `Datum` conversion helpers (`*GetDatum`/`DatumGet*`).  Deliberately
// NOT glob-re-exported at the crate root to avoid colliding with the legacy
// duplicate definitions scattered across dependent crates; consumers reach it
// via the explicit `pg_ffi_fgram::datum` path.
pub mod datum;
// `dbcommands_abi` is deliberately NOT glob-re-exported below (no `pub use
// dbcommands_abi::*;`), mirroring the `tcop`-style module convention: it carries
// `DatabaseRelationId` / `T_*` / `Anum_pg_database_*` names that overlap other
// modules, so it is named explicitly as `pg_ffi_fgram::dbcommands_abi::*` to
// avoid the ambiguous-glob trap.
pub mod dbcommands_abi;
// `extension_abi` is deliberately NOT glob-re-exported below (no `pub use
// extension_abi::*;`), mirroring the `tcop`/`dbcommands_abi` convention: it
// carries `ExtensionRelationId` / `T_*` / `Anum_pg_extension_*` /
// `Natts_pg_extension` names that overlap other modules, so it is named
// explicitly as `pg_ffi_fgram::extension_abi::*` to avoid the ambiguous-glob
// trap.
pub mod extension_abi;
// `tablecmds_abi` is likewise NOT glob-re-exported (no `pub use tablecmds_abi::*;`):
// it carries `AlterTableType` / `T_*` parse-node tags and statement structs that
// overlap other modules, so it is named explicitly as
// `pg_ffi_fgram::tablecmds_abi::*` to avoid the ambiguous-glob trap.
pub mod datetime;
pub mod dynahash;
pub mod tablecmds_abi;
// `foreign_publication_parsenodes` is deliberately NOT glob-re-exported (no
// `pub use foreign_publication_parsenodes::*;`): it carries `CreateStmt` /
// `T_*` parse-node tags and `RangeVar`/`RoleSpec`/`List`/`Node`-bearing structs
// that overlap other modules, so it is named explicitly as
// `pg_ffi_fgram::foreign_publication_parsenodes::*` to avoid the ambiguous-glob
// trap (mirrors `publication`/`tablecmds_abi`).
pub mod foreign_publication_parsenodes;
// `foreign_catalog` is deliberately NOT glob-re-exported: it carries catalog
// relation/index OIDs and `NameData`-bearing `FormData_*` structs that overlap
// other modules.  Named explicitly as `pg_ffi_fgram::foreign_catalog::*`.
pub mod encoding;
pub mod error;
pub mod event_trigger;
pub mod execexpr;
pub mod execnodes;
pub mod execparallel;
pub mod executor;
pub mod expandedrecord;
pub mod extensible;
pub mod fmgr;
pub mod foreign_catalog;
pub mod freepage;
pub mod fsm;
pub mod funcapi;
pub mod funccache;
pub mod geo;
pub mod gin;
pub mod gist;
pub mod guc;
pub mod hash;
pub mod heap;
pub mod heaptuple;
pub mod hstore;
pub mod init;
pub mod instrument;
pub mod jit;
pub mod jsonb;
pub mod jsonpath;
pub mod keywords;
pub mod large_object;
pub mod libpq;
pub mod list;
pub mod locale;
// `matview` (REFRESH MATERIALIZED VIEW file-local ABI: the private
// `DR_transientrel` DestReceiver subtype) is referenced by path
// (`pg_ffi_fgram::matview::DR_transientrel`); deliberately NOT in the
// crate-root glob to avoid ambiguous-glob collisions with the widely-named
// `DestReceiver` / `Relation` / `BulkInsertState` types it reuses.
pub mod matview;
pub mod memory;
pub mod nbtree;
pub mod net;
pub mod nodeagg_abi;
pub mod nodeforeigncustom_abi;
pub mod nodefunctionscan_abi;
pub mod nodegather_abi;
pub mod nodegathermerge_abi;
pub mod nodehash_abi;
pub mod nodehashjoin_abi;
pub mod nodeincrementalsort_abi;
pub mod nodeindexscan;
pub mod nodelockrows;
pub mod nodememoize_abi;
pub mod nodemergejoin_abi;
pub mod nodemodifytable_abi;
pub mod nodemodifytable_state;
pub mod nodenestloop_abi;
pub mod nodeprojectset_abi;
pub mod nodes;
pub mod nodesort_abi;
pub mod nodesubplan_abi;
pub mod nodetablefuncscan_abi;
pub mod nodetidscan_abi;
pub mod numeric;
pub mod object_access;
pub mod optimizer_plan_abi;
pub mod params;
pub mod parse;
pub mod partition;
pub mod pathnodes;
// `plannodes_gen` carries concrete `#[repr(C)]` plan-node structs
// (SeqScan/Agg/Append/...) for the optimizer's createplan/setrefs providers.
// Deliberately NOT glob-re-exported: its `Agg`/`Sort`-adjacent names would
// collide with the opaque `Agg = c_void` alias in `nodeagg_abi`. Reach the
// structs by module path (`plannodes_gen::Agg`).
pub mod plannodes_gen;
// `policy` (policy.c RLS-DDL ABI: `CreatePolicyStmt`/`AlterPolicyStmt`,
// `FormData_pg_policy`, the `pg_policy` relation/index OIDs, `Anum_pg_policy_*`,
// `Natts_pg_policy`, `T_CreatePolicyStmt`/`T_AlterPolicyStmt`) is referenced by
// path (`pg_ffi_fgram::policy::*`); deliberately NOT in the crate-root glob to
// avoid ambiguous-glob collisions (it re-uses widely-named items like
// `PolicyRelationId` / `Anum_*` / `Natts_*` / the `T_*` tags), mirroring the
// `tcop` / `dbcommands_abi` convention.
pub mod policy;
pub mod prep;
// `publication` (pg_publication* catalog ABI: `Publication`,
// `PublicationActions`, `PublicationRelInfo`, `FormData_pg_publication_rel`,
// `PublicationPartOpt`, `PublishGencolsType`) is referenced by path
// (`pg_ffi_fgram::publication::*`); deliberately NOT in the crate-root glob to
// avoid ambiguous-glob collisions (it re-uses widely-named items).
pub mod publication;

pub mod queryjumble;
pub mod rangetypes;
pub mod reloptions;
pub mod relscan;
pub mod resowner;
// `ruleutils` (SQL-deparser file-local ABI: `deparse_context` /
// `deparse_namespace` / `deparse_columns` / `NameHashEntry` / `rsv_callback`)
// is referenced by path (`pg_ffi_fgram::ruleutils::*`); deliberately NOT in
// the crate-root glob to avoid ambiguous-glob collisions (it re-uses the
// widely-named `List`/`Plan`/`Bitmapset`/`Node` types).
pub mod rmgrdesc;
pub mod ruleutils;
pub mod scankey;
// `selfuncs` (selectivity / index-cost estimator ABI: `VariableStatData`,
// `GenericCosts`, `EstimationInfo`) is referenced by path
// (`pg_ffi_fgram::selfuncs::*`); deliberately NOT in the crate-root glob to
// avoid ambiguous-glob collisions with the optimizer ABI modules.
pub mod selfuncs;
pub mod skipsupport;
// `smgr_md_abi` is deliberately NOT glob-re-exported below (no `pub use
// smgr_md_abi::*`), mirroring the `tcop`-style module convention: its
// md-specific types (`MdfdVec`/`BulkWriteState`/`f_smgr`) overlap in purpose
// with the boundary-only `SMgrRelationData` in `storage` and would otherwise
// produce ambiguous-glob collisions. Reach them via the module path,
// e.g. `pg_ffi_fgram::smgr_md_abi::MdfdVec`.
pub mod smgr_md_abi;
pub mod snapshot;
pub mod sortsupport;
// `spi` (SPI + SQL-language function executor ABI) is referenced by path
// (`pg_ffi_fgram::spi::*`); deliberately NOT in the crate-root glob to avoid
// ambiguous-glob collisions (`QueryDesc`, `_SPI_plan`, result-code consts).
pub mod spgist;
pub mod spgist_private;
pub mod spi;
pub mod statistics;
pub mod statscmds_abi;
pub mod storage;
pub mod stringinfo;
pub mod tcop;
pub mod tidbitmap;
pub mod toast;
pub mod transam_status;
pub mod tsearch;
pub mod tuplesort;
pub mod types;
pub mod wait_event;
pub mod wal;
pub mod wchar;
pub mod xact;
pub mod xml;

pub use access::*;
pub use acl::*;
pub use adt::*;
pub use algorithms::*;
pub use array::*;
pub use bitmapset::*;
pub use cache::*;
// The remaining `utils/cache` ABI (plancache/relmapper/ts_cache/evtcache/
// lsyscache result structs) is surfaced through its own glob; verified
// collision-free against `cache::*` and the rest of the crate root.
pub use cache_remainder::*;
pub use catalog::*;
pub use catalog_dependency::*;
pub use catalog_relcreate::*;
pub use commands::*;
pub use commands_ddl_parsenodes::*;
pub use commands_parsenodes::*;
pub use commands_vacuum_stats::*;
pub use control::*;
pub use datetime::*;
pub use dynahash::*;
pub use encoding::*;
pub use error::*;
pub use execexpr::*;
pub use execnodes::*;
pub use executor::*;
pub use expandedrecord::*;
pub use fmgr::*;
pub use freepage::*;
pub use fsm::*;
pub use funcapi::*;
pub use funccache::*;
pub use gist::*;
pub use guc::*;
pub use heap::*;
pub use heaptuple::*;
pub use hstore::*;
pub use init::*;
pub use instrument::*;
pub use jit::*;
pub use keywords::*;
pub use large_object::*;
pub use libpq::*;
pub use list::*;
pub use locale::*;
pub use memory::*;
pub use nbtree::*;
pub use net::*;
// `EPQState` (EvalPlanQual recheck state) is the canonical executor ABI struct
// defined in `nodeindexscan`; the modify/lock-rows node ABI modules reach it via
// `crate::EPQState`, so surface it at the crate root.
pub use nodeforeigncustom_abi::*;
pub use nodefunctionscan_abi::*;
pub use nodegather_abi::*;
pub use nodegathermerge_abi::*;
pub use nodehash_abi::*;
pub use nodehashjoin_abi::*;
pub use nodeindexscan::EPQState;
pub use nodememoize_abi::*;
pub use nodemergejoin_abi::*;
pub use nodenestloop_abi::*;
pub use nodes::*;
pub use nodesubplan_abi::*;
pub use nodetablefuncscan_abi::*;
pub use object_access::*;
// Selective re-export: `optimizer_plan_abi` also defines `ParamPtr`/`WindowClausePtr`
// (opaque) that collide by-name with the `pathnodes` aliases, so we re-export only
// the genuinely-new planner cost/plan types here and reach the opaque pointer
// typedefs via the module path (`optimizer_plan_abi::ParamPtr`, …).
pub use optimizer_plan_abi::{
    CostSelector, JoinCostWorkspace, JoinPathExtraData, PathKeysComparison, SemiAntiJoinFactors,
};
pub use parse::*;
pub use partition::*;
pub use pathnodes::*;
pub use prep::*;
pub use reloptions::*;
pub use relscan::*;
pub use resowner::*;
pub use rmgrdesc::*;
pub use scankey::*;
pub use skipsupport::*;
pub use snapshot::*;
pub use sortsupport::*;
pub use statistics::*;
pub use storage::*;
pub use stringinfo::*;
pub use toast::*;
pub use tsearch::*;
pub use tuplesort::*;
pub use types::*;
pub use wait_event::*;
pub use wal::*;
pub use wchar::*;
pub use xact::*;
pub use xml::*;
