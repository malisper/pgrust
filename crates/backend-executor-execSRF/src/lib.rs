//! `executor/execSRF.c` — the executor-frame API for set-returning functions.
//!
//! This unit serves `nodeFunctionscan.c` and `nodeProjectSet.c`, providing the
//! common code for calling set-returning functions through the `ReturnSetInfo`
//! API. It is the #349 K2 keystone: the executor builds its OWN
//! [`types_nodes::fmgr::FunctionCallInfoBaseData`] with a LIVE
//! `fcinfo.resultinfo = ReturnSetInfo` (+ `fn_extra` / `fn_mcxt` channels) and
//! dispatches the SRF's `PGFunction` through it, letting the callee read
//! `econtext`/`expectedDesc` and write `isDone`/`returnMode`/`setResult`/`setDesc`
//! each iteration (the ValuePerCall loop + Materialize mode).
//!
//! ## The executor-frame SRF dispatch (the dual-home boundary)
//!
//! `FunctionCallInvoke(fcinfo)` in C is `fcinfo->flinfo->fn_addr(fcinfo)`: the
//! same `PGFunction` callable receives ordinary calls AND set-returning calls
//! (the `resultinfo` is just a field on the frame). The owned model has two
//! `FunctionCallInfoBaseData` homes (WONTFIX, DESIGN_DEBT): the by-OID builtin
//! registry (`backend_utils_fmgr_core`) holds `types_fmgr::PGFunction`s whose
//! frame's `resultinfo` is tag-only, so an SRF dispatched through it can never
//! see the LIVE `ReturnSetInfo`. The live `ReturnSetInfo` lives on the
//! `types_nodes` frame.
//!
//! So this unit keeps a small executor-frame SRF table keyed by OID, holding
//! [`types_nodes::execexpr::PGFunction`]s (the `for<'mcx> fn(&mut
//! FunctionCallInfoBaseData<'mcx>) -> Datum<'mcx>` whose frame DOES carry the
//! live `ReturnSetInfo`). This is the faithful `FunctionCallInvoke`-with-
//! `resultinfo` over the executor frame — it mirrors `fmgr_builtins[]` for the
//! executor-frame ABI, exactly as the C `fn_addr` is the same callable for both
//! call shapes. SRFs register their executor-frame core here (e.g.
//! `generate_series_int4`, OID 1066/1067/1068, registered by
//! `backend-utils-adt-int`'s `init_seams`).

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use backend_utils_error::ereport;
use mcx::{Mcx, MemoryContext, PgBox};
use types_core::fmgr::FmgrInfo;
use types_core::Oid;
use types_datum::NullableDatum;
use types_error::error::{
    ERRCODE_DATATYPE_MISMATCH, ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INTERNAL_ERROR, ERRCODE_TOO_MANY_ARGUMENTS,
};
use types_error::{PgResult, ERROR};
use types_nodes::execexpr::{ExprDoneCond, SetExprState};
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_nodes::funcapi::{
    ReturnSetInfo, SetFunctionReturnMode, Tuplestorestate, SFRM_Materialize,
    SFRM_Materialize_Preferred, SFRM_Materialize_Random, SFRM_ValuePerCall,
};
use types_nodes::primnodes::Expr;
use types_nodes::{EcxtId, EStateData, PlanStateData, SlotId, TupleSlotKind};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::TupleDescData;

use backend_executor_execSRF_seams as seams;

mod generate_series;
mod generate_series_numeric;
mod generate_subscripts;
mod regexp_matches;
mod string_to_table;
mod json_each;
mod json_record;
mod json_srf;
mod jsonb_srf;
mod recordset_srf;
mod pg_input_error_info;
mod regexp_split;
mod srf_registry;
mod unnest;
mod control_srf;
mod multirange_unnest;
mod tsvector_unnest;
mod pg_get_keywords;
mod pg_tablespace_databases;
mod pg_listening_channels;
mod pg_get_multixact_members;
mod pg_get_catalog_foreign_keys;
mod pg_partition_tree;
mod pg_cursor;
mod pg_event_trigger_dropped_objects;
mod pg_event_trigger_ddl_commands;
mod pg_get_publication_tables;
mod pg_lock_status;
mod pg_prepared_xact;
mod pg_snapshot_xip;
mod aclexplode;
mod pg_stat_get_io;
mod pg_stat_get_slru;
mod pgstat_composite_srf;
mod pg_mcv_list_items;
mod shmem_numa_srf;
mod system_srf;
pub use srf_registry::{register_srf, srf_invoke_by_oid, srf_is_registered};
pub use json_record::{invoke_scalar_record_function, is_scalar_record_function};

#[cfg(test)]
mod tests;

/// Install this unit's seams. Idempotent in spirit (the seam registry tolerates
/// re-set in tests via the framework). Called from `seams-init`.
pub fn init_seams() {
    seams::exec_init_table_function_result::set(ExecInitTableFunctionResult);
    seams::exec_make_table_function_result::set(ExecMakeTableFunctionResult);
    seams::exec_init_function_result_set::set(ExecInitFunctionResultSet);
    seams::exec_make_function_result_set::set(ExecMakeFunctionResultSet);
    seams::restart_set_expr_state::set(RestartSetExprState);
    seams::is_scalar_record_function::set(json_record::is_scalar_record_function);
    seams::invoke_scalar_record_function::set(json_record::invoke_scalar_record_function);
    // The executor-frame `fmgrtab` analogue for the int4/int8 generate_series
    // SRFs (the by-OID builtin registry's tag-only resultinfo can't carry the
    // live ReturnSetInfo — WONTFIX dual-home).
    generate_series::register_generate_series();
    // `pg_input_error_info(text, text) RETURNS record` (OID 6211) — a
    // single-row composite record function reached via nodeFunctionscan.
    pg_input_error_info::register_pg_input_error_info();
    // `unnest(anyarray)` (OID 2331) — the value-per-call SRF emitting each
    // array element, registered in the executor-frame table (its element
    // deconstruction core is `backend-utils-adt-arrayfuncs::array_unnest`).
    unnest::register_unnest();
    // `generate_subscripts(anyarray, int4 [, bool])` (OIDs 1191/1192) — the
    // value-per-call SRF emitting a dimension's subscript range (its bound core
    // is `backend-utils-adt-arrayfuncs::sql::generate_subscripts`).
    generate_subscripts::register_generate_subscripts();
    // `regexp_split_to_table(text, text [, text])` (OIDs 2765/2766) — the
    // value-per-call SRF emitting the split-out substrings (its glob match +
    // split core is `backend-utils-adt-regexp::regexp_split_to_table`).
    regexp_split::register_regexp_split();
    // `generate_series(numeric, numeric [, numeric])` (OIDs 3259/3260) — the
    // value-per-call numeric SRF (its NaN/zero-step validation + per-call
    // cmp_var/add_var advance core is `backend-utils-adt-numeric::series_srf`,
    // over the numeric kernels).
    generate_series_numeric::register_generate_series_numeric();
    // `regexp_matches(text, text [, text])` (OIDs 2763/2764) — the value-per-call
    // SRF emitting one `text[]` row per match (its glob scan + per-row build core
    // is `backend-utils-adt-regexp::regexp_matches`).
    regexp_matches::register_regexp_matches();
    // `string_to_table(text, text [, text])` (OIDs 6160/6161) — the value-per-call
    // SRF emitting one `text` field per call (its split core is
    // `backend-utils-adt-varlena::split_format::split_text`).
    string_to_table::register_string_to_table();
    // `json_array_elements`/`json_array_elements_text`/`json_object_keys` (OIDs
    // 3955/3969/3957) — the value-per-call json (text) SRFs (their SAX-callback
    // collection cores are `backend-utils-adt-jsonfuncs::{elements,keys}`).
    json_srf::register_json_srfs();
    // `json_each`/`json_each_text`/`jsonb_each`/`jsonb_each_text` (OIDs
    // 3958/3959/3208/3932) — the materialize-mode json/jsonb (key,value) SRFs
    // (their `each_worker`/`each_worker_jsonb` bodies fill the materialize
    // tuplestore via InitMaterializedSRF; core is
    // `backend-utils-adt-jsonfuncs::each`).
    json_each::register_json_each_srfs();
    // `jsonb_array_elements`/`jsonb_array_elements_text`/`jsonb_object_keys`
    // (OIDs 3219/3465/3931) — the materialize-mode jsonb array-elements /
    // object-keys SRFs (their `elements_worker_jsonb` / `jsonb_object_keys`
    // bodies fill the materialize tuplestore via InitMaterializedSRF; core is
    // `backend-utils-adt-jsonfuncs::{elements,keys}`).
    jsonb_srf::register_jsonb_srfs();
    // `json_to_recordset`/`jsonb_to_recordset` (OIDs 3205/3491) — the
    // materialize-mode json/jsonb array-of-objects -> setof record SRFs (their
    // `populate_recordset_worker` body fills the materialize tuplestore via
    // InitMaterializedSRF; core is `backend-utils-adt-jsonfuncs::recordset`).
    recordset_srf::register_recordset_srfs();
    // The REST of the composite-record family that `recordset_srf` does not own:
    // `json[b]_to_record` (one composite row, OIDs 3204/3490) and the
    // `json[b]_populate_record[set]` (+ `jsonb_populate_record_valid`) seed-record
    // family (OIDs 3960/3209/6338/3961/3475), which read an optional composite
    // `record` argument through the now-installed funcapi `srf_arg_record` seam.
    // Their bodies are `backend-utils-adt-jsonfuncs::{populate,recordset}`.
    json_record::register_json_record_srfs();
    // `unnest(anymultirange)` (OID 1293) — the value-per-call SRF emitting each
    // member range (its `multirange_get_typcache`->rngtype + `multirange_get_range`
    // serialization core is
    // `backend-utils-adt-multirangetypes::operators::multirange_unnest_images`).
    multirange_unnest::register_multirange_unnest();
    // `tsvector_unnest(tsvector)` (OID 3322) — the materialize-mode SRF behind
    // `unnest(tsvector)`, emitting one `(lexeme text, positions int2[], weights
    // "char"[])` row per WordEntry (its decode core is
    // `backend-utils-adt-tsvector-core::op::tsvector_unnest`).
    tsvector_unnest::register_tsvector_unnest();
    // `pg_get_keywords()` (OID 1686) — the materialize-mode SRF emitting one
    // `(word text, catcode "char", barelabel bool, catdesc text, baredesc text)`
    // row per grammar keyword (its render core is
    // `backend-utils-adt-misc::pg_get_keywords`).
    pg_get_keywords::register_pg_get_keywords();
    // `pg_tablespace_databases(oid)` (OID 2556) — the materialize-mode SRF
    // emitting one `oid` per database directory under a tablespace (its
    // directory-scan core is `backend-utils-adt-misc::pg_tablespace_databases`).
    pg_tablespace_databases::register_pg_tablespace_databases();
    // `pg_listening_channels()` (OID 3035) — the materialize-mode SRF emitting one
    // `text` per LISTENed channel (its collector core is
    // `backend-commands-async::pg_listening_channels_rows`).
    pg_listening_channels::register_pg_listening_channels();
    // `pg_get_multixact_members(xid)` (OID 3819) — the materialize-mode SRF
    // emitting one `(xid, mode text)` per MultiXact member (its resolver core is
    // `backend-access-transam-multixact::pg_get_multixact_members`).
    pg_get_multixact_members::register_pg_get_multixact_members();
    // `pg_get_catalog_foreign_keys()` (OID 6159) — the materialize-mode SRF
    // emitting one `(fktable regclass, fkcols text[], pktable regclass, pkcols
    // text[], is_array bool, is_opt bool)` per `sys_fk_relationships[]` entry
    // (its render core is
    // `backend-utils-adt-misc::pg_get_catalog_foreign_keys`; the text[] columns
    // are built with `construct_text_array`).
    pg_get_catalog_foreign_keys::register_pg_get_catalog_foreign_keys();
    // `pg_partition_tree(regclass)` (OID 3423) and `pg_partition_ancestors(regclass)`
    // (OID 3425) — the materialize-mode partition-hierarchy SRFs (their traversal
    // cores are `backend-catalog-pg-inherits::find_all_inheritors` and
    // `backend-catalog-partition::get_partition_ancestors`; the per-row
    // parent/isleaf/level computation is the partitionfuncs.c inner block).
    pg_partition_tree::register_pg_partition_tree();
    pg_partition_tree::register_pg_partition_ancestors();
    // `pg_prepared_xact()` (OID 1065) — the materialize-mode SRF emitting one
    // `(transaction xid, gid text, prepared timestamptz, ownerid oid, dbid oid)`
    // per valid prepared transaction (its locked snapshot-and-project core is
    // `backend-access-transam-twophase::pg_prepared_xact_rows` over the live
    // `TwoPhaseState` via `with_twophase_state`).
    pg_prepared_xact::register_pg_prepared_xact();

    // `pg_mcv_list_items(pg_mcv_list)` (OID 3427, prosrc
    // `pg_stats_ext_mcvlist_items`) — the materialize-mode SRF deconstructing a
    // serialized MCV list into one `(index int4, values text[], nulls bool[],
    // frequency float8, base_frequency float8)` row per item (its deserialize
    // core is `backend-statistics-mcv::statext_mcv_deserialize`).
    pg_mcv_list_items::register_pg_mcv_list_items();
    // `pg_lock_status` (OID 1371) — the `pg_locks` view's underlying SRF.
    pg_lock_status::register_pg_lock_status();
    // `pg_event_trigger_dropped_objects` (OID 3566) — the `sql_drop`
    // event-trigger SRF listing the command's dropped objects.
    pg_event_trigger_dropped_objects::register_pg_event_trigger_dropped_objects();
    // `pg_event_trigger_ddl_commands` (OID 4568) — the `ddl_command_end`
    // event-trigger SRF listing the DDL commands the firing command ran.
    pg_event_trigger_ddl_commands::register_pg_event_trigger_ddl_commands();
    // `pg_cursor()` (OID 2511) — the `pg_cursors` view's underlying SRF, listing
    // every open cursor (portal) of the current session.
    pg_cursor::register_pg_cursor();
    // `pg_get_publication_tables(VARIADIC text[])` (OID 6119) — the published
    // tables (column lists + row filters) of one or more publications.
    pg_get_publication_tables::register_pg_get_publication_tables();
    // `pg_snapshot_xip(pg_snapshot)` (OID 5064) — the value-per-call SRF emitting
    // the snapshot's in-progress `xip[]` as `setof xid8` (its value sequence is
    // `backend-utils-adt-xid8funcs::pg_snapshot_xip`).
    pg_snapshot_xip::register_pg_snapshot_xip();
    // `aclexplode(aclitem[])` (OID 1689) — the materialize-mode SRF expanding an
    // acl array into one `(grantor oid, grantee oid, privilege_type text,
    // is_grantable bool)` row per set privilege bit (its per-bit expansion core
    // is `backend-utils-adt-acl::acl_ops::aclexplode`).
    aclexplode::register_aclexplode();
    // The pgstatfuncs.c cumulative-statistics SRF / composite-row family:
    // `pg_stat_get_io` (OID 6214, the `pg_stat_io` view) materializes the 20-col
    // per-(BackendType,IOObject,IOContext) IO snapshot; `pg_stat_get_slru` (OID
    // 2306, `pg_stat_slru`) the 9-col per-SLRU rows; and the single-composite-row
    // `pg_stat_get_wal`/`pg_stat_get_archiver`/`pg_stat_get_replication_slot`/
    // `pg_stat_get_subscription_stats` (OIDs 1136/3195/6169/6231) build their own
    // descriptor + `heap_form_tuple`. All read the now-ported pgstat fetch
    // substrate (pgstat_fetch_stat_io/slru/wal/archiver/replslot/subscription).
    pg_stat_get_io::register_pg_stat_get_io();
    pg_stat_get_slru::register_pg_stat_get_slru();
    pgstat_composite_srf::register_pgstat_composite_srfs();
    // `pg_options_to_table(text[])` (OID 2289) and `pg_prepared_statement()` (OID
    // 2510) — the materialize-mode system SRFs whose `(mcx, fcinfo)` bodies drive
    // `InitMaterializedSRF`/`materialized_srf_putvalues` themselves (cores in
    // `backend-foreign-foreign` / `backend-commands-prepare`).
    system_srf::register_system_srfs();
    // `pg_control_system/checkpoint/recovery/init` (OIDs 3441-3444) and
    // `pg_stat_file[_1arg]` (OIDs 3307/2623) — the single-composite-row system
    // builtins reached via the FROM clause (function RTE), dispatched through the
    // executor-frame SRF table like the `json_to_record` family (cores in
    // `backend-utils-misc-more` / `backend-utils-adt-misc2`).
    control_srf::register_control_srfs();
    shmem_numa_srf::register_shmem_numa_srf();
}

// ===========================================================================
//  init_sexpr — initialize a SetExprState node during first use (execSRF.c:695)
// ===========================================================================

/// `init_sexpr(foid, input_collation, node, sexpr, parent, sexprCxt, allowSRF,
/// needDescForSRF)` (execSRF.c:695).
///
/// The faithful C does the `object_aclcheck` / `InvokeFunctionExecuteHook` /
/// `FUNC_MAX_ARGS` guard, `fmgr_info_cxt` + `fmgr_info_set_expr`, builds the
/// `fcinfo`, and (for a `fn_retset` function with `needDescForSRF`) prepares the
/// expected `funcResultDesc` via `get_expr_result_type`.
///
/// In the owned model the function is dispatched through the executor-frame SRF
/// table (`srf_registry`), so the FmgrInfo carries the OID + the resolved
/// `proisstrict`/`proretset` flags (read by lsyscache). The fcinfo is built
/// sized for the args. The `funcResultDesc` precomputation belongs to the
/// targetlist (ProjectSet) path and is computed there; the table-function path
/// (`ExecMakeTableFunctionResult`) builds its descriptor lazily from the
/// expected/returned type, so `needDescForSRF` is `false` for it.
fn init_sexpr<'mcx>(
    foid: Oid,
    input_collation: Oid,
    sexpr: &mut SetExprState<'mcx>,
    allow_srf: bool,
    need_desc_for_srf: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // C: aclresult = object_aclcheck(ProcedureRelationId, foid, GetUserId(),
    //                                ACL_EXECUTE); ...; InvokeFunctionExecuteHook(foid);
    // (Execute-permission check + hook — not modeled at this layer; the planner
    // already resolved the call. Faithful to the no-op when ACL is open.)

    let numargs = sexpr.args.as_ref().map(|a| a.len()).unwrap_or(0);

    // C: if (list_length(sexpr->args) > FUNC_MAX_ARGS) ereport(...);
    // FUNC_MAX_ARGS = 100. A planner-checked call never exceeds it; surface
    // loudly if it does.
    const FUNC_MAX_ARGS: usize = 100;
    if numargs > FUNC_MAX_ARGS {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg("cannot pass more than 100 arguments to a function")
            .into_error());
    }

    // C: fmgr_info_cxt(foid, &(sexpr->func), sexprCxt);
    //    fmgr_info_set_expr((Node *) sexpr->expr, &(sexpr->func));
    // The owned FmgrInfo carries the OID and resolved flags; the executor-frame
    // SRF table is the `fn_addr` re-resolution at dispatch.
    let fn_retset = backend_utils_cache_lsyscache_seams::get_func_retset::call(foid)?;
    let fn_strict = backend_utils_cache_lsyscache_seams::func_strict::call(foid)?;
    sexpr.func = FmgrInfo {
        fn_addr: 0,
        fn_oid: foid,
        fn_nargs: numargs as i16,
        fn_strict,
        fn_retset,
        fn_stats: 0,
        fn_expr: None,
    };

    // C: fmgr_info_set_expr((Node *) sexpr->expr, &(sexpr->func));
    // Stamp the call-expression node onto the resolved FmgrInfo so the later
    // `get_fn_expr_*` readers (e.g. jsonb_populate_record's
    // get_record_type_from_argument) can recover the declared argument type.
    // Without this, `get_fn_expr_argtype` returns InvalidOid and downstream
    // type lookups raise "cache lookup failed for type 0".
    if let Some(expr) = sexpr.expr.as_deref() {
        backend_utils_fmgr_fmgr_seams::fmgr_info_set_expr::call(
            estate.es_query_cxt,
            &mut sexpr.func,
            expr,
        )?;
    }

    // C: sexpr->fcinfo = palloc(SizeForFunctionCallInfo(numargs));
    //    InitFunctionCallInfoData(*sexpr->fcinfo, &(sexpr->func), numargs,
    //                             input_collation, NULL, NULL);
    let mut args = Vec::with_capacity(numargs);
    args.resize(numargs, NullableDatum::default());
    let fcinfo = FunctionCallInfoBaseData {
        flinfo: Some(sexpr.func.clone()),
        context: None,
        resultinfo: None,
        fncollation: input_collation,
        isnull: false,
        nargs: numargs as i16,
        args,
        ref_args: Vec::new(),
        fn_extra: None,
        fn_mcxt: None,
    };
    sexpr.fcinfo = Some(mcx::alloc_in(estate.es_query_cxt, fcinfo)?);

    // C: if (sexpr->func.fn_retset && !allowSRF) ereport(ERROR, "set-valued
    //    function called in context that cannot accept a set");
    if sexpr.func.fn_retset && !allow_srf {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("set-valued function called in context that cannot accept a set")
            .into_error());
    }

    // C: Assert(sexpr->func.fn_retset == sexpr->funcReturnsSet);
    // (the caller set funcReturnsSet; keep them in sync for the ProjectSet path.)

    // C: funcResultStore = NULL; funcResultSlot = NULL; shutdown_reg = false;
    //    funcResultDesc = NULL; funcReturnsTuple = false; setArgsValid = false;
    sexpr.funcResultStore = None;
    sexpr.funcResultSlot = None;
    sexpr.shutdown_reg = false;
    sexpr.funcResultDesc = None;
    sexpr.funcReturnsTuple = false;
    sexpr.setArgsValid = false;

    // C (execSRF.c init_sexpr):
    //   /* If function returns set, prepare a resultinfo node for communication */
    //   if (sexpr->func.fn_retset && needDescForSRF)
    //   {
    //       TypeFuncClass functypclass;
    //       Oid          funcrettype;
    //       TupleDesc    tupdesc;
    //       functypclass = get_expr_result_type(sexpr->expr, &funcrettype, &tupdesc);
    //       if (functypclass == TYPEFUNC_COMPOSITE ||
    //           functypclass == TYPEFUNC_COMPOSITE_DOMAIN)
    //       {
    //           sexpr->funcReturnsTuple = true;
    //           sexpr->funcResultDesc = CreateTupleDescCopy(tupdesc);
    //       }
    //       else if (functypclass == TYPEFUNC_SCALAR)
    //       {
    //           sexpr->funcReturnsTuple = false;
    //           tupdesc = CreateTemplateTupleDesc(1);
    //           TupleDescInitEntry(tupdesc, 1, NULL, funcrettype, -1, 0);
    //           TupleDescInitEntryCollation(tupdesc, 1, exprCollation((Node *) sexpr->expr));
    //           sexpr->funcResultDesc = tupdesc;
    //       }
    //       else if (functypclass == TYPEFUNC_RECORD)
    //       {
    //           /* leave funcResultDesc = NULL; nodeFunctionscan will set up */
    //       }
    //       else
    //       {
    //           /* crummy error message, but parser should have caught this */
    //           elog(ERROR, "function in FROM has unsupported return type");
    //       }
    //   }
    //
    // This precomputes `funcResultDesc` (the `expectedDesc` the materialize-mode
    // SRF protocol reads through `MAT_SRF_USE_EXPECTED_DESC`). Without it, a
    // SCALAR SETOF function called in a targetlist (ProjectSet path, e.g.
    // `SELECT jsonb_array_elements(...)`) reaches `InitMaterializedSRF` with a
    // NULL `expectedDesc` and errors "materialize mode required, but it is not
    // allowed in this context".
    if sexpr.func.fn_retset && need_desc_for_srf {
        let per_query = estate.es_query_cxt;
        // C: get_expr_result_type((Node *) sexpr->expr, &funcrettype, &tupdesc).
        // The owned `sexpr->expr` is an `Expr`; wrap it in a `Node` for the
        // funcapi classifier (which dispatches on the node tag).
        let expr_node = {
            let e = sexpr
                .expr
                .as_deref()
                .expect("init_sexpr: sexpr->expr set by the caller")
                .clone_in(per_query)?;
            mcx::alloc_in(per_query, types_nodes::nodes::Node::mk_expr(per_query, e)?)?
        };
        let resolved = backend_utils_fmgr_funcapi::result_type::get_expr_result_type(
            per_query,
            Some(&expr_node),
        )?;
        match resolved.class {
            Some(types_nodes::funcapi::TypeFuncClass::Composite)
            | Some(types_nodes::funcapi::TypeFuncClass::CompositeDomain) => {
                // Composite data type, e.g. a table's row type.
                sexpr.funcReturnsTuple = true;
                let src = resolved
                    .result_tuple_desc
                    .as_deref()
                    .expect("get_expr_result_type: COMPOSITE class with NULL tupdesc");
                let copy = backend_access_common_tupdesc::CreateTupleDescCopy(per_query, src)?;
                sexpr.funcResultDesc = Some(mcx::alloc_in(per_query, copy)?);
            }
            Some(types_nodes::funcapi::TypeFuncClass::Scalar) => {
                // Base data type, i.e. scalar — build a 1-column descriptor.
                let funcrettype = resolved.result_type_id.unwrap_or_default();
                let td = backend_access_common_tupdesc::CreateTemplateTupleDesc(per_query, 1)?;
                let mut td = mcx::alloc_in(per_query, td)?;
                backend_access_common_tupdesc::TupleDescInitEntry(
                    &mut td, 1, None, funcrettype, -1, 0,
                )?;
                let collation =
                    backend_nodes_core::nodefuncs::expr_collation(sexpr.expr.as_deref())?;
                backend_access_common_tupdesc::TupleDescInitEntryCollation(&mut td, 1, collation)?;
                sexpr.funcReturnsTuple = false;
                sexpr.funcResultDesc = Some(td);
            }
            Some(types_nodes::funcapi::TypeFuncClass::Record) => {
                // Indeterminate rowtype — leave funcResultDesc = NULL; the
                // FunctionScan path resolves a RECORD result from column defs.
            }
            _ => {
                // crummy error message, but parser should have caught this.
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg("function in FROM has unsupported return type")
                    .into_error());
            }
        }
    }

    Ok(())
}

// ===========================================================================
//  ExecInitTableFunctionResult (execSRF.c:55)
// ===========================================================================

/// `ExecInitTableFunctionResult(expr, econtext, parent)` (execSRF.c:55) — build
/// the [`SetExprState`] for a function in a range-table function (FunctionScan /
/// ROWS FROM).
fn ExecInitTableFunctionResult<'mcx>(
    expr: &Expr<'mcx>,
    _econtext: EcxtId,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, SetExprState<'mcx>>> {
    let per_query = estate.es_query_cxt;
    let mut state = SetExprState::default();
    // C: state->funcReturnsSet = false; state->func.fn_oid = InvalidOid;
    state.funcReturnsSet = false;
    state.func.fn_oid = Oid::default();

    // C: state->expr = expr;
    state.expr = Some(mcx::alloc_in(per_query, expr.clone_in(per_query)?)?);

    if let Some(func) = expr.as_funcexpr() {
        // C: state->funcReturnsSet = func->funcretset;
        //    state->args = ExecInitExprList(func->args, parent);
        //    init_sexpr(func->funcid, func->inputcollid, expr, state, parent,
        //               econtext->ecxt_per_query_memory, func->funcretset, false);
        state.funcReturnsSet = func.funcretset;
        state.args = Some(init_expr_list(&func.args, parent, estate)?);
        init_sexpr(func.funcid, func.inputcollid, &mut state, func.funcretset, false, estate)?;
    } else {
        // C: state->elidedFuncState = ExecInitExpr(expr, parent);
        let es = backend_executor_execExpr_seams::exec_init_expr::call(expr, parent, estate)?;
        state.elidedFuncState = Some(es);
    }

    mcx::alloc_in(per_query, state)
}

/// `ExecInitExprList(args, parent)` over the function's argument expressions.
/// A NULL `Expr *` cell compiles to a `None` `ExprState` in C, but the SetExprState
/// `args` carries `ExprState` by value (positional), so we surface any NULL cell
/// loudly (an SRF call argument list never contains a NULL expression).
fn init_expr_list<'mcx>(
    args: &[Expr<'mcx>],
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<mcx::PgVec<'mcx, types_nodes::execexpr::ExprState<'mcx>>> {
    let _ = parent;
    let refs: Vec<Option<&Expr<'mcx>>> = args.iter().map(Some).collect();
    let states =
        backend_executor_execExpr_seams::exec_init_expr_list_no_parent::call(&refs, estate)?;
    let mut out = mcx::PgVec::new_in(estate.es_query_cxt);
    out.try_reserve(states.len()).map_err(|_| {
        estate
            .es_query_cxt
            .oom(states.len() * core::mem::size_of::<types_nodes::execexpr::ExprState>())
    })?;
    for s in states.into_iter() {
        out.push(s.expect("SRF argument expression compiled to a non-NULL ExprState"));
    }
    Ok(out)
}

/// Rebuild the live `ReturnSetInfo`'s materialize result (`setResult` tuplestore
/// + `setDesc`) from a USER (plpgsql/SQL) SETOF function's `MatSrfSink`, the
/// owned-model counterpart of `fmgr_sql`/`plpgsql_call_handler` filling
/// `rsinfo->setResult`/`setDesc` in place. Sets `returnMode = SFRM_Materialize`
/// so the caller takes the Materialize branch and (after the loop) cross-checks
/// `setDesc` against `expectedDesc`.
fn materialize_sink_into_rsinfo<'mcx>(
    rsinfo: &mut ReturnSetInfo<'mcx>,
    sink: types_fmgr::mat_srf::MatSrfSink,
    expected_desc: &TupleDescData<'mcx>,
    funcrettype: Oid,
    returns_tuple: bool,
    random_access: bool,
    per_query: Mcx<'mcx>,
) -> PgResult<()> {
    use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;

    // C: `rsinfo.returnMode = SFRM_Materialize;` (the callee chose materialize).
    rsinfo.returnMode = SetFunctionReturnMode::Materialize;
    rsinfo.isDone = ExprDoneCond::ExprSingleResult;

    // Build the result descriptor: a composite/whole-row SETOF function returns
    // rows shaped like `expectedDesc`; a scalar SETOF function returns a single
    // column whose type is `funcrettype` (C's `CreateTemplateTupleDesc(1)` +
    // `TupleDescInitEntry`). Both are charged to the per-query context.
    let result_desc: PgBox<'mcx, TupleDescData<'mcx>> = if returns_tuple {
        mcx::alloc_in(per_query, expected_desc.clone_in(per_query)?)?
    } else {
        let td = backend_access_common_tupdesc::CreateTemplateTupleDesc(per_query, 1)?;
        let mut td = mcx::alloc_in(per_query, td)?;
        backend_access_common_tupdesc::TupleDescInitEntry(
            &mut td,
            1,
            Some("column"),
            funcrettype,
            -1,
            0,
        )?;
        td
    };

    // C: `rsinfo.setResult = tuplestore_begin_heap(...);`
    let ts = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
        per_query,
        random_access,
        false,
        backend_utils_init_small_seams::work_mem::call(),
    )?;
    rsinfo.setResult = allocator_api2::boxed::Box::into_inner(ts);

    // Append each materialized row. A by-value column carries its bare word
    // (`ByVal`); a by-reference column carries its owned varlena/cstring/composite
    // image — the same `(value | ref_payload, isnull)` split the producer
    // (`fmgr_sql` capture receiver / plpgsql RETURN NEXT) emitted. The columns are
    // parallel to `result_desc`.
    let natts = result_desc.natts.max(0) as usize;
    for row in sink.rows.into_iter() {
        // C `exec_stmt_return_next` (pl_exec.c), the `estate->retistuple` arm:
        // when a composite-returning SETOF function does `RETURN NEXT <row-expr>`,
        // the producer (plpgsql) hands the whole composite back as ONE column
        // carrying its HeapTupleHeader image, but the result descriptor has the
        // rowtype's `natts` columns. C deconstructs that composite Datum into the
        // per-column tuple (`deconstruct_composite_datum` + `tuplestore_puttuple`);
        // mirror that here by deforming the single composite cell against
        // `result_desc` before storing. (A genuinely scalar SETOF, `natts == 1`,
        // never takes this branch.)
        if returns_tuple && natts != 1 && row.len() == 1 {
            let col = &row[0];
            if col.isnull {
                // Composite NULL: store a row of all-NULLs (C's else arm).
                let vals: alloc::vec::Vec<CanonDatum> =
                    (0..natts).map(|_| CanonDatum::default()).collect();
                let nuls: alloc::vec::Vec<bool> = (0..natts).map(|_| true).collect();
                backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                    &mut rsinfo.setResult,
                    &result_desc,
                    &vals,
                    &nuls,
                )?;
                continue;
            }
            let comp: CanonDatum = match &col.ref_payload {
                Some(types_fmgr::boundary::RefPayload::Varlena(b))
                | Some(types_fmgr::boundary::RefPayload::Composite(b)) => {
                    CanonDatum::ByRef(mcx::slice_in(per_query, b.as_slice())?)
                }
                _ => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_DATATYPE_MISMATCH)
                        .errmsg(
                            "cannot return non-composite value from function returning \
                             composite type",
                        )
                        .into_error());
                }
            };
            let formed =
                backend_access_common_heaptuple::DatumGetHeapTupleHeader(per_query, &comp)?;
            let cols = backend_access_common_heaptuple::heap_deform_tuple(
                per_query,
                &formed.tuple,
                &result_desc,
                &formed.data,
            )
            .map_err(|e| {
                ereport(ERROR)
                    .errcode(ERRCODE_INTERNAL_ERROR)
                    .errmsg(alloc::format!(
                        "heap_deform_tuple in RETURN NEXT composite: {e:?}"
                    ))
                    .into_error()
            })?;
            let vals: alloc::vec::Vec<CanonDatum> = cols.iter().map(|(d, _)| d.clone()).collect();
            let nuls: alloc::vec::Vec<bool> = cols.iter().map(|(_, n)| *n).collect();
            backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                &mut rsinfo.setResult,
                &result_desc,
                &vals,
                &nuls,
            )?;
            continue;
        }

        let mut values: alloc::vec::Vec<CanonDatum> = alloc::vec::Vec::with_capacity(natts);
        let mut nulls: alloc::vec::Vec<bool> = alloc::vec::Vec::with_capacity(natts);
        for col in row.into_iter() {
            if col.isnull {
                values.push(CanonDatum::default());
                nulls.push(true);
                continue;
            }
            let v = match col.ref_payload {
                None => CanonDatum::ByVal(col.value),
                Some(types_fmgr::boundary::RefPayload::Varlena(b)) => {
                    CanonDatum::ByRef(mcx::slice_in(per_query, b.as_slice())?)
                }
                Some(types_fmgr::boundary::RefPayload::Cstring(s)) => CanonDatum::Cstring(s),
                Some(types_fmgr::boundary::RefPayload::Composite(b)) => {
                    CanonDatum::ByRef(mcx::slice_in(per_query, b.as_slice())?)
                }
                Some(_) => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INTERNAL_ERROR)
                        .errmsg(
                            "SETOF function materialize sink: Expanded/Internal column \
                             value not supported",
                        )
                        .into_error());
                }
            };
            values.push(v);
            nulls.push(false);
        }
        // Pad a short row to the descriptor width with NULLs (defensive; a
        // well-formed producer emits `natts` columns per row).
        while values.len() < natts {
            values.push(CanonDatum::default());
            nulls.push(true);
        }
        backend_utils_sort_storage_seams::tuplestore_putvalues::call(
            &mut rsinfo.setResult,
            &result_desc,
            &values,
            &nulls,
        )?;
    }

    // C: `rsinfo.setDesc = <the descriptor the function built>`. The caller's
    // post-loop `tupledesc_match(expectedDesc, setDesc)` validates it.
    rsinfo.setDesc = Some(result_desc);
    Ok(())
}

// ===========================================================================
//  ExecMakeTableFunctionResult (execSRF.c:100) — the K2 value-per-call loop
// ===========================================================================

/// `ExecMakeTableFunctionResult(setexpr, econtext, argContext, expectedDesc,
/// randomAccess)` (execSRF.c:100) — evaluate a table function, producing a
/// materialized result in a Tuplestore. The faithful ValuePerCall +
/// Materialize-mode loop, dispatching the SRF through the executor-frame table
/// while threading the live `ReturnSetInfo`.
fn ExecMakeTableFunctionResult<'mcx>(
    setexpr: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    arg_context: &mut MemoryContext,
    expected_desc: &TupleDescData<'mcx>,
    random_access: bool,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, Tuplestorestate<'mcx>>> {
    let per_query: Mcx<'mcx> = estate.es_query_cxt;

    // C: MemoryContextReset(argContext);
    //    callerContext = MemoryContextSwitchTo(argContext);
    arg_context.reset();

    // C: funcrettype = exprType((Node *) setexpr->expr);
    //    returnsTuple = type_is_rowtype(funcrettype);
    let funcrettype =
        backend_nodes_core::nodefuncs::expr_type(setexpr.expr.as_deref())?;
    let returns_tuple =
        backend_utils_cache_lsyscache_seams::type_is_rowtype::call(funcrettype)?;

    // C: rsinfo.type = T_ReturnSetInfo; econtext/expectedDesc/allowedModes/...
    let mut allowed_modes =
        SFRM_ValuePerCall | SFRM_Materialize | SFRM_Materialize_Preferred;
    if random_access {
        allowed_modes |= SFRM_Materialize_Random;
    }
    let mut rsinfo = ReturnSetInfo {
        econtext: Some(econtext),
        expectedDesc: Some(mcx::alloc_in(per_query, expected_desc.clone_in(per_query)?)?),
        allowedModes: allowed_modes,
        returnMode: SetFunctionReturnMode::ValuePerCall,
        isDone: ExprDoneCond::ExprSingleResult,
        setResult: Tuplestorestate::default(),
        setDesc: None,
    };

    // For a scalar return type the loop builds a 1-column descriptor lazily.
    let mut tupdesc: Option<PgBox<'mcx, TupleDescData<'mcx>>> = None;
    let mut first_time = true;
    let returns_set = setexpr.funcReturnsSet;
    let elided = setexpr.elidedFuncState.is_some();

    // C: fcinfo = palloc(SizeForFunctionCallInfo(...));
    //    InitFunctionCallInfoData(*fcinfo, &(setexpr->func), ...,
    //                             setexpr->fcinfo->fncollation, NULL, &rsinfo);
    // The owned model dispatches through `setexpr->fcinfo` (the long-lived call
    // frame); the live ReturnSetInfo is threaded onto it for the call, then
    // recovered. `fn_extra`/`fn_mcxt` channels persist across the row series.

    'no_function_result: {
        if !elided {
            // C: ExecEvalFuncArgs(fcinfo, setexpr->args, econtext);
            // The args were compiled into setexpr->args; evaluate them in the
            // argContext (the caller already switched into it).
            exec_eval_func_args(setexpr, econtext, estate)?;

            // C: if (setexpr->func.fn_strict) { for each arg if NULL goto
            //    no_function_result; }
            if setexpr.func.fn_strict {
                let fcinfo = setexpr
                    .fcinfo
                    .as_ref()
                    .expect("ExecMakeTableFunctionResult: fcinfo not initialized");
                if fcinfo.args.iter().any(|a| a.isnull) {
                    break 'no_function_result;
                }
            }
        }

        // C: MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        //    for (;;) { ... ValuePerCall protocol ... }
        loop {
            // CHECK_FOR_INTERRUPTS();
            // C: ResetExprContext(econtext);
            estate.ecxt_mut(econtext).ecxt_per_tuple_memory.reset();

            // C: rsinfo.isDone = ExprSingleResult; result = FunctionCallInvoke(fcinfo);
            let (result, result_isnull) = if !elided {
                let fcinfo = setexpr
                    .fcinfo
                    .as_mut()
                    .expect("ExecMakeTableFunctionResult: fcinfo not initialized");
                fcinfo.isnull = false;
                // Thread the live ReturnSetInfo + cross-call channels onto the
                // frame for the call, dispatch, then take it back.
                fcinfo.resultinfo = Some(core::mem::take(&mut rsinfo));
                fcinfo.fn_mcxt = Some(per_query);
                fcinfo.resultinfo.as_mut().unwrap().isDone =
                    ExprDoneCond::ExprSingleResult;
                let foid = setexpr.func.fn_oid;
                let dispatch = srf_invoke_by_oid(foid, fcinfo)?;
                let isnull = fcinfo.isnull;
                rsinfo = fcinfo
                    .resultinfo
                    .take()
                    .expect("ExecMakeTableFunctionResult: resultinfo round-trip");
                match dispatch {
                    srf_registry::SrfDispatch::Builtin(res) => (res, isnull),
                    srf_registry::SrfDispatch::Materialized(sink) => {
                        // A USER (plpgsql/SQL) SETOF function ran the
                        // SFRM_Materialize protocol through the fmgr path: rebuild
                        // the live ReturnSetInfo's tuplestore + descriptor from the
                        // sink, exactly as C's `fmgr_sql`/`plpgsql` filled
                        // `rsinfo->setResult`/`setDesc`. Then the Materialize branch
                        // below sees `returnMode == SFRM_Materialize` and breaks.
                        materialize_sink_into_rsinfo(
                            &mut rsinfo,
                            sink,
                            expected_desc,
                            funcrettype,
                            returns_tuple,
                            random_access,
                            per_query,
                        )?;
                        (Datum::default(), true)
                    }
                }
            } else {
                // C: result = ExecEvalExpr(setexpr->elidedFuncState, econtext,
                //                          &fcinfo->isnull); rsinfo.isDone = ExprSingleResult;
                let st = setexpr
                    .elidedFuncState
                    .as_deref_mut()
                    .expect("elidedFuncState present");
                let (d, isnull) =
                    backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                        st, econtext, estate,
                    )?;
                rsinfo.isDone = ExprDoneCond::ExprSingleResult;
                (d, isnull)
            };

            // C: if (rsinfo.returnMode == SFRM_ValuePerCall) { ... }
            match rsinfo.returnMode {
                SetFunctionReturnMode::ValuePerCall => {
                    // C: if (rsinfo.isDone == ExprEndResult) break;
                    if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                        break;
                    }

                    // C: if (first_time) { build tuplestore (+scalar tupdesc) }
                    if first_time {
                        let ts = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
                            per_query,
                            random_access,
                            false,
                            backend_utils_init_small_seams::work_mem::call(),
                        )?;
                        rsinfo.setResult = allocator_api2::boxed::Box::into_inner(ts);
                        if !returns_tuple {
                            // CreateTemplateTupleDesc(1) + TupleDescInitEntry(1,
                            //     "column", funcrettype, -1, 0).
                            let td = backend_access_common_tupdesc::CreateTemplateTupleDesc(
                                per_query, 1,
                            )?;
                            let mut td = mcx::alloc_in(per_query, td)?;
                            backend_access_common_tupdesc::TupleDescInitEntry(
                                &mut td,
                                1,
                                Some("column"),
                                funcrettype,
                                -1,
                                0,
                            )?;
                            tupdesc = Some(td);
                            // rsinfo.setDesc points at the built desc (a copy for
                            // the cross-check below).
                            rsinfo.setDesc =
                                Some(mcx::alloc_in(per_query, tupdesc.as_ref().unwrap().clone_in(per_query)?)?);
                        }
                    }

                    // C: store current resultset item.
                    if returns_tuple {
                        // Composite return: C does `tuple = DatumGetHeapTupleHeader(result)`
                        // then `tuplestore_puttuple(tupstore, tuple)`. The owned model
                        // accepts EITHER carrier shape a composite-returning PGFunction
                        // produces: a live `Datum::Composite(FormedTuple)` (e.g.
                        // `pg_input_error_info`), or a `Datum::ByRef(image)` composite
                        // disk image (e.g. `json_to_record`'s `HeapTupleGetDatum`, which
                        // hands back the self-contained header+data byte image) — both are
                        // valid `HeapTupleHeader` Datums. `DatumGetHeapTupleHeader` is the
                        // C `DatumGetHeapTupleHeader(result)` that normalizes either into a
                        // `FormedTuple`. We then deform it against the expected row
                        // descriptor and store the per-column `(value, isnull)` series with
                        // `tuplestore_putvalues` (the same descriptor the printtup output
                        // lane reads it back with, so a text column's by-reference varlena
                        // round-trips header-for-header).
                        if !result_isnull {
                            let formed = match &result {
                                Datum::Composite(_) | Datum::ByRef(_) => {
                                    backend_access_common_heaptuple::DatumGetHeapTupleHeader(
                                        per_query, &result,
                                    )?
                                }
                                _ => {
                                    return Err(ereport(ERROR)
                                        .errcode(ERRCODE_INTERNAL_ERROR)
                                        .errmsg(
                                            "table function returning a composite type did not \
                                             return a composite Datum",
                                        )
                                        .into_error())
                                }
                            };
                            let cols = backend_access_common_heaptuple::heap_deform_tuple(
                                per_query,
                                &formed.tuple,
                                expected_desc,
                                &formed.data,
                            )
                            .map_err(|e| {
                                ereport(ERROR)
                                    .errcode(ERRCODE_INTERNAL_ERROR)
                                    .errmsg(alloc::format!(
                                        "heap_deform_tuple in table function: {e:?}"
                                    ))
                                    .into_error()
                            })?;
                            let values: Vec<Datum> =
                                cols.iter().map(|(d, _)| d.clone()).collect();
                            let nulls: Vec<bool> = cols.iter().map(|(_, n)| *n).collect();
                            backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                                &mut rsinfo.setResult,
                                expected_desc,
                                &values,
                                &nulls,
                            )?;
                        } else {
                            // A NULL composite Datum stores a single all-NULLs row
                            // (C: `tuplestore_puttuple` of a NULL is not reached; a
                            // strict composite SRF that yields NULL puts an all-NULL
                            // row matching the descriptor).
                            let natts = expected_desc.natts.max(0) as usize;
                            let values: Vec<Datum> =
                                (0..natts).map(|_| Datum::default()).collect();
                            let nulls: Vec<bool> = (0..natts).map(|_| true).collect();
                            backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                                &mut rsinfo.setResult,
                                expected_desc,
                                &values,
                                &nulls,
                            )?;
                        }
                    } else {
                        // C: tuplestore_putvalues(tupstore, tupdesc, &result,
                        //                         &fcinfo->isnull);
                        let td = tupdesc
                            .as_deref()
                            .expect("scalar SRF: tupdesc built on first_time");
                        let values = [result];
                        let nulls = [result_isnull];
                        backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                            &mut rsinfo.setResult,
                            td,
                            &values,
                            &nulls,
                        )?;
                    }

                    // C: if (rsinfo.isDone != ExprMultipleResult) break;
                    if rsinfo.isDone != ExprDoneCond::ExprMultipleResult {
                        break;
                    }

                    // C: if (!returnsSet) ereport(ERROR, "table-function
                    //    protocol for value-per-call mode was not followed");
                    if !returns_set {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED)
                            .errmsg(
                                "table-function protocol for value-per-call mode was not followed",
                            )
                            .into_error());
                    }
                }
                SetFunctionReturnMode::Materialize => {
                    // C: if (!first_time || rsinfo.isDone != ExprSingleResult ||
                    //        !returnsSet) ereport(ERROR, "... materialize ...");
                    if !first_time
                        || rsinfo.isDone != ExprDoneCond::ExprSingleResult
                        || !returns_set
                    {
                        return Err(ereport(ERROR)
                            .errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED)
                            .errmsg(
                                "table-function protocol for materialize mode was not followed",
                            )
                            .into_error());
                    }
                    // Done evaluating the set result.
                    break;
                }
            }

            first_time = false;
        }
    }

    // no_function_result:
    // C: if (rsinfo.setResult == NULL) { create tuplestore; if (!returnsSet)
    //    putvalues a single all-nulls row from expectedDesc; }
    if rsinfo.setResult.payload().is_none() {
        let ts = backend_utils_sort_storage_seams::tuplestore_begin_heap::call(
            per_query,
            random_access,
            false,
            backend_utils_init_small_seams::work_mem::call(),
        )?;
        rsinfo.setResult = allocator_api2::boxed::Box::into_inner(ts);

        if !returns_set {
            // natts all-nulls row from expectedDesc.
            let natts = expected_desc.natts.max(0) as usize;
            let values: Vec<Datum> = (0..natts).map(|_| Datum::default()).collect();
            let nulls: Vec<bool> = (0..natts).map(|_| true).collect();
            backend_utils_sort_storage_seams::tuplestore_putvalues::call(
                &mut rsinfo.setResult,
                expected_desc,
                &values,
                &nulls,
            )?;
        }
    }

    // C: if (rsinfo.setDesc) { tupledesc_match(expectedDesc, rsinfo.setDesc);
    //    if (rsinfo.setDesc->tdrefcount == -1) FreeTupleDesc(rsinfo.setDesc); }
    if let Some(set_desc) = rsinfo.setDesc.as_deref() {
        tupledesc_match(per_query, expected_desc, set_desc)?;
        // Dynamically-allocated TupleDesc is dropped by ownership (RAII).
    }

    // C: MemoryContextSwitchTo(callerContext); return rsinfo.setResult;
    let setResult = core::mem::take(&mut rsinfo.setResult);
    mcx::alloc_in(per_query, setResult)
}

/// `ExecEvalFuncArgs(fcinfo, argList, econtext)` (execSRF.c:833) — evaluate the
/// function's argument expressions into `fcinfo->args[]`.
fn exec_eval_func_args<'mcx>(
    sexpr: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    // Evaluate each compiled arg ExprState into the call frame's args cells.
    // The arg states live in `sexpr.args`; the frame in `sexpr.fcinfo`.
    let n = sexpr.args.as_ref().map(|a| a.len()).unwrap_or(0);
    for i in 0..n {
        let (value, isnull) = {
            let argstate = &mut sexpr.args.as_mut().unwrap()[i];
            backend_executor_execExpr_seams::exec_eval_expr_switch_context::call(
                argstate, econtext, estate,
            )?
        };
        let fcinfo = sexpr
            .fcinfo
            .as_mut()
            .expect("ExecEvalFuncArgs: fcinfo not initialized");
        // The compiled argument expression produced a canonical
        // `types_tuple::Datum`; the fmgr call frame carries the bare-word
        // `args[i].value` plus the by-reference `ref_args[i]` side channel.
        // Marshal each kind onto the frame: a by-value scalar is the bare word
        // (no referent); a by-reference value (text/varlena/cstring/composite)
        // passes a null word plus its image in `ref_args[i]` — exactly the C
        // "`args[i].value` is a pointer to the referent" convention, so the
        // callee's `PG_GETARG_TEXT_PP`/`PG_GETARG_CSTRING` readers see the
        // value. (The old `as_usize()` downgrade panicked on a by-ref arg —
        // the `pg_input_error_info('junk','bool')` wall.)
        use types_tuple::backend_access_common_heaptuple::Datum as CanonDatum;
        use types_nodes::fmgr::FmgrArgRef;
        match value {
            CanonDatum::ByVal(word) => {
                fcinfo.args[i].value = types_datum::Datum::from_usize(word);
            }
            CanonDatum::ByRef(bytes) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Varlena(bytes.as_slice().to_vec()));
            }
            CanonDatum::Cstring(s) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Cstring(s.to_string()));
            }
            CanonDatum::Composite(t) => {
                fcinfo.args[i].value = types_datum::Datum::null();
                fcinfo.set_ref_arg(i, FmgrArgRef::Varlena(t.to_datum_image()));
            }
            CanonDatum::Expanded(_) | CanonDatum::Internal(_) => {
                return Err(types_error::PgError::error(
                    "ExecEvalFuncArgs: Expanded/Internal argument not supported on the SRF call frame",
                ));
            }
        }
        fcinfo.args[i].isnull = isnull;
    }
    Ok(())
}

// ===========================================================================
//  ExecInitFunctionResultSet / ExecMakeFunctionResultSet (ProjectSet path)
// ===========================================================================

/// `ExecInitFunctionResultSet(expr, econtext, parent)` (execSRF.c:443) — prepare
/// a targetlist SRF for execution (nodeProjectSet.c).
fn ExecInitFunctionResultSet<'mcx>(
    expr: &Expr<'mcx>,
    _econtext: EcxtId,
    parent: &mut PlanStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<PgBox<'mcx, SetExprState<'mcx>>> {
    let per_query = estate.es_query_cxt;
    let mut state = SetExprState::default();
    // C: state->funcReturnsSet = true; state->func.fn_oid = InvalidOid;
    state.funcReturnsSet = true;
    state.func.fn_oid = Oid::default();
    state.expr = Some(mcx::alloc_in(per_query, expr.clone_in(per_query)?)?);

    if let Some(func) = expr.as_funcexpr() {
        // C: state->args = ExecInitExprList(func->args, parent);
        //    init_sexpr(func->funcid, func->inputcollid, ..., true, true);
        state.args = Some(init_expr_list(&func.args, parent, estate)?);
        init_sexpr(func.funcid, func.inputcollid, &mut state, true, true, estate)?;
    } else if let Some(op) = expr.as_opexpr() {
        // C: state->args = ExecInitExprList(op->args, parent);
        //    init_sexpr(op->opfuncid, op->inputcollid, ..., true, true);
        state.args = Some(init_expr_list(&op.args, parent, estate)?);
        init_sexpr(op.opfuncid, op.inputcollid, &mut state, true, true, estate)?;
    } else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INTERNAL_ERROR)
            .errmsg(alloc::format!("unrecognized node type: {expr:?}"))
            .into_error());
    }

    // C: Assert(state->func.fn_retset);  (the selected function returns a set.)
    mcx::alloc_in(estate.es_query_cxt, state)
}

/// `ExecPrepareTuplestoreResult(sexpr, econtext, resultStore, resultDesc)`
/// (execSRF.c:864) — set up to return values from an SRF's whole-tuplestore
/// result. Stows the tuplestore on `funcResultStore` and (lazily) builds the
/// `funcResultSlot` the drain reads each row out of.
///
/// In the owned model the C raw `TupleTableSlot *funcResultSlot` is an EState
/// tuple-table pool [`SlotId`]; `MakeSingleTupleTableSlot(slotDesc,
/// &TTSOpsMinimalTuple)` ↦ `ExecInitExtraTupleSlot` against the per-query pool
/// (the slot lives as long as the SRF result, i.e. the query, which matches the
/// C `func.fn_mcxt` lifetime for this leg).
fn exec_prepare_tuplestore_result<'mcx>(
    sexpr: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    result_store: Tuplestorestate<'mcx>,
    result_desc: Option<PgBox<'mcx, TupleDescData<'mcx>>>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let _ = econtext;
    let per_query = estate.es_query_cxt;

    // sexpr->funcResultStore = resultStore;
    sexpr.funcResultStore = Some(mcx::alloc_in(per_query, result_store)?);

    if sexpr.funcResultSlot.is_none() {
        // Create a slot so we can read data out of the tuplestore. C picks the
        // descriptor: funcResultDesc if known, else the function-provided
        // resultDesc (copied so we don't assume it's long-lived), else fail.
        let slot_desc: PgBox<'mcx, TupleDescData<'mcx>> = if let Some(d) =
            sexpr.funcResultDesc.as_deref()
        {
            mcx::alloc_in(per_query, d.clone_in(per_query)?)?
        } else if let Some(rd) = result_desc.as_deref() {
            // don't assume resultDesc is long-lived: CreateTupleDescCopy.
            let copy = backend_access_common_tupdesc::CreateTupleDescCopy(per_query, rd)?;
            mcx::alloc_in(per_query, copy)?
        } else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(
                    "function returning setof record called in context that cannot accept type record",
                )
                .into_error());
        };

        // funcResultSlot = MakeSingleTupleTableSlot(slotDesc, &TTSOpsMinimalTuple);
        // The descriptor moves into the pool slot.
        let slot = backend_executor_execTuples_seams::exec_init_extra_tuple_slot::call(
            estate,
            Some(slot_desc),
            TupleSlotKind::MinimalTuple,
        )?;
        sexpr.funcResultSlot = Some(slot);
    }

    // If function provided a tupdesc, cross-check it against funcResultDesc and
    // (in C) free a dynamically-allocated one. The owned model drops
    // `result_desc` by ownership at end of scope.
    if let Some(rd) = result_desc.as_deref() {
        if let Some(frd) = sexpr.funcResultDesc.as_deref() {
            tupledesc_match(per_query, frd, rd)?;
        }
    }

    // Register cleanup callback if we didn't already (C: RegisterExprContextCallback
    // ShutdownSetExpr). The owned drain ends the tuplestore inline when exhausted
    // (tuplestore_end in the drain tail); record the registration flag for parity.
    sexpr.shutdown_reg = true;

    Ok(())
}

/// `ExecMakeFunctionResultSet(fcache, econtext, argContext, &isNull, &isDone)`
/// (execSRF.c:496) — evaluate a targetlist SRF and return one result row's
/// `(Datum, isNull, isDone)`. nodeProjectSet.c.
///
/// Both protocols are ported: the ValuePerCall loop (one `(Datum, isnull,
/// isDone)` per call, reporting `ExprMultipleResult` until exhaustion — the
/// path `generate_series`/`unnest` take) and the Materialize leg
/// (`SFRM_Materialize`: the function returns a whole tuplestore, prepared by
/// [`exec_prepare_tuplestore_result`] and drained row-by-row through
/// `funcResultStore`/`funcResultSlot`).
fn ExecMakeFunctionResultSet<'mcx>(
    fcache: &mut SetExprState<'mcx>,
    econtext: EcxtId,
    arg_context: &MemoryContext,
    estate: &mut EStateData<'mcx>,
) -> PgResult<(Datum<'mcx>, bool, ExprDoneCond)> {
    let _ = arg_context;

    // C `restart:` — re-entered after a Materialize-mode call sets up the
    // tuplestore. In this port the Materialize leg panics, so the loop body
    // runs at most once after the (unreachable here) tuplestore setup.
    loop {
        // Guard against stack overflow due to overly complex expressions.
        backend_tcop_postgres_seams::check_stack_depth::call()?;

        // If a previous call of the function returned a set result in the form
        // of a tuplestore, continue reading rows from it until it's empty
        // (execSRF.c:519). `funcResultSlot` is the pool slot prepared by
        // ExecPrepareTuplestoreResult.
        if fcache.funcResultStore.is_some() {
            let slot = fcache
                .funcResultSlot
                .expect("funcResultStore set without a funcResultSlot");
            // foundTup = tuplestore_gettupleslot(funcResultStore, true, false,
            //                                    funcResultSlot);
            // (C switches into slot->tts_mcxt so the fetched tuple outlives the
            // slot clear; the owned slot pool allocates the carrier in the
            // per-query context, so no switch is needed.)
            let found_tup = {
                let store = fcache
                    .funcResultStore
                    .as_mut()
                    .expect("funcResultStore present");
                backend_utils_sort_storage_seams::tuplestore_gettupleslot::call(
                    store, true, false, slot, estate,
                )?
            };

            if found_tup {
                // *isDone = ExprMultipleResult;
                if fcache.funcReturnsTuple {
                    // We must return the whole tuple as a Datum.
                    // return ExecFetchSlotHeapTupleDatum(funcResultSlot);
                    let d =
                        backend_executor_execTuples_seams::exec_fetch_slot_heap_tuple_datum::call(
                            estate, slot,
                        )?;
                    return Ok((d, false, ExprDoneCond::ExprMultipleResult));
                } else {
                    // Extract the first column and return it as a scalar.
                    // return slot_getattr(funcResultSlot, 1, isNull);
                    let (d, isnull) = backend_executor_execTuples_seams::slot_getattr::call(
                        estate, slot, 1,
                    )?;
                    return Ok((d, isnull, ExprDoneCond::ExprMultipleResult));
                }
            }
            // Exhausted the tuplestore, so clean up.
            // tuplestore_end(funcResultStore); funcResultStore = NULL;
            if let Some(store) = fcache.funcResultStore.take() {
                backend_utils_sort_storage_seams::tuplestore_end::call(store);
            }
            // *isDone = ExprEndResult; *isNull = true; return (Datum) 0;
            return Ok((Datum::default(), true, ExprDoneCond::ExprEndResult));
        }

        // Collect the current argument values into fcinfo, unless we already
        // did so on a previous call of this set-valued function.
        if !fcache.setArgsValid {
            // ExecEvalFuncArgs(fcinfo, fcache->args, econtext) — evaluated in
            // argContext so ValuePerCall SRFs don't reference freed memory.
            exec_eval_func_args(fcache, econtext, estate)?;
        } else {
            // Reset flag (we may set it again below).
            fcache.setArgsValid = false;
        }

        // If function is strict and any argument is NULL, skip calling it; a
        // strict SRF's result for NULL is an empty set (execSRF.c:625).
        let mut callit = true;
        if fcache.func.fn_strict {
            let fcinfo = fcache
                .fcinfo
                .as_ref()
                .expect("ExecMakeFunctionResultSet: fcinfo not initialized");
            if fcinfo.args.iter().any(|a| a.isnull) {
                callit = false;
            }
        }

        let (result, result_isnull, mut this_isdone, return_mode);
        if callit {
            // Thread a live ReturnSetInfo onto the call frame, dispatch, recover.
            let mut rsinfo = ReturnSetInfo {
                econtext: Some(econtext),
                expectedDesc: fcache
                    .funcResultDesc
                    .as_deref()
                    .map(|d| mcx::alloc_in(estate.es_query_cxt, d.clone_in(estate.es_query_cxt)?))
                    .transpose()?,
                allowedModes: SFRM_ValuePerCall | SFRM_Materialize,
                returnMode: SetFunctionReturnMode::ValuePerCall,
                isDone: ExprDoneCond::ExprSingleResult,
                setResult: Tuplestorestate::default(),
                setDesc: None,
            };
            let foid = fcache.func.fn_oid;
            // For a USER SETOF function in a targetlist, the result-column type
            // (scalar) / row-ness (composite) come from the expression type.
            let funcrettype_tl =
                backend_nodes_core::nodefuncs::expr_type(fcache.expr.as_deref())?;
            let returns_tuple_tl =
                backend_utils_cache_lsyscache_seams::type_is_rowtype::call(funcrettype_tl)?;
            let fcinfo = fcache
                .fcinfo
                .as_mut()
                .expect("ExecMakeFunctionResultSet: fcinfo not initialized");
            fcinfo.isnull = false;
            fcinfo.fn_mcxt = Some(estate.es_query_cxt);
            fcinfo.resultinfo = Some(core::mem::take(&mut rsinfo));
            let dispatch = srf_invoke_by_oid(foid, fcinfo)?;
            let isnull = fcinfo.isnull;
            rsinfo = fcinfo
                .resultinfo
                .take()
                .expect("ExecMakeFunctionResultSet: resultinfo round-trip");
            let res = match dispatch {
                srf_registry::SrfDispatch::Builtin(d) => d,
                srf_registry::SrfDispatch::Materialized(sink) => {
                    // A USER (plpgsql/SQL) SETOF function in a targetlist: rebuild
                    // the live ReturnSetInfo's materialize tuplestore from the
                    // sink, then fall into the Materialize branch below (which
                    // drains it row-by-row via exec_prepare_tuplestore_result).
                    let exp = match rsinfo.expectedDesc.as_deref() {
                        Some(d) => d.clone_in(estate.es_query_cxt)?,
                        None => backend_access_common_tupdesc::CreateTemplateTupleDesc(
                            estate.es_query_cxt,
                            0,
                        )?,
                    };
                    materialize_sink_into_rsinfo(
                        &mut rsinfo,
                        sink,
                        &exp,
                        funcrettype_tl,
                        returns_tuple_tl,
                        false,
                        estate.es_query_cxt,
                    )?;
                    Datum::default()
                }
            };
            result = res;
            result_isnull = isnull;
            this_isdone = rsinfo.isDone;
            return_mode = rsinfo.returnMode;
            // SFRM_Materialize: the function built a whole tuplestore; prepare
            // to drain it row-by-row (execSRF.c:658).
            if matches!(return_mode, SetFunctionReturnMode::Materialize) {
                // Protocol cross-check: materialize mode must report
                // ExprSingleResult (execSRF.c:660).
                if this_isdone != ExprDoneCond::ExprSingleResult {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_E_R_I_E_SRF_PROTOCOL_VIOLATED)
                        .errmsg("table-function protocol for materialize mode was not followed")
                        .into_error());
                }
                if rsinfo.setResult.payload().is_some() {
                    // prepare to return values from the tuplestore.
                    let set_desc = rsinfo.setDesc.take();
                    let set_result = core::mem::take(&mut rsinfo.setResult);
                    exec_prepare_tuplestore_result(
                        fcache, econtext, set_result, set_desc, estate,
                    )?;
                    // loop back to top to start returning from the tuplestore.
                    continue;
                }
                // if setResult was left null, treat it as empty set.
                return Ok((Datum::default(), true, ExprDoneCond::ExprEndResult));
            }
        } else {
            // Strict SRF with a NULL argument ⇒ empty set.
            result = Datum::default();
            result_isnull = true;
            this_isdone = ExprDoneCond::ExprEndResult;
            return_mode = SetFunctionReturnMode::ValuePerCall;
        }

        // ValuePerCall protocol bookkeeping (execSRF.c:638).
        debug_assert!(matches!(return_mode, SetFunctionReturnMode::ValuePerCall));
        if this_isdone != ExprDoneCond::ExprEndResult {
            // Save the current argument values to re-use on the next call when
            // the function reported it has more rows to come.
            if this_isdone == ExprDoneCond::ExprMultipleResult {
                fcache.setArgsValid = true;
                // C registers a ShutdownSetExpr cleanup callback here. In the
                // owned model the ValuePerCall series holds no tuplestore to
                // free (funcResultStore stays NULL), so the shutdown is a no-op;
                // we record that "registration" without a raw-pointer callback.
                fcache.shutdown_reg = true;
            }
        } else {
            // Reflect the ExprEndResult in the caller's isdone (already set).
            this_isdone = ExprDoneCond::ExprEndResult;
        }

        return Ok((result, result_isnull, this_isdone));
    }
}

/// `RestartSetExprState(fcache)` — reset a [`SetExprState`] that may have been
/// abandoned mid value-per-call series (e.g. a tSRF cut short by an enclosing
/// LIMIT), so a subsequent rescan re-evaluates it from the start.
///
/// This is the owned-model equivalent of the cleanup C performs through the
/// `ExprContext` shutdown callback that `init_MultiFuncCall` registers on
/// `rsi->econtext`. In C, `ExecReScan` calls `ReScanExprContext(node->
/// ps_ExprContext)` before the node-specific rescan, which fires
/// `shutdown_MultiFuncCall` for every SRF that left a `FuncCallContext` in
/// `flinfo->fn_extra`; that resets `fn_extra` to NULL so the next call is a
/// fresh `SRF_IS_FIRSTCALL()`. The owned model cannot register that bare-`fn`
/// callback (the cross-call `fn_extra` lives on the owned call frame, which the
/// callback cannot name — see `init_MultiFuncCall`), so the rescanning node
/// (nodeProjectSet) drives the same teardown directly through this function for
/// each of its SRF elements.
///
/// It mirrors `shutdown_MultiFuncCall` (tear down any leftover multi-call
/// context bound to `fn_extra`), ends any partially-drained materialize-mode
/// `funcResultStore`, and clears `setArgsValid` so the next call re-evaluates the
/// function arguments. A SetExprState that ran to completion (`fn_extra` already
/// NULL, no `funcResultStore`, `setArgsValid` false) is left untouched.
pub fn RestartSetExprState<'mcx>(fcache: &mut SetExprState<'mcx>) -> PgResult<()> {
    // Tear down any leftover value-per-call cross-call context: the C ExprContext
    // shutdown callback (`shutdown_MultiFuncCall`) unbinds `flinfo->fn_extra` and
    // deletes the SRF multi-call context. `end_MultiFuncCall` performs exactly
    // that teardown off the owned `fn_extra` channel.
    if let Some(fcinfo) = fcache.fcinfo.as_deref_mut() {
        if fcinfo.fn_extra.is_some() {
            backend_utils_fmgr_funcapi_seams::end_MultiFuncCall::call(fcinfo)?;
        }
    }

    // End any partially-drained materialize-mode tuplestore (C frees it when the
    // store is exhausted in ExecMakeFunctionResultSet; an abandoned one must be
    // released here so the rescan starts clean).
    if let Some(store) = fcache.funcResultStore.take() {
        backend_utils_sort_storage_seams::tuplestore_end::call(store);
    }
    fcache.funcResultSlot = None;

    // Forget any half-collected arguments so the next call re-evaluates them
    // (C: setArgsValid is only meaningful while a series is in flight).
    fcache.setArgsValid = false;

    Ok(())
}

// ===========================================================================
//  tupledesc_match (execSRF.c:942)
// ===========================================================================

/// `tupledesc_match(dst_tupdesc, src_tupdesc)` (execSRF.c:942) — check that the
/// function's result tuple type matches what the query expects (number of
/// attributes; per-attribute binary-coercibility, ignoring dropped columns
/// whose physical storage still matches).
fn tupledesc_match<'mcx>(
    mcx: Mcx<'mcx>,
    dst: &TupleDescData<'mcx>,
    src: &TupleDescData<'mcx>,
) -> PgResult<()> {
    // C: if (dst->natts != src->natts) ereport(ERROR, "function return row and
    //    query-specified return row do not match",
    //    errdetail_plural("Returned row contains %d attribute, ...", ...));
    if dst.natts != src.natts {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DATATYPE_MISMATCH)
            .errmsg("function return row and query-specified return row do not match")
            .errdetail_plural(
                alloc::format!(
                    "Returned row contains {} attribute, but query expects {}.",
                    src.natts, dst.natts
                ),
                alloc::format!(
                    "Returned row contains {} attributes, but query expects {}.",
                    src.natts, dst.natts
                ),
                src.natts as u64,
            )
            .into_error());
    }

    // for (i = 0; i < dst->natts; i++)
    for i in 0..dst.natts as usize {
        // Form_pg_attribute dattr = TupleDescAttr(dst_tupdesc, i);
        // Form_pg_attribute sattr = TupleDescAttr(src_tupdesc, i);
        let dattr = &dst.attrs[i];
        let sattr = &src.attrs[i];

        // if (IsBinaryCoercible(sattr->atttypid, dattr->atttypid)) continue;
        //
        // IsBinaryCoercibleWithCast (parse_coerce.c) short-circuits identical
        // types before any pg_cast lookup; mirror that fast path here so the
        // overwhelmingly-common matching-type case needs no catalog access.
        if sattr.atttypid == dattr.atttypid
            || backend_parser_coerce_seams::is_binary_coercible::call(
                sattr.atttypid,
                dattr.atttypid,
            )?
        {
            continue; // no worries
        }

        // if (!dattr->attisdropped)
        //     ereport("Returned type %s at ordinal position %d, but query expects %s.")
        if !dattr.attisdropped {
            let returned =
                backend_utils_adt_format_type_seams::format_type_be::call(mcx, sattr.atttypid)?;
            let expects =
                backend_utils_adt_format_type_seams::format_type_be::call(mcx, dattr.atttypid)?;
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("function return row and query-specified return row do not match")
                .errdetail(alloc::format!(
                    "Returned type {} at ordinal position {}, but query expects {}.",
                    returned.as_str(),
                    i + 1,
                    expects.as_str(),
                ))
                .into_error());
        }

        // Dropped column: physical storage must still match.
        // if (dattr->attlen != sattr->attlen || dattr->attalign != sattr->attalign)
        //     ereport("Physical storage mismatch on dropped attribute ...")
        if dattr.attlen != sattr.attlen || dattr.attalign != sattr.attalign {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DATATYPE_MISMATCH)
                .errmsg("function return row and query-specified return row do not match")
                .errdetail(alloc::format!(
                    "Physical storage mismatch on dropped attribute at ordinal position {}.",
                    i + 1
                ))
                .into_error());
        }
    }

    Ok(())
}
