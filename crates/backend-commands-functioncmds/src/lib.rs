#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/functioncmds.c` — CREATE/ALTER/DROP FUNCTION & PROCEDURE,
//! CREATE CAST, CREATE TRANSFORM, DO, CALL support (PostgreSQL 18.3).
//!
//! Decomposed into per-family modules off the shared [`keystone`] foundation:
//!
//!   * [`keystone`] — shared carrier types / ABI / lifetime foundation, the
//!     `Node`/`DefElem`/`TypeName` extraction helpers, the shared error
//!     helpers, `check_language_permissions`, and the `ObjectType`/language-OID
//!     constants. Ported in full so every other family compiles.
//!   * [`ddl_core`] — `CreateFunction` / `AlterFunction` / `RemoveFunctionById`
//!     and their static helpers.
//!   * [`cast_transform_do`] — `CreateCast`, `CreateTransform`,
//!     `get_transform_oid`, `IsThereFunctionInNamespace`, `ExecuteDoStmt`.
//!   * [`call_stmt`] — `ExecuteCallStmt` + `CallStmtResultDesc` (Family 4, the
//!     genuine remaining decomp work).
//!
//! Carrier types live in the LAYERED `types-*` stack and the per-owner
//! [`backend_commands_functioncmds_seams`] crate (NOT the monolithic
//! src-idiomatic `seams`/`types` crates, which do not exist in this repo).
//! Every genuine external crosses a per-owner seam, panicking until its owner
//! lands; `QualifiedNameGetCreationNamespace` and the
//! `GetSysCacheOid2(TRFTYPELANG, …)` core are called directly.

mod call_stmt;
mod cast_transform_do;
mod ddl_core;
mod keystone;

pub use call_stmt::{CallStmtResultDesc, ExecuteCallStmt};
pub use cast_transform_do::{
    get_transform_oid, CreateCast, CreateTransform, ExecuteDoStmt, IsThereFunctionInNamespace,
};
pub use ddl_core::{
    interpret_function_parameter_list, AlterFunction, CreateFunction, InterpretedParameters,
    RemoveFunctionById,
};

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every seam this crate owns. functioncmds owns the `get_transform_oid`
/// lookup seam (consumed by objectaddress's resolution engine); the rest of the
/// crate's surface is reached only from unported callers.
pub fn init_seams() {
    backend_commands_functioncmds_seams::get_transform_oid::set(
        cast_transform_do::get_transform_oid,
    );
    // `ExecuteDoStmt` (DO) — reached by standard_ProcessUtility's T_DoStmt arm
    // through the `backend_tcop_utility_out_seams::execute_do_stmt` inward seam.
    backend_tcop_utility_out_seams::execute_do_stmt::set(cast_transform_do::execute_do_stmt_seam);

    // ProcessUtilitySlow dispatch targets (utility.c): CREATE FUNCTION / CREATE
    // CAST. Decode the rich statement node into the flat parsenodes form the
    // ported bodies consume, then run them.
    backend_tcop_utility_out_seams::create_function::set(create_function_seam);
    backend_tcop_utility_out_seams::create_cast::set(create_cast_seam);

    // ProcessUtilitySlow dispatch target (utility.c): CREATE TRANSFORM. The
    // ported body consumes the rich `CreateTransformStmt` directly.
    backend_tcop_utility_out_seams::create_transform::set(create_transform_seam);

    // ProcessUtilitySlow dispatch target (utility.c): ALTER FUNCTION/PROCEDURE/
    // ROUTINE. Decode the rich statement into the flat parsenodes form the ported
    // `AlterFunction` body consumes.
    backend_tcop_utility_out_seams::alter_function::set(alter_function_seam);

    // `aclcheck_error(aclresult, OBJECT_LANGUAGE, lanname)` (functioncmds.c) —
    // the language-USAGE permission-denied raise reached when creating a
    // function/DO/transform in a language the caller lacks USAGE on. Delegates
    // to the generic `aclcheck_error` in aclchk.
    backend_commands_functioncmds_seams::aclcheck_error_language::set(aclcheck_error_language_seam);

    // CALL (functioncmds.c): `ExecuteCallStmt` is reached by
    // standard_ProcessUtility's T_CallStmt arm, and `CallStmtResultDesc` by
    // UtilityTupleDescriptor — both through the `backend_tcop_utility_out_seams`
    // inward seams, operating on the live `T_CallStmt` node whose `funcexpr` /
    // `outargs` transformCallStmt (analyze.c) already populated.
    backend_tcop_utility_out_seams::execute_call_stmt::set(call_stmt::execute_call_stmt_seam);
    backend_tcop_utility_out_seams::call_stmt_result_desc::set(
        call_stmt::call_stmt_result_desc_seam,
    );
}

/// `aclcheck_error(aclresult, OBJECT_LANGUAGE, lanname)` (functioncmds.c).
fn aclcheck_error_language_seam(
    aclresult: types_acl::AclResult,
    objname: String,
) -> types_error::PgResult<()> {
    backend_catalog_aclchk_seams::aclcheck_error::call(
        aclresult,
        types_nodes::parsenodes::ObjectType::Language,
        Some(objname),
    )
}

/// Outward-seam adapter for `AlterFunction(pstate, stmt)` (utility.c
/// `ProcessUtilitySlow` `T_AlterFunctionStmt`): decode the rich
/// `AlterFunctionStmt` into the flat [`types_parsenodes::AlterFunctionStmt`] and
/// run the ported [`ddl_core::AlterFunction`] body. `pstate` is threaded for
/// parity; `AlterFunction` re-derives what it needs.
fn alter_function_seam<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    _pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &types_nodes::nodes::Node<'mcx>,
) -> types_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    use backend_parser_parse_type::rich_node_to_parse;
    use types_error::PgError;

    let afs = match stmt.as_alterfunctionstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "alter_function_seam: statement is not an AlterFunctionStmt",
            ))
        }
    };

    let func = match afs.func.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };

    let mut actions: Vec<types_parsenodes::Node> = Vec::with_capacity(afs.actions.len());
    for n in afs.actions.iter() {
        actions.push(rich_node_to_parse(n)?);
    }

    let pn = types_parsenodes::AlterFunctionStmt {
        objtype: afs.objtype as i32,
        func,
        actions,
    };

    ddl_core::AlterFunction(&pn)
}

/// Outward-seam adapter for `CreateTransform(stmt)` (utility.c
/// `ProcessUtilitySlow` `T_CreateTransformStmt`): decode the rich
/// `CreateTransformStmt` into the flat [`types_parsenodes::CreateTransformStmt`]
/// the ported body consumes.
fn create_transform_seam<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    stmt: &types_nodes::nodes::Node<'mcx>,
) -> types_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    use backend_parser_parse_type::rich_node_to_parse;
    use types_error::PgError;

    let cts = match stmt.as_createtransformstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "create_transform_seam: statement is not a CreateTransformStmt",
            ))
        }
    };

    let type_name = match cts.type_name.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };
    let fromsql = match cts.fromsql.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };
    let tosql = match cts.tosql.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };

    let pn = types_parsenodes::CreateTransformStmt {
        replace: cts.replace,
        type_name,
        lang: cts.lang.as_ref().map(|s| s.as_str().to_string()),
        fromsql,
        tosql,
    };

    CreateTransform(&pn)
}

/// Outward-seam adapter for `CreateFunction(pstate, stmt)` (utility.c
/// `ProcessUtilitySlow` `T_CreateFunctionStmt`): decode the rich
/// `CreateFunctionStmt` into the flat [`types_parsenodes::CreateFunctionStmt`]
/// and run the ported [`ddl_core::CreateFunction`] body. `pstate` is threaded for
/// parity; `CreateFunction` re-derives what it needs.
fn create_function_seam<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    pstate: &mut types_nodes::parsestmt::ParseState<'mcx>,
    stmt: &types_nodes::nodes::Node<'mcx>,
) -> types_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    use backend_parser_parse_type::rich_node_to_parse;
    use types_error::PgError;

    let cfs = match stmt.as_createfunctionstmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "create_function_seam: statement is not a CreateFunctionStmt",
            ))
        }
    };

    // funcname: List of String -> Vec<StringNode>.
    let mut funcname: Vec<types_parsenodes::StringNode> =
        Vec::with_capacity(cfs.funcname.len());
    for n in cfs.funcname.iter() {
        match n.as_string() {
            Some(s) => funcname.push(types_parsenodes::StringNode {
                sval: Some(s.sval.as_str().to_string()),
            }),
            None => {
                return Err(PgError::error(
                    "CREATE FUNCTION: function name element is not a String",
                ))
            }
        }
    }

    let mut parameters: Vec<types_parsenodes::Node> =
        Vec::with_capacity(cfs.parameters.len());
    for n in cfs.parameters.iter() {
        parameters.push(rich_node_to_parse(n)?);
    }

    let returnType = match cfs.returnType.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };

    let mut options: Vec<types_parsenodes::Node> =
        Vec::with_capacity(cfs.options.len());
    for n in cfs.options.iter() {
        options.push(rich_node_to_parse(n)?);
    }

    // The SQL-standard function body (`RETURN expr` -> a `ReturnStmt`, or
    // `BEGIN ATOMIC ... END` -> a statement list) is a rich raw parse tree that
    // the flat `types_parsenodes` vocabulary cannot represent. It is threaded
    // directly as a rich node into `CreateFunction`, which transforms it
    // (`transformStmt`) into the cooked `prosqlbody`. The flat `sql_body` field
    // only ever needs to signal presence, which `CreateFunction` reads from the
    // rich node instead.
    let sql_body_rich: Option<&types_nodes::nodes::Node<'mcx>> = cfs.sql_body.as_deref();

    let pn = types_parsenodes::CreateFunctionStmt {
        is_procedure: cfs.is_procedure,
        replace: cfs.replace,
        funcname,
        parameters,
        returnType,
        options,
        sql_body: None,
    };

    // The rich `FunctionParameter` nodes carry the raw DEFAULT expressions (an
    // arbitrary `A_Const`/`TypeCast`/`FuncCall`/... node the flat
    // `types_parsenodes` vocabulary can't hold); thread them as a side-channel
    // parallel to the flat `parameters`, exactly as `sql_body_rich` is threaded.
    let rich_parameters: Vec<&types_nodes::nodes::Node<'mcx>> =
        cfs.parameters.iter().map(|n| n.as_ref()).collect();

    let query_string = pstate.p_sourcetext.as_ref().map(|s| s.as_str().to_string());
    ddl_core::CreateFunction(mcx, &pn, &rich_parameters, sql_body_rich, query_string)
}

/// Outward-seam adapter for `CreateCast(stmt)` (utility.c `ProcessUtilitySlow`
/// `T_CreateCastStmt`): decode the rich `CreateCastStmt` into the flat
/// [`types_parsenodes::CreateCastStmt`] and run the ported
/// [`cast_transform_do::CreateCast`] body.
fn create_cast_seam<'mcx>(
    _mcx: mcx::Mcx<'mcx>,
    stmt: &types_nodes::nodes::Node<'mcx>,
) -> types_error::PgResult<types_catalog::catalog_dependency::ObjectAddress> {
    use backend_parser_parse_type::rich_node_to_parse;
    use types_error::PgError;

    let ccs = match stmt.as_createcaststmt() {
        Some(s) => s,
        None => {
            return Err(PgError::error(
                "create_cast_seam: statement is not a CreateCastStmt",
            ))
        }
    };

    let sourcetype = match ccs.sourcetype.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };
    let targettype = match ccs.targettype.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };
    let func = match ccs.func.as_deref() {
        Some(n) => Some(Box::new(rich_node_to_parse(n)?)),
        None => None,
    };

    let pn = types_parsenodes::CreateCastStmt {
        sourcetype,
        targettype,
        func,
        context: ccs.context,
        inout: ccs.inout,
    };

    cast_transform_do::CreateCast(&pn)
}

#[allow(unused_imports)]
use backend_commands_functioncmds_seams as _functioncmds_seams_dep;
#[allow(unused_imports)]
use seam_core as _seam_core_dep;
#[allow(unused_imports)]
use types_nodes as _types_nodes_dep;
