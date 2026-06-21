//! `catalog/pg_proc.c` — routines to support manipulation of the `pg_proc`
//! relation. The per-catalog MUTATE-carrier owner (the pattern of
//! `catalog/pg_database.c` / `catalog/pg_type.c`): forms/deforms `pg_proc`
//! tuples and owns `ProcedureCreate`.
//!
//! Faithful 1:1 port of every C function: [`ProcedureCreate`],
//! [`fmgr_internal_validator`], [`fmgr_c_validator`], [`fmgr_sql_validator`],
//! [`function_parse_error_transpose`], [`oid_array_to_list`], and the statics
//! [`sql_function_parse_error_callback`] / `match_prosrc_to_query` /
//! `match_prosrc_to_literal` — original branch order, validation, error codes /
//! messages / SQLSTATE, lock levels, and dependency-recording order preserved.
//!
//! ## Shape of this port
//!
//!   * The decision logic — the parameter sanity check, the array deconstruct,
//!     the polymorphic / internal signature checks, the variadic-type loop, the
//!     full field-formation order, the replace-vs-insert branch, the
//!     dependency-recording sequence — runs in-crate over owned values.
//!   * The catalog crates below `pg_proc.c` are called **directly**:
//!     `new_object_addresses` / `add_exact_object_address` /
//!     `record_object_address_dependencies` / `recordDependencyOnExpr`
//!     (`dependency.c`), `deleteDependencyRecordsFor` (`pg_depend.c`),
//!     `recordDependencyOnOwner` (`pg_shdepend.c`),
//!     `invoke_object_post_create_hook` (`objectaccess.c`),
//!     `check_valid_polymorphic_signature` / `check_valid_internal_signature`
//!     (`parse_coerce.c`), and `table_open` / `Relation::close`.
//!   * The catalog-tuple value layer (`GetNewOidWithIndex` /
//!     `heap_form_tuple` / `heap_modify_tuple` / `CatalogTuple{Insert,Update}`)
//!     is owned by `catalog/indexing.c` and crosses through that owner's
//!     `-seams` crate (the typed `catalog_tuple_{insert,update}_pg_proc` rows).
//!     The `nodeToString` serialization (`outfuncs.c`), the
//!     `format_procedure` / `format_type_be` printers, the lsyscache helpers,
//!     the ACL default + new-ACL dependency recording, and
//!     `CommandCounterIncrement` cross their owners' seams.
//!   * The replace-path old-tuple probe + sub-checks and the three validators'
//!     bodies (which reach the unported syscache / funcapi / fmgr / dfmgr /
//!     parser / executor-functions / pgstat owners) cross
//!     [`backend_catalog_pg_proc_seams`]; each loud-panics until its owner
//!     lands (`mirror-pg-and-panic`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// The C declares locals up top and assigns later; keep that decl-then-assign
// shape so the port reads 1:1 against pg_proc.c.
#![allow(clippy::needless_late_init)]

mod fmgr_builtins;

use mcx::MemoryContext;

use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_catalog::pg_proc::{
    ProcFormFields, ProcedureRelationId, PgProcInsertRow,
    PROKIND_AGGREGATE, PROKIND_FUNCTION, PROKIND_PROCEDURE, PROKIND_WINDOW, PROARGMODE_IN,
    PROARGMODE_INOUT, PROARGMODE_OUT, PROARGMODE_TABLE, PROARGMODE_VARIADIC, SQLlanguageId,
};
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_storage::lock::RowExclusiveLock;
use types_tuple::heaptuple::{
    ANYARRAYOID, ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEMULTIRANGEOID, ANYCOMPATIBLENONARRAYOID,
    ANYCOMPATIBLEOID, ANYCOMPATIBLERANGEOID, ANYELEMENTOID, ANYENUMOID, ANYMULTIRANGEOID,
    ANYNONARRAYOID, ANYOID, ANYRANGEOID, RECORDOID, VOIDOID,
};
use types_catalog::pg_type::TYPTYPE_PSEUDO;

use backend_utils_error::ereport;
use types_error::{
    PgError, PgResult, ERRCODE_DUPLICATE_FUNCTION, ERRCODE_INVALID_FUNCTION_DEFINITION,
    ERRCODE_TOO_MANY_ARGUMENTS, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};

use backend_access_table_table::table_open;
use backend_catalog_dependency::{
    add_exact_object_address, new_object_addresses, record_object_address_dependencies,
};
use backend_catalog_objectaccess::invoke_object_post_create_hook;
use backend_catalog_pg_depend::deleteDependencyRecordsFor;
use backend_catalog_pg_shdepend::recordDependencyOnOwner;
use backend_parser_coerce::{check_valid_internal_signature, check_valid_polymorphic_signature};

use backend_access_transam_xact_seams as xact_seams;
use backend_catalog_aclchk_seams as aclchk_seams;
use backend_catalog_indexing_seams as indexing_seams;
use backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension;
use backend_catalog_pg_proc_seams as seam;
use backend_catalog_pg_proc_seams::{DefaultCompat, RecordTypeChange};
use backend_utils_adt_format_type_seams::format_type_be_str;
use backend_utils_adt_regproc_seams::format_procedure;
use backend_utils_cache_lsyscache_seams::{get_element_type, get_typtype};

use types_acl::ACLCHECK_NOT_OWNER;
use types_parsenodes::Node;

/// `FUNC_MAX_ARGS` (pg_config_manual.h:43).
const FUNC_MAX_ARGS: i32 = 100;

/// Relation OIDs the dependency records reference.
const NamespaceRelationId: Oid = types_catalog::catalog::NAMESPACE_RELATION_ID; // 2615
const LanguageRelationId: Oid = types_catalog::catalog::LANGUAGE_RELATION_ID; // 2612
const TypeRelationId: Oid = types_catalog::catalog::TYPE_RELATION_ID; // 1247
const TransformRelationId: Oid = types_catalog::catalog::TRANSFORM_RELATION_ID; // 3576

/// `OBJECT_FUNCTION` (parsenodes.h `ObjectType`).
const OBJECT_FUNCTION: types_nodes::parsenodes::ObjectType =
    types_nodes::parsenodes::OBJECT_FUNCTION;

/// `IsPolymorphicType(typid)` (catalog/pg_type.h:313): a pure OID comparison.
fn IsPolymorphicType(typid: Oid) -> bool {
    // IsPolymorphicTypeFamily1
    typid == ANYELEMENTOID
        || typid == ANYARRAYOID
        || typid == ANYNONARRAYOID
        || typid == ANYENUMOID
        || typid == ANYRANGEOID
        || typid == ANYMULTIRANGEOID
        // IsPolymorphicTypeFamily2
        || typid == ANYCOMPATIBLEOID
        || typid == ANYCOMPATIBLEARRAYOID
        || typid == ANYCOMPATIBLENONARRAYOID
        || typid == ANYCOMPATIBLERANGEOID
        || typid == ANYCOMPATIBLEMULTIRANGEOID
}

/// `ObjectAddressSet(object, classId, objectId)` (objectaddress.h): a fresh
/// `ObjectAddress` with `objectSubId == 0`.
fn object_address(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `elog(ERROR, msg)` — internal error (no SQLSTATE, message not translated).
fn elog_error(msg: impl Into<String>) -> PgError {
    ereport(ERROR).errmsg_internal(msg.into()).into_error()
}

/// `format_procedure(funcoid)` rendered to an owned `String` for an error hint.
fn format_procedure_owned(funcoid: Oid) -> PgResult<String> {
    let ctx = MemoryContext::new("format_procedure");
    let s = format_procedure::call(ctx.mcx(), funcoid)?;
    Ok(s.to_string())
}

/* ===========================================================================
 * ProcedureCreate (pg_proc.c:97-735)
 * ========================================================================= */

/// `ProcedureCreate(...)` (pg_proc.c:97). See pg_proc.c:60-95 for the full
/// argument documentation. Returns the new object's [`ObjectAddress`]
/// (`myself`).
///
/// The Datum array arguments are owned idiomatic types: `parameterTypes` is the
/// input-argument `oidvector` (`&[Oid]`); `allParameterTypes` / `parameterModes`
/// / `parameterNames` / `trftypes` / `proconfig` are `Option<Vec<…>>` (`None` ≡
/// the C `PointerGetDatum(NULL)`); `prosqlbody` is the cooked SQL-body already
/// serialized to its `pg_node_tree` text (`Option<String>`);
/// `parameterDefaults` is a `Vec<Node>` (empty ≡ `NIL`); `trfoids` a `Vec<Oid>`.
pub fn ProcedureCreate(
    procedureName: &str,
    procNamespace: Oid,
    replace: bool,
    returnsSet: bool,
    returnType: Oid,
    proowner: Oid,
    languageObjectId: Oid,
    languageValidator: Oid,
    prosrc: &str,
    probin: Option<&str>,
    prosqlbody: Option<String>,
    prosqlbody_refs: Vec<types_catalog::catalog_dependency::ObjectAddress>,
    prokind: i8,
    security_definer: bool,
    isLeakProof: bool,
    isStrict: bool,
    volatility: i8,
    parallel: i8,
    parameterTypes: &[Oid],
    allParameterTypes: Option<Vec<Oid>>,
    parameterModes: Option<Vec<i8>>,
    parameterNames: Option<Vec<Option<String>>>,
    parameterDefaults: Vec<Node>,
    trftypes: Option<Vec<Oid>>,
    trfoids: Vec<Oid>,
    proconfig: Option<Vec<String>>,
    prosupport: Oid,
    procost: f32,
    prorows: f32,
) -> PgResult<ObjectAddress> {
    let retval: Oid;
    let parameterCount: i32;
    let allParamCount: i32;
    let allParams: Vec<Oid>;
    // char *paramModes = NULL;
    let paramModes: Option<&[i8]> = parameterModes.as_deref();
    let mut variadicType: Oid = InvalidOid;
    let mut proacl_present: bool = false;
    let is_update: bool;
    let mut i: i32;

    /*
     * sanity checks
     */
    // Assert(PointerIsValid(prosrc)); — prosrc is a `&str`, always valid.

    parameterCount = parameterTypes.len() as i32;
    if !(0..=FUNC_MAX_ARGS).contains(&parameterCount) {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_TOO_MANY_ARGUMENTS)
            .errmsg_plural(
                format!("functions cannot have more than {FUNC_MAX_ARGS} argument"),
                format!("functions cannot have more than {FUNC_MAX_ARGS} arguments"),
                FUNC_MAX_ARGS as u64,
            )
            .into_error());
    }
    /* note: the above is correct, we do NOT count output arguments */

    /* Deconstruct array inputs */
    if let Some(ref v) = allParameterTypes {
        /*
         * We expect the array to be a 1-D OID array. The idiomatic caller hands
         * us the already-deconstructed `Vec`; the `ARR_*` validity is the
         * caller's responsibility (matching "we assume caller got the contents
         * right").
         */
        allParamCount = v.len() as i32;
        debug_assert!(allParamCount >= parameterCount);
        allParams = v.clone();
    } else {
        allParamCount = parameterCount;
        allParams = parameterTypes.to_vec();
    }

    /*
     * Do not allow polymorphic return type unless there is a polymorphic input
     * argument that we can use to deduce the actual return type.
     */
    let detailmsg = check_valid_polymorphic_signature(returnType, parameterTypes, parameterCount)?;
    if let Some(detailmsg) = detailmsg {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("cannot determine result data type")
            .errdetail_internal(detailmsg)
            .into_error());
    }

    /*
     * Also, do not allow return type INTERNAL unless at least one input
     * argument is INTERNAL.
     */
    let detailmsg = check_valid_internal_signature(returnType, parameterTypes, parameterCount);
    if let Some(detailmsg) = detailmsg {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg("unsafe use of pseudo-type \"internal\"")
            .errdetail_internal(detailmsg)
            .into_error());
    }

    /*
     * Apply the same tests to any OUT arguments.
     */
    if allParameterTypes.is_some() {
        i = 0;
        while i < allParamCount {
            let mode = paramModes.map(|m| m[i as usize]);
            if mode.is_none() || mode == Some(PROARGMODE_IN) || mode == Some(PROARGMODE_VARIADIC) {
                i += 1;
                continue; /* ignore input-only params */
            }

            let detailmsg =
                check_valid_polymorphic_signature(allParams[i as usize], parameterTypes, parameterCount)?;
            if let Some(detailmsg) = detailmsg {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("cannot determine result data type")
                    .errdetail_internal(detailmsg)
                    .into_error());
            }
            let detailmsg = check_valid_internal_signature(
                allParams[i as usize],
                parameterTypes,
                parameterCount,
            );
            if let Some(detailmsg) = detailmsg {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("unsafe use of pseudo-type \"internal\"")
                    .errdetail_internal(detailmsg)
                    .into_error());
            }
            i += 1;
        }
    }

    /* Identify variadic argument type, if any */
    if let Some(paramModes) = paramModes {
        /*
         * Only the last input parameter can be variadic; if it is, save its
         * element type. Errors here are just elog since caller should have
         * checked this already.
         */
        i = 0;
        while i < allParamCount {
            match paramModes[i as usize] {
                m if m == PROARGMODE_IN || m == PROARGMODE_INOUT => {
                    if OidIsValid(variadicType) {
                        return Err(elog_error("variadic parameter must be last"));
                    }
                }
                m if m == PROARGMODE_OUT => {
                    if OidIsValid(variadicType) && prokind == PROKIND_PROCEDURE {
                        return Err(elog_error("variadic parameter must be last"));
                    }
                }
                m if m == PROARGMODE_TABLE => { /* okay */ }
                m if m == PROARGMODE_VARIADIC => {
                    if OidIsValid(variadicType) {
                        return Err(elog_error("variadic parameter must be last"));
                    }
                    match allParams[i as usize] {
                        ANYOID => {
                            variadicType = ANYOID;
                        }
                        ANYARRAYOID => {
                            variadicType = ANYELEMENTOID;
                        }
                        ANYCOMPATIBLEARRAYOID => {
                            variadicType = ANYCOMPATIBLEOID;
                        }
                        other => {
                            variadicType = get_element_type::call(other)?.unwrap_or(InvalidOid);
                            if !OidIsValid(variadicType) {
                                return Err(elog_error("variadic parameter is not an array"));
                            }
                        }
                    }
                }
                other => {
                    return Err(elog_error(format!(
                        "invalid parameter mode '{}'",
                        other as u8 as char
                    )));
                }
            }
            i += 1;
        }
    }

    /*
     * All seems OK; prepare the data to be inserted into pg_proc.
     *
     * The full `values[]`/`nulls[]`/`replaces[]` assembly (pg_proc.c:320-380,
     * 580-585) lives inside the catalog-tuple seam, driven by the
     * `PgProcInsertRow` we build here. `pronargdefaults` = list_length.
     */
    let pronargdefaults_new: i32 = parameterDefaults.len() as i32;

    /* serialize the two pg_node_tree columns up front (nodeToString). The
     * cooked-tree serializer (outfuncs.c) crosses through the pg-proc seam since
     * `prosqlbody` / `parameterDefaults` carry the consumer's cooked
     * `types_parsenodes::Node` vocabulary. */
    let proargdefaults_text: Option<String> = if !parameterDefaults.is_empty() {
        Some(seam::node_to_string_defaults::call(parameterDefaults.clone())?)
    } else {
        None
    };
    /* `prosqlbody` already arrives serialized to its `pg_node_tree` text
     * (interpret_sql_body did the nodeToString in the parser-owning crate). */
    let prosqlbody_text: Option<String> = prosqlbody.clone();

    let ctx = MemoryContext::new("ProcedureCreate");
    let mcx = ctx.mcx();

    let rel = table_open(mcx, ProcedureRelationId, RowExclusiveLock)?;

    /* Check for pre-existing definition */
    let oldtup = seam::search_proc_name_args_nsp::call(mcx, procedureName, parameterTypes, procNamespace)?;

    if let Some((oldproc, old_formed)) = oldtup {
        /* There is one; okay to replace it? */
        let dropcmd: &str;

        if !replace {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_DUPLICATE_FUNCTION)
                .errmsg(format!(
                    "function \"{procedureName}\" already exists with same argument types"
                ))
                .into_error());
        }
        if !aclchk_seams::object_ownercheck::call(ProcedureRelationId, oldproc.oid, proowner)? {
            aclchk_seams::aclcheck_error::call(
                ACLCHECK_NOT_OWNER,
                OBJECT_FUNCTION,
                Some(procedureName.to_string()),
            )?;
        }

        /* Not okay to change routine kind */
        if oldproc.prokind != prokind {
            let mut b = ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg("cannot change routine kind");
            b = if oldproc.prokind == PROKIND_AGGREGATE {
                b.errdetail(format!("\"{procedureName}\" is an aggregate function."))
            } else if oldproc.prokind == PROKIND_FUNCTION {
                b.errdetail(format!("\"{procedureName}\" is a function."))
            } else if oldproc.prokind == PROKIND_PROCEDURE {
                b.errdetail(format!("\"{procedureName}\" is a procedure."))
            } else if oldproc.prokind == PROKIND_WINDOW {
                b.errdetail(format!("\"{procedureName}\" is a window function."))
            } else {
                b
            };
            return Err(b.into_error());
        }

        dropcmd = if prokind == PROKIND_PROCEDURE {
            "DROP PROCEDURE"
        } else if prokind == PROKIND_AGGREGATE {
            "DROP AGGREGATE"
        } else {
            "DROP FUNCTION"
        };

        /*
         * Not okay to change the return type of the existing proc, since
         * existing rules, views, etc may depend on the return type.
         *
         * In case of a procedure, a changing return type means that whether the
         * procedure has output parameters was changed. Since there is no user
         * visible return type, we produce a more specific error message.
         */
        if returnType != oldproc.prorettype || returnsSet != oldproc.proretset {
            let msg = if prokind == PROKIND_PROCEDURE {
                "cannot change whether a procedure has output parameters"
            } else {
                "cannot change return type of existing function"
            };
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                .errmsg(msg)
                .errhint(format!(
                    "Use {} {} first.",
                    dropcmd,
                    format_procedure_owned(oldproc.oid)?
                ))
                .into_error());
        }

        /*
         * If it returns RECORD, check for possible change of record type
         * implied by OUT parameters
         */
        if returnType == RECORDOID {
            let change = seam::record_type_change::call(
                oldproc.oid,
                prokind,
                allParameterTypes.clone(),
                parameterModes.clone(),
                parameterNames.clone(),
            )?;
            match change {
                RecordTypeChange::BothRuntime | RecordTypeChange::Equal => { /* ok */ }
                RecordTypeChange::Different => {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                        .errmsg("cannot change return type of existing function")
                        .errdetail("Row type defined by OUT parameters is different.")
                        .errhint(format!(
                            "Use {} {} first.",
                            dropcmd,
                            format_procedure_owned(oldproc.oid)?
                        ))
                        .into_error());
                }
            }
        }

        /*
         * If there were any named input parameters, check to make sure the
         * names have not been changed, as this could break existing calls. We
         * allow adding names to formerly unnamed parameters, though.
         */
        if oldproc.proargnames.is_some() {
            if let Some(old_name) = seam::check_input_param_names_unchanged::call(
                oldproc.proargnames.clone(),
                oldproc.proargmodes.clone(),
                parameterNames.clone(),
                parameterModes.clone(),
            )? {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "cannot change name of input parameter \"{old_name}\""
                    ))
                    .errhint(format!(
                        "Use {} {} first.",
                        dropcmd,
                        format_procedure_owned(oldproc.oid)?
                    ))
                    .into_error());
            }
        }

        /*
         * If there are existing defaults, check compatibility: redefinition must
         * not remove any defaults nor change their types.
         */
        if oldproc.pronargdefaults != 0 {
            if pronargdefaults_new < oldproc.pronargdefaults as i32 {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("cannot remove parameter defaults from existing function")
                    .errhint(format!(
                        "Use {} {} first.",
                        dropcmd,
                        format_procedure_owned(oldproc.oid)?
                    ))
                    .into_error());
            }

            /* old proargdefaults is non-NULL here (pronargdefaults != 0). */
            let old_defaults_text = oldproc
                .proargdefaults
                .clone()
                .ok_or_else(|| elog_error("missing proargdefaults in existing function"))?;
            /* the new defaults cross as cooked nodes so the seam can exprType()
             * both sides (the old side is stringToNode'd from its text). */
            let compat = seam::check_defaults_compatible::call(
                old_defaults_text,
                oldproc.pronargdefaults,
                parameterDefaults.clone(),
            )?;
            if compat == DefaultCompat::TypeChanged {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg("cannot change data type of existing parameter default value")
                    .errhint(format!(
                        "Use {} {} first.",
                        dropcmd,
                        format_procedure_owned(oldproc.oid)?
                    ))
                    .into_error());
            }
        }

        /*
         * Do not change existing oid, ownership or permissions, either. (The
         * `replaces[oid/proowner/proacl] = false` clearing happens inside
         * `catalog_tuple_update_pg_proc`, pg_proc.c:580-585.)
         *
         * Okay, do it...
         */
        let row = build_insert_row(
            procedureName,
            procNamespace,
            proowner,
            languageObjectId,
            procost,
            prorows,
            variadicType,
            prosupport,
            prokind,
            security_definer,
            isLeakProof,
            isStrict,
            returnsSet,
            volatility,
            parallel,
            parameterCount,
            pronargdefaults_new,
            returnType,
            parameterTypes,
            &allParameterTypes,
            &parameterModes,
            &parameterNames,
            &proargdefaults_text,
            &trftypes,
            prosrc,
            probin,
            &prosqlbody_text,
            &proconfig,
            oldproc.oid, /* keep existing oid (replaces[oid] = false) */
            None,        /* proacl unchanged on replace */
        );
        indexing_seams::catalog_tuple_update_pg_proc::call(mcx, &rel, &old_formed, &row)?;

        retval = oldproc.oid;
        is_update = true;
    } else {
        /* Creating a new procedure */

        /* First, get default permissions and set up proacl */
        let proacl = aclchk_seams::get_user_default_acl::call(
            mcx,
            OBJECT_FUNCTION,
            proowner,
            procNamespace,
        )?;
        proacl_present = proacl.is_some();
        let proacl_bytes: Option<Vec<u8>> = match &proacl {
            Some(types_tuple::backend_access_common_heaptuple::Datum::ByRef(b)) => {
                Some(b[..].to_vec())
            }
            _ => None,
        };

        let newOid = indexing_seams::get_new_oid_with_index_pg_proc::call(&rel)?;
        let row = build_insert_row(
            procedureName,
            procNamespace,
            proowner,
            languageObjectId,
            procost,
            prorows,
            variadicType,
            prosupport,
            prokind,
            security_definer,
            isLeakProof,
            isStrict,
            returnsSet,
            volatility,
            parallel,
            parameterCount,
            pronargdefaults_new,
            returnType,
            parameterTypes,
            &allParameterTypes,
            &parameterModes,
            &parameterNames,
            &proargdefaults_text,
            &trftypes,
            prosrc,
            probin,
            &prosqlbody_text,
            &proconfig,
            newOid,
            proacl_bytes,
        );
        indexing_seams::catalog_tuple_insert_pg_proc::call(mcx, &rel, &row)?;

        retval = newOid;
        is_update = false;
    }

    /*
     * Create dependencies for the new function. If we are updating an existing
     * function, first delete any existing pg_depend entries. (However, since we
     * are not changing ownership or permissions, the shared dependencies do
     * *not* need to change, and we leave them alone.)
     */
    if is_update {
        deleteDependencyRecordsFor(ProcedureRelationId, retval, true)?;
    }

    let mut addrs = new_object_addresses();

    let myself = object_address(ProcedureRelationId, retval);

    /* dependency on namespace */
    add_exact_object_address(&object_address(NamespaceRelationId, procNamespace), &mut addrs);

    /* dependency on implementation language */
    add_exact_object_address(&object_address(LanguageRelationId, languageObjectId), &mut addrs);

    /* dependency on return type */
    add_exact_object_address(&object_address(TypeRelationId, returnType), &mut addrs);

    /* dependency on parameter types */
    i = 0;
    while i < allParamCount {
        add_exact_object_address(
            &object_address(TypeRelationId, allParams[i as usize]),
            &mut addrs,
        );
        i += 1;
    }

    /* dependency on transforms, if any */
    for transformid in trfoids.iter().copied() {
        add_exact_object_address(&object_address(TransformRelationId, transformid), &mut addrs);
    }

    /* dependency on support function, if any */
    if OidIsValid(prosupport) {
        add_exact_object_address(&object_address(ProcedureRelationId, prosupport), &mut addrs);
    }

    record_object_address_dependencies(&myself, &mut addrs, DEPENDENCY_NORMAL)?;
    /* free_object_addresses(addrs) — the owned ObjectAddresses drops here. */
    drop(addrs);

    /* dependency on SQL routine body */
    if languageObjectId == SQLlanguageId && !prosqlbody_refs.is_empty() {
        /* recordDependencyOnExpr(&myself, prosqlbody, NIL, DEPENDENCY_NORMAL):
         * the body's object references were extracted from the *in-memory*
         * cooked node by `interpret_sql_body` (so we never have to round-trip
         * the stored text back through `stringToNode`). Record them against the
         * new function exactly as `recordDependencyOnExpr` would. */
        let mut body_addrs = new_object_addresses();
        for r in &prosqlbody_refs {
            add_exact_object_address(r, &mut body_addrs);
        }
        record_object_address_dependencies(&myself, &mut body_addrs, DEPENDENCY_NORMAL)?;
    }

    /* dependency on parameter default expressions */
    if !parameterDefaults.is_empty() {
        /* recordDependencyOnExpr(&myself, (Node *) parameterDefaults, NIL,
         * DEPENDENCY_NORMAL) over the cooked default-expr list. */
        seam::record_dependency_on_defaults::call(retval, parameterDefaults.clone())?;
    }

    /* dependency on owner */
    if !is_update {
        recordDependencyOnOwner(ProcedureRelationId, retval, proowner)?;
    }

    /* dependency on any roles mentioned in ACL */
    if !is_update {
        /* The default ACL crossed into the insert seam; here we record its
         * role dependencies. We re-derive the default ACL the same way the
         * insert path did (get_user_default_acl is deterministic for the same
         * inputs), matching the C, which passes the same `proacl` pointer. */
        let proacl = if proacl_present {
            aclchk_seams::get_user_default_acl::call(mcx, OBJECT_FUNCTION, proowner, procNamespace)?
        } else {
            None
        };
        aclchk_seams::record_dependency_on_new_acl::call(
            mcx,
            ProcedureRelationId,
            retval,
            0,
            proowner,
            proacl,
        )?;
    }

    /* dependency on extension */
    recordDependencyOnCurrentExtension::call(mcx, &myself, is_update)?;

    /* Post creation hook for new function */
    invoke_object_post_create_hook(ProcedureRelationId, retval, 0, false)?;

    rel.close(RowExclusiveLock)?;

    /* Verify function body */
    if OidIsValid(languageValidator) {
        /* Advance command counter so new tuple can be seen by validator */
        xact_seams::command_counter_increment::call()?;

        /*
         * Set per-function configuration parameters so that the validation is
         * done with the environment the function expects (gated on
         * check_function_bodies), then OidFunctionCall1(languageValidator,
         * ObjectIdGetDatum(retval)) wrapped in the GUC nest level
         * (pg_proc.c:700-728). The GUC nest-level lifetime must wrap the
         * validator dispatch, so the whole dance is encapsulated in the seam.
         */
        seam::run_language_validator::call(languageValidator, retval, proconfig)?;
    }

    /* ensure that stats are dropped if transaction aborts */
    if !is_update {
        seam::pgstat_create_function::call(retval)?;
    }

    Ok(myself)
}

/// Assemble the [`PgProcInsertRow`] for the catalog-tuple seam from the
/// decisions `ProcedureCreate` made. Mirrors the C `values[]`/`nulls[]`
/// field-formation order (pg_proc.c:327-379).
fn build_insert_row(
    procedure_name: &str,
    proc_namespace: Oid,
    proowner: Oid,
    language_object_id: Oid,
    procost: f32,
    prorows: f32,
    variadic_type: Oid,
    prosupport: Oid,
    prokind: i8,
    security_definer: bool,
    is_leak_proof: bool,
    is_strict: bool,
    returns_set: bool,
    volatility: i8,
    parallel: i8,
    parameter_count: i32,
    pronargdefaults: i32,
    return_type: Oid,
    parameter_types: &[Oid],
    all_parameter_types: &Option<Vec<Oid>>,
    parameter_modes: &Option<Vec<i8>>,
    parameter_names: &Option<Vec<Option<String>>>,
    proargdefaults_text: &Option<String>,
    trftypes: &Option<Vec<Oid>>,
    prosrc: &str,
    probin: Option<&str>,
    prosqlbody_text: &Option<String>,
    proconfig: &Option<Vec<String>>,
    oid: Oid,
    proacl: Option<Vec<u8>>,
) -> PgProcInsertRow {
    PgProcInsertRow {
        fields: ProcFormFields {
            oid,
            proname: namestrcpy(procedure_name),
            pronamespace: proc_namespace,
            proowner,
            prolang: language_object_id,
            procost,
            prorows,
            provariadic: variadic_type,
            prosupport,
            prokind,
            prosecdef: security_definer,
            proleakproof: is_leak_proof,
            proisstrict: is_strict,
            proretset: returns_set,
            provolatile: volatility,
            proparallel: parallel,
            pronargs: parameter_count as i16,
            pronargdefaults: pronargdefaults as i16,
            prorettype: return_type,
        },
        proargtypes: parameter_types.to_vec(),
        proallargtypes: all_parameter_types.clone(),
        proargmodes: parameter_modes.clone(),
        proargnames: parameter_names.clone(),
        proargdefaults: proargdefaults_text.clone(),
        protrftypes: trftypes.clone(),
        prosrc: prosrc.to_string(),
        probin: probin.map(|s| s.to_string()),
        prosqlbody: prosqlbody_text.clone(),
        proconfig: proconfig.clone(),
        proacl,
    }
}

/* ===========================================================================
 * fmgr_internal_validator (pg_proc.c:739-777)
 * ========================================================================= */

/// Validator for internal functions. Check that the given internal function
/// name (the "prosrc" value) is a known builtin function.
///
/// `validator_fn_oid` is the validator function's own OID
/// (`fcinfo->flinfo->fn_oid`), needed by `CheckFunctionValidatorAccess`.
pub fn fmgr_internal_validator(validator_fn_oid: Oid, funcoid: Oid) -> PgResult<()> {
    if !seam::check_function_validator_access::call(validator_fn_oid, funcoid)? {
        return Ok(()); /* PG_RETURN_VOID() */
    }

    /*
     * We do not honor check_function_bodies since it's unlikely the function
     * name will be found later if it isn't there now. The syscache `prosrc`
     * read, the `fmgr_internal_function(prosrc) == InvalidOid` test, and the
     * `there is no built-in function named "%s"` error (pg_proc.c:761-774) live
     * in the seam (fmgr value layer).
     */
    seam::validate_internal_function::call(funcoid)
}

/* ===========================================================================
 * fmgr_c_validator (pg_proc.c:781-823)
 * ========================================================================= */

/// Validator for C language functions. Make sure that the library file exists,
/// is loadable, and contains the specified link symbol.
pub fn fmgr_c_validator(validator_fn_oid: Oid, funcoid: Oid) -> PgResult<()> {
    if !seam::check_function_validator_access::call(validator_fn_oid, funcoid)? {
        return Ok(()); /* PG_RETURN_VOID() */
    }

    /*
     * The syscache `prosrc`/`probin` read + `load_external_function` +
     * `fetch_finfo_record` (pg_proc.c:807-820) live in the seam.
     */
    seam::validate_c_function::call(funcoid)
}

/* ===========================================================================
 * fmgr_sql_validator (pg_proc.c:826-993)
 * ========================================================================= */

/// Validator for SQL language functions. Parse it here in order to be sure that
/// it contains no syntax errors.
pub fn fmgr_sql_validator(validator_fn_oid: Oid, funcoid: Oid) -> PgResult<()> {
    let mut haspolyarg: bool;
    let mut i: i32;

    if !seam::check_function_validator_access::call(validator_fn_oid, funcoid)? {
        return Ok(()); /* PG_RETURN_VOID() */
    }

    /*
     * SearchSysCache1(PROCOID, ...) + GETSTRUCT reads. Raises the "cache lookup
     * failed for function %u" elog inside the seam helper when absent.
     */
    let proc = seam::search_proc_oid_sql::call(funcoid)?.ok_or_else(|| {
        elog_error(format!("cache lookup failed for function {funcoid}"))
    })?;

    /* Disallow pseudotype result */
    /* except for RECORD, VOID, or polymorphic */
    if get_typtype::call(proc.prorettype)? == TYPTYPE_PSEUDO as u8
        && proc.prorettype != RECORDOID
        && proc.prorettype != VOIDOID
        && !IsPolymorphicType(proc.prorettype)
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
            .errmsg(format!(
                "SQL functions cannot return type {}",
                format_type_be_str::call(proc.prorettype)?
            ))
            .into_error());
    }

    /* Disallow pseudotypes in arguments */
    /* except for polymorphic */
    haspolyarg = false;
    i = 0;
    while i < proc.pronargs as i32 {
        let argtype = proc.proargtypes[i as usize];
        if get_typtype::call(argtype)? == TYPTYPE_PSEUDO as u8 {
            if IsPolymorphicType(argtype) {
                haspolyarg = true;
            } else {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_FUNCTION_DEFINITION)
                    .errmsg(format!(
                        "SQL functions cannot have arguments of type {}",
                        format_type_be_str::call(argtype)?
                    ))
                    .into_error());
            }
        }
        i += 1;
    }

    /* Postpone body checks if !check_function_bodies */
    if seam::check_function_bodies::call()? {
        /*
         * The body-check (pg_proc.c:884-988): prosrc/prosqlbody read, the
         * `sql_function_parse_error_callback` error-context wiring, the
         * `pg_parse_query`/`AcquireRewriteLocks`/`pg_rewrite_query` /
         * `pg_analyze_and_rewrite_withcb` re-parse, and `check_sql_fn_statements`
         * / `check_sql_fn_retval`. Encapsulated in the seam because the
         * `error_context_stack` push/pop (referencing the in-crate
         * `sql_function_parse_error_callback`) must wrap the cross-crate parse,
         * and the `haspolyarg` gating + the rettype/rettupdesc check live with
         * it.
         */
        let _ = haspolyarg;
        seam::run_sql_function_body_check::call(funcoid)?;
    }

    Ok(())
}

/* ===========================================================================
 * sql_function_parse_error_callback (pg_proc.c:995-1009)
 * ========================================================================= */

/// Error context callback for handling errors in SQL function definitions.
pub fn sql_function_parse_error_callback(proname: &str, prosrc: &str) -> PgResult<()> {
    /* See if it's a syntax error; if so, transpose to CREATE FUNCTION */
    if !function_parse_error_transpose(prosrc)? {
        /* If it's not a syntax error, push info onto context stack */
        seam::errcontext_sql_function::call(proname.to_string())?;
    }
    Ok(())
}

/* ===========================================================================
 * function_parse_error_transpose (pg_proc.c:1011-1081)
 * ========================================================================= */

/// Adjust a syntax error occurring inside the function body of a CREATE
/// FUNCTION or DO command. Returns true if a syntax error was processed.
pub fn function_parse_error_transpose(prosrc: &str) -> PgResult<bool> {
    let mut origerrposition: i32;
    let newerrposition: i32;

    /*
     * Nothing to do unless we are dealing with a syntax error that has a cursor
     * position. Some PLs may prefer to report the error position as an internal
     * error to begin with, so check that too.
     */
    origerrposition = seam::geterrposition::call()?;
    if origerrposition <= 0 {
        origerrposition = seam::getinternalerrposition::call()?;
        if origerrposition <= 0 {
            return Ok(false);
        }
    }

    /* We can get the original query text from the active portal (hack...) */
    if let Some(queryText) = seam::active_portal_source_text::call()? {
        /* Try to locate the prosrc in the original text */
        newerrposition = match_prosrc_to_query(prosrc, &queryText, origerrposition)?;
    } else {
        /*
         * Quietly give up if no ActivePortal. This is an unusual situation but
         * it can happen in, e.g., logical replication workers.
         */
        newerrposition = -1;
    }

    if newerrposition > 0 {
        /* Successful, so fix error position to reference original query */
        seam::errposition::call(newerrposition)?;
        /* Get rid of any report of the error as an "internal query" */
        seam::internalerrposition::call(0)?;
        seam::internalerrquery::call(None)?;
    } else {
        /*
         * If unsuccessful, convert the position to an internal position marker
         * and give the function text as the internal query.
         */
        seam::errposition::call(0)?;
        seam::internalerrposition::call(origerrposition)?;
        seam::internalerrquery::call(Some(prosrc.to_string()))?;
    }

    Ok(true)
}

/// Value-form of [`function_parse_error_transpose`] for the PL/pgSQL compile
/// path, where the syntax error is carried as a `PgError` value (the SDK's
/// `PgResult` error model) rather than being live on the ereport stack.
///
/// Mirrors `function_parse_error_transpose` exactly, but reads/writes the
/// error's cursor/internal position and internal query directly on the value.
/// Returns the (possibly adjusted) error; when there is no cursor/internal
/// position the error is returned unchanged (the `return false` C path leaves
/// the error untouched).
pub fn function_parse_error_transpose_value(prosrc: &str, mut err: PgError) -> PgResult<PgError> {
    /*
     * Nothing to do unless we are dealing with a syntax error that has a cursor
     * position. Some PLs may prefer to report the error position as an internal
     * error to begin with, so check that too.
     */
    let mut origerrposition = err.cursor_position.unwrap_or(0);
    if origerrposition <= 0 {
        origerrposition = err.internal_position.unwrap_or(0);
        if origerrposition <= 0 {
            return Ok(err);
        }
    }

    /* We can get the original query text from the active portal (hack...) */
    let newerrposition = if let Some(queryText) = seam::active_portal_source_text::call()? {
        match_prosrc_to_query(prosrc, &queryText, origerrposition)?
    } else {
        /*
         * Quietly give up if no ActivePortal. This is an unusual situation but
         * it can happen in, e.g., logical replication workers.
         */
        -1
    };

    if newerrposition > 0 {
        /* Successful, so fix error position to reference original query */
        err.cursor_position = Some(newerrposition);
        /* Get rid of any report of the error as an "internal query" */
        err.internal_position = None;
        err.internal_query = None;
    } else {
        /*
         * If unsuccessful, convert the position to an internal position marker
         * and give the function text as the internal query.
         */
        err.cursor_position = None;
        err.internal_position = Some(origerrposition);
        err.internal_query = Some(prosrc.to_string());
    }

    Ok(err)
}

/* ===========================================================================
 * match_prosrc_to_query (pg_proc.c:1083-1137)
 * ========================================================================= */

/// Try to locate the string literal containing the function body in the given
/// text of the CREATE FUNCTION or DO command. If successful, return the
/// character (not byte) index within the command corresponding to the given
/// character index within the literal. If not successful, return 0.
pub fn match_prosrc_to_query(prosrc: &str, queryText: &str, cursorpos: i32) -> PgResult<i32> {
    /*
     * Rather than fully parsing the original command, we just scan the command
     * looking for $prosrc$ or 'prosrc'. This could be fooled, so fail if we
     * find more than one match.
     */
    let prosrc_bytes = prosrc.as_bytes();
    let prosrclen = prosrc_bytes.len() as i32;
    let query_bytes = queryText.as_bytes();
    let querylen = query_bytes.len() as i32;
    let mut matchpos: i32 = 0;
    let mut curpos: i32;
    let mut newcursorpos: i32 = 0;

    curpos = 0;
    while curpos < querylen - prosrclen {
        let cp = curpos as usize;
        if query_bytes[cp] == b'$'
            && strncmp_bytes(prosrc_bytes, &query_bytes[cp + 1..], prosrclen as usize) == 0
            // C reads queryText[curpos+1+prosrclen], the NUL terminator at the
            // loop's last iteration; mirror that NUL-safe read.
            && byte_at_or_nul(query_bytes, cp + 1 + prosrclen as usize) == b'$'
        {
            /*
             * Found a $foo$ match. Since there are no embedded quoting
             * characters in a dollar-quoted literal, we just offset by the
             * starting position.
             */
            if matchpos != 0 {
                return Ok(0); /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(query_bytes, curpos + 1)? + cursorpos;
        } else if query_bytes[cp] == b'\''
            && match_prosrc_to_literal(prosrc, &query_bytes[cp + 1..], cursorpos, &mut newcursorpos)?
        {
            /*
             * Found a 'foo' match. match_prosrc_to_literal() has adjusted for
             * any quotes or backslashes embedded in the literal.
             */
            if matchpos != 0 {
                return Ok(0); /* multiple matches, fail */
            }
            matchpos = pg_mbstrlen_with_len(query_bytes, curpos + 1)? + newcursorpos;
        }
        curpos += 1;
    }

    Ok(matchpos)
}

/* ===========================================================================
 * match_prosrc_to_literal (pg_proc.c:1139-1202)
 * ========================================================================= */

/// Try to match the given source text to a single-quoted literal. If
/// successful, adjust newcursorpos to correspond to the character (not byte)
/// index corresponding to cursorpos in the source text.
///
/// At entry, `literal` points just past a `'` character.
fn match_prosrc_to_literal(
    prosrc: &str,
    literal: &[u8],
    mut cursorpos: i32,
    newcursorpos: &mut i32,
) -> PgResult<bool> {
    let mut newcp = cursorpos;
    let mut chlen: i32;

    /*
     * This implementation handles backslashes and doubled quotes in the string
     * literal. We do the comparison a character at a time, not a byte at a
     * time, so that we can do the correct cursorpos math.
     */
    let prosrc_bytes = prosrc.as_bytes();
    let mut p: usize = 0; /* index into prosrc_bytes (the `*prosrc` cursor) */
    let mut l: usize = 0; /* index into literal (the `literal` cursor) */

    'outer: {
        while p < prosrc_bytes.len() {
            cursorpos -= 1; /* characters left before cursor */

            /*
             * Check for backslashes and doubled quotes in the literal; adjust
             * newcp when one is found before the cursor.
             */
            if l < literal.len() && literal[l] == b'\\' {
                l += 1;
                if cursorpos > 0 {
                    newcp += 1;
                }
            } else if l < literal.len() && literal[l] == b'\'' {
                if !(l + 1 < literal.len() && literal[l + 1] == b'\'') {
                    break 'outer; /* goto fail */
                }
                l += 1;
                if cursorpos > 0 {
                    newcp += 1;
                }
            }
            chlen = pg_mblen(&prosrc_bytes[p..])?;
            let n = chlen as usize;
            if strncmp_bytes(&prosrc_bytes[p..], literal_from(literal, l), n) != 0 {
                break 'outer; /* goto fail */
            }
            p += n;
            l += n;
        }

        /* Reached end of prosrc: check the trailing quote. */
        if l < literal.len()
            && literal[l] == b'\''
            && !(l + 1 < literal.len() && literal[l + 1] == b'\'')
        {
            /* success */
            *newcursorpos = newcp;
            return Ok(true);
        }
        /* fall through to fail */
    }

    /* fail: Must set *newcursorpos to suppress compiler warning */
    *newcursorpos = newcp;
    Ok(false)
}

/* ===========================================================================
 * oid_array_to_list (pg_proc.c:1204-1217)
 * ========================================================================= */

/// `oid_array_to_list(datum)` (pg_proc.c:1204): `deconstruct_array_builtin(array,
/// OIDOID, ...)` then collect the elements into a `Vec<Oid>` (the idiomatic
/// owned-list analog of the C `List *`). The input `datum` is the OID array's
/// detoasted on-disk image (the caller passes the array bytes).
pub fn oid_array_to_list<'mcx>(mcx: mcx::Mcx<'mcx>, datum: &[u8]) -> PgResult<Vec<Oid>> {
    // deconstruct_array_builtin(array, OIDOID, &values, NULL, &nelems);
    let pairs = backend_utils_adt_arrayfuncs::construct::deconstruct_array_builtin(
        mcx,
        datum,
        26, /* OIDOID */
    )?;
    let mut result: Vec<Oid> = Vec::new(); /* NIL */
    for (d, _isnull) in pairs.iter() {
        // result = lappend_oid(result, values[i]);
        result.push(d.as_oid());
    }
    Ok(result)
}

/* ===========================================================================
 * Small in-crate helpers (name copy, node serialization, C-string byte ops).
 * ========================================================================= */

/// `namestrcpy(&procname, procedureName)` (pg_proc.c:327): the name truncated to
/// `NAMEDATALEN - 1` bytes (the catalog-tuple seam frames it into the on-disk
/// `NameData`). `namestrcpy` enforces the truncation in C.
fn namestrcpy(name: &str) -> String {
    const NAMEDATALEN: usize = 64;
    let limit = NAMEDATALEN - 1;
    let take = limit.min(name.len());
    let mut end = take;
    while end > 0 && !name.is_char_boundary(end) {
        end -= 1;
    }
    name[..end].to_string()
}

/// `strncmp(a, b, n)` over byte slices, where `a` is at least `n` bytes
/// (mirrors the C semantics; only equality vs. inequality matters at the call
/// sites).
fn strncmp_bytes(a: &[u8], b: &[u8], n: usize) -> i32 {
    let alen = a.len().min(n);
    let blen = b.len().min(n);
    let m = alen.min(blen);
    for k in 0..m {
        if a[k] != b[k] {
            return a[k] as i32 - b[k] as i32;
        }
    }
    /* prefix equal; if one side ran short of n, treat as inequality */
    if alen != n || blen != n {
        return alen as i32 - blen as i32;
    }
    0
}

/// Read `bytes[i]`, or `0` (the C NUL terminator) if `i` is at/past the end.
fn byte_at_or_nul(bytes: &[u8], i: usize) -> u8 {
    if i < bytes.len() {
        bytes[i]
    } else {
        0
    }
}

/// `&literal[l]`, or an empty slice if `l` is at/after the end.
fn literal_from(literal: &[u8], l: usize) -> &[u8] {
    if l >= literal.len() {
        &[]
    } else {
        &literal[l..]
    }
}

/// `pg_mbstrlen_with_len(s, len)` (pg_proc.c:1118,1131).
fn pg_mbstrlen_with_len(s: &[u8], len: i32) -> PgResult<i32> {
    backend_utils_mb_mbutils_seams::pg_mbstrlen_with_len::call(s, len)
}

/// `pg_mblen(s)` (pg_proc.c:1184, `pg_mblen_cstr`) — the slice-bounded variant.
fn pg_mblen(s: &[u8]) -> PgResult<i32> {
    backend_utils_mb_mbutils_seams::pg_mblen_range::call(s)
}

/// Install this unit's outward consumer seam: `functioncmds.c` reaches
/// `ProcedureCreate` through `backend-commands-functioncmds-seams`'s
/// `procedure_create`. pg_proc.c owns the C function, so it installs the seam.
pub fn init_seams() {
    backend_commands_functioncmds_seams::procedure_create::set(procedure_create_from_args);
    fmgr_builtins::register_pg_proc_builtins();

    // Value-form `function_parse_error_transpose` for the PL/pgSQL compile path
    // (pl_comp.c `plpgsql_compile_error_callback`). The body + active-portal
    // text reader live here; the comp crate calls it through this comp-seam.
    backend_pl_plpgsql_comp_seams::function_parse_error_transpose::set(|prosrc, err| {
        function_parse_error_transpose_value(prosrc, err)
    });
}

/// Adapt the `functioncmds.c` `ProcedureCreateArgs` bundle to the positional
/// `ProcedureCreate` call.
fn procedure_create_from_args(
    args: backend_commands_functioncmds_seams::ProcedureCreateArgs,
) -> PgResult<ObjectAddress> {
    let backend_commands_functioncmds_seams::ProcedureCreateArgs {
        procedure_name,
        namespace_id,
        replace,
        returns_set,
        prorettype,
        proowner,
        language_oid,
        language_validator,
        prosrc,
        probin,
        prosqlbody,
        prosqlbody_refs,
        prokind,
        security,
        is_leak_proof,
        is_strict,
        volatility,
        parallel,
        parameter_types,
        all_parameter_types,
        parameter_modes,
        parameter_names,
        parameter_defaults,
        trftypes,
        trfoids,
        proconfig,
        prosupport,
        procost,
        prorows,
    } = args;

    ProcedureCreate(
        &procedure_name,
        namespace_id,
        replace,
        returns_set,
        prorettype,
        proowner,
        language_oid,
        language_validator,
        &prosrc,
        probin.as_deref(),
        prosqlbody,
        prosqlbody_refs,
        prokind,
        security,
        is_leak_proof,
        is_strict,
        volatility,
        parallel,
        &parameter_types,
        all_parameter_types,
        parameter_modes,
        parameter_names,
        parameter_defaults,
        trftypes,
        trfoids,
        proconfig,
        prosupport,
        procost,
        prorows,
    )
}
