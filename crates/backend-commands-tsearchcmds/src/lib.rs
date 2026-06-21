#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

//! `backend/commands/tsearchcmds.c` — `CREATE`/`ALTER`/`DROP` TEXT SEARCH
//! PARSER / DICTIONARY / TEMPLATE / CONFIGURATION DDL.
//!
//! Every command driver and static helper is ported branch-for-branch against
//! the owned `'mcx` node tree, with byte-identical SQLSTATEs and messages. The
//! C threads opened `Relation`/`HeapTuple` handles between the catalog insert,
//! the `GETSTRUCT` form read, and the dependency builders; the owned model does
//! not model those handles. Each catalog read/write crosses a self-contained
//! seam in [`backend_commands_tsearchcmds_seams`] that returns the relevant
//! row columns as an owned snapshot (`TS*Form`) or the per-map rows as
//! `ConfigMapEntry`. The dependency-set *assembly*, the option-list parsing,
//! the (de)serialization state machine, the mapping ADD/ALTER/DROP/REPLACE
//! orchestration, and `tstoken_list_member` are all in-crate.
//!
//! Note (PG 18.3): there are no `RemoveTSParserById` / `RemoveTSDictionaryById`
//! / `RemoveTSTemplateById` — generic dependency.c deletion handles those
//! classes; only [`RemoveTSConfigurationById`] survives because it must also
//! clear `pg_ts_config_map`.

extern crate alloc;

use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_catalog_namespace::{
    get_ts_config_oid, get_ts_dict_oid, get_ts_parser_oid, get_ts_template_oid, NameListToString,
    QualifiedNameGetCreationNamespace,
};
use backend_utils_error::ereport;

use mcx::{Mcx, MemoryContext, PgString};

use types_acl::{AclResult, ACL_CREATE, ACLCHECK_NOT_OWNER, ACLCHECK_OK};
use types_catalog::catalog::{
    NAMESPACE_RELATION_ID, PROCEDURE_RELATION_ID, TS_CONFIG_RELATION_ID, TS_DICTIONARY_RELATION_ID,
    TS_PARSER_RELATION_ID, TS_TEMPLATE_RELATION_ID,
};
use types_catalog::catalog_dependency::{ObjectAddress, DEPENDENCY_NORMAL};
use types_core::{InvalidOid, Oid, OidIsValid};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
    ERRCODE_UNDEFINED_OBJECT, ERROR, NOTICE,
};
use types_nodes::nodes::{ntag, Node, NodePtr};
use types_nodes::value::{Boolean, Float, Integer, StringNode};
use types_nodes::ddlnodes::{
    AlterTSConfigurationStmt, AlterTSDictionaryStmt, DefElem, DEFELEM_UNSPEC,
};
use types_nodes::parsenodes::{OBJECT_TSCONFIGURATION, OBJECT_TSDICTIONARY};
use types_tuple::heaptuple::{INT4OID, INTERNALOID, TSQUERYOID, VOIDOID};

use backend_commands_tsearchcmds_seams as seam;
use backend_commands_tsearchcmds_seams::{
    ConfigMapEntry, LexDescr, NewTSParser, NewTSTemplate, TSConfigForm, TSDictForm, TSParserForm,
    TSTemplateForm,
};

use types_cache::DefElemString;

/* C-spelling relation-id aliases used throughout (catalog/pg_*.h). */
const TSParserRelationId: Oid = TS_PARSER_RELATION_ID;
const TSDictionaryRelationId: Oid = TS_DICTIONARY_RELATION_ID;
const TSTemplateRelationId: Oid = TS_TEMPLATE_RELATION_ID;
const TSConfigRelationId: Oid = TS_CONFIG_RELATION_ID;
const NamespaceRelationId: Oid = NAMESPACE_RELATION_ID;
const ProcedureRelationId: Oid = PROCEDURE_RELATION_ID;

/* pg_ts_parser attribute numbers (1-based, catalog/pg_ts_parser.h order). */
const Anum_pg_ts_parser_prsstart: i32 = 4;
const Anum_pg_ts_parser_prstoken: i32 = 5;
const Anum_pg_ts_parser_prsend: i32 = 6;
const Anum_pg_ts_parser_prsheadline: i32 = 7;
const Anum_pg_ts_parser_prslextype: i32 = 8;

/* pg_ts_template attribute numbers (1-based, catalog/pg_ts_template.h order). */
const Anum_pg_ts_template_tmplinit: i32 = 4;
const Anum_pg_ts_template_tmpllexize: i32 = 5;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/tsearchcmds.c", 0, funcname)
}

/* =========================================================================
 * TS Parser commands
 * ========================================================================= */

/// `get_ts_parser_func(defel, attnum)` (tsearchcmds.c:73).
fn get_ts_parser_func<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem, attnum: i32) -> PgResult<Oid> {
    let func_name = func_name_components(mcx, defel)?;
    let mut type_id = [InvalidOid; 3];
    let mut ret_type_id = INTERNALOID; /* correct for most */
    type_id[0] = INTERNALOID;
    let nargs;
    match attnum {
        Anum_pg_ts_parser_prsstart => {
            nargs = 2;
            type_id[1] = INT4OID;
        }
        Anum_pg_ts_parser_prstoken => {
            nargs = 3;
            type_id[1] = INTERNALOID;
            type_id[2] = INTERNALOID;
        }
        Anum_pg_ts_parser_prsend => {
            nargs = 1;
            ret_type_id = VOIDOID;
        }
        Anum_pg_ts_parser_prsheadline => {
            nargs = 3;
            type_id[1] = INTERNALOID;
            type_id[2] = TSQUERYOID;
        }
        Anum_pg_ts_parser_prslextype => {
            nargs = 1;
            /*
             * Note: because the lextype method returns type internal, it must
             * have an internal-type argument for security reasons.  The
             * argument is not actually used, but is just passed as a zero.
             */
        }
        _ => {
            /* should not be here */
            return elog_error(
                mcx,
                format!("unrecognized attribute for text search parser: {attnum}"),
                "get_ts_parser_func",
            );
        }
    }

    let arg_types: &[Oid] = &type_id[..nargs];
    let proc_oid = backend_parser_parse_func_seams::lookup_func_name::call(
        &func_name,
        nargs as i32,
        arg_types,
        false,
    )?;
    if backend_utils_cache_lsyscache_seams::get_func_rettype::call(proc_oid)? != ret_type_id {
        let sig = backend_parser_func::func_signature_string(&func_name, nargs as i32, &[], arg_types)?;
        let fmt = backend_utils_adt_format_type_seams::format_type_be::call(mcx, ret_type_id)?;
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "function {} should return type {}",
                sig,
                fmt.as_str()
            ))
            .finish(here("get_ts_parser_func"))
            .map(|()| unreachable!());
    }

    Ok(proc_oid)
}

/// `makeParserDependencies(tuple)` (tsearchcmds.c:136).
fn makeParserDependencies<'mcx>(mcx: Mcx<'mcx>, prs: &TSParserForm) -> PgResult<ObjectAddress> {
    let myself = addr(TSParserRelationId, prs.oid);

    let mut addrs = backend_catalog_dependency_seams::new_object_addresses::call()?;

    /* dependency on namespace */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(NamespaceRelationId, prs.prsnamespace),
        &mut addrs,
    )?;

    /* dependency on extension */
    backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    /* dependencies on functions */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(ProcedureRelationId, prs.prsstart),
        &mut addrs,
    )?;
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(ProcedureRelationId, prs.prstoken),
        &mut addrs,
    )?;
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(ProcedureRelationId, prs.prsend),
        &mut addrs,
    )?;
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(ProcedureRelationId, prs.prslextype),
        &mut addrs,
    )?;

    if OidIsValid(prs.prsheadline) {
        backend_catalog_dependency_seams::add_exact_object_address::call(
            addr(ProcedureRelationId, prs.prsheadline),
            &mut addrs,
        )?;
    }

    backend_catalog_dependency_seams::record_object_address_dependencies::call(
        myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;

    Ok(myself)
}

/// `DefineTSParser(names, parameters)` (tsearchcmds.c:183).
pub fn DefineTSParser<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[NodePtr<'mcx>],
    parameters: &[NodePtr<'mcx>],
) -> PgResult<ObjectAddress> {
    if !backend_utils_init_miscinit_seams::superuser::call(mcx)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create text search parsers")
            .finish(here("DefineTSParser"))
            .map(|()| unreachable!());
    }

    /* Convert list of names to a name and namespace */
    let nl = namelist(names)?;
    let (namespaceoid, prsname) = QualifiedNameGetCreationNamespace(mcx, &nl)?;

    let mut row = NewTSParser {
        prsname: prsname.to_string(),
        prsnamespace: namespaceoid,
        ..Default::default()
    };

    /* loop over the definition list and extract the information we need. */
    for node in parameters {
        let defel = expect_defelem(node)?;
        let defname = def_name(defel);
        match defname {
            "start" => row.prsstart = get_ts_parser_func(mcx, defel, Anum_pg_ts_parser_prsstart)?,
            "gettoken" => {
                row.prstoken = get_ts_parser_func(mcx, defel, Anum_pg_ts_parser_prstoken)?
            }
            "end" => row.prsend = get_ts_parser_func(mcx, defel, Anum_pg_ts_parser_prsend)?,
            "headline" => {
                row.prsheadline = get_ts_parser_func(mcx, defel, Anum_pg_ts_parser_prsheadline)?
            }
            "lextypes" => {
                row.prslextype = get_ts_parser_func(mcx, defel, Anum_pg_ts_parser_prslextype)?
            }
            _ => {
                return ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "text search parser parameter \"{defname}\" not recognized"
                    ))
                    .finish(here("DefineTSParser"))
                    .map(|()| unreachable!());
            }
        }
    }

    /* Validation */
    if !OidIsValid(row.prsstart) {
        return required_error("text search parser start method is required", "DefineTSParser");
    }
    if !OidIsValid(row.prstoken) {
        return required_error("text search parser gettoken method is required", "DefineTSParser");
    }
    if !OidIsValid(row.prsend) {
        return required_error("text search parser end method is required", "DefineTSParser");
    }
    if !OidIsValid(row.prslextype) {
        return required_error("text search parser lextypes method is required", "DefineTSParser");
    }

    /* Looks good, insert */
    let (prs_oid, prs) = seam::insert_ts_parser::call(&row)?;

    let address = makeParserDependencies(mcx, &prs)?;

    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        TSParserRelationId,
        prs_oid,
        0,
    )?;

    Ok(address)
}

/* =========================================================================
 * TS Dictionary commands
 * ========================================================================= */

/// `makeDictionaryDependencies(tuple)` (tsearchcmds.c:306).
fn makeDictionaryDependencies<'mcx>(mcx: Mcx<'mcx>, dict: &TSDictForm) -> PgResult<ObjectAddress> {
    let myself = addr(TSDictionaryRelationId, dict.oid);

    /* dependency on owner */
    backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
        myself.classId,
        myself.objectId,
        dict.dictowner,
    )?;

    /* dependency on extension */
    backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    let mut addrs = backend_catalog_dependency_seams::new_object_addresses::call()?;

    /* dependency on namespace */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(NamespaceRelationId, dict.dictnamespace),
        &mut addrs,
    )?;

    /* dependency on template */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(TSTemplateRelationId, dict.dicttemplate),
        &mut addrs,
    )?;

    backend_catalog_dependency_seams::record_object_address_dependencies::call(
        myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;

    Ok(myself)
}

/// `verify_dictoptions(tmplId, dictoptions)` (tsearchcmds.c:341).
///
/// The owned port carries the option list as its already-serialized text
/// (`dictoptions`), which the init method re-parses; `None` means an empty list.
fn verify_dictoptions<'mcx>(
    mcx: Mcx<'mcx>,
    tmpl_id: Oid,
    dictoptions: Option<&str>,
) -> PgResult<()> {
    /* Suppress this test when running in a standalone backend (initdb hack). */
    if !backend_utils_init_small_seams::is_under_postmaster::call() {
        return Ok(());
    }

    let (tmplname, initmethod) = match seam::ts_template_init_method::call(tmpl_id)? {
        Some(v) => v,
        None => {
            return elog_error(
                mcx,
                format!("cache lookup failed for text search template {tmpl_id}"),
                "verify_dictoptions",
            );
        }
    };

    if !OidIsValid(initmethod) {
        /* If there is no init method, disallow any options */
        if dictoptions.is_some() {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "text search template \"{tmplname}\" does not accept options"
                ))
                .finish(here("verify_dictoptions"))
                .map(|()| ());
        }
    } else {
        /*
         * Call the init method and see if it complains.  We don't worry about
         * leaking memory; our command will soon be over anyway. The option list
         * crosses as the `(defname, value)` string pairs the init method reads.
         */
        let pairs = match dictoptions {
            Some(txt) => deserialize_deflist_strings(mcx, txt)?,
            None => Vec::new(),
        };
        seam::call_dict_init::call(initmethod, &pairs)?;
    }

    Ok(())
}

/// `DefineTSDictionary(names, parameters)` (tsearchcmds.c:396).
pub fn DefineTSDictionary<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[NodePtr<'mcx>],
    parameters: &[NodePtr<'mcx>],
) -> PgResult<ObjectAddress> {
    let nl = namelist(names)?;
    let (namespaceoid, dictname) = QualifiedNameGetCreationNamespace(mcx, &nl)?;

    /* Check we have creation rights in target namespace */
    namespace_create_aclcheck(mcx, namespaceoid)?;

    let mut templ_id = InvalidOid;
    /* Collected dictionary options (those that are not "template"). */
    let mut dictoptions: Vec<&DefElem> = Vec::new();

    for node in parameters {
        let defel = expect_defelem(node)?;
        let defname = def_name(defel);
        if defname == "template" {
            templ_id = get_ts_template_oid(mcx, &def_qualified_namelist(defel)?, false)?;
        } else {
            /* Assume it's an option for the dictionary itself */
            dictoptions.push(defel);
        }
    }

    /* Validation */
    if !OidIsValid(templ_id) {
        return required_error("text search template is required", "DefineTSDictionary");
    }

    /* (None dictinitoption => SQL NULL). */
    let dictoption_text = if !dictoptions.is_empty() {
        Some(serialize_deflist(mcx, &dictoptions)?)
    } else {
        None
    };

    verify_dictoptions(mcx, templ_id, dictoption_text.as_deref())?;

    /* Looks good, insert */
    let (dict_oid, dict) = seam::insert_ts_dict::call(
        dictname,
        namespaceoid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        templ_id,
        dictoption_text.as_deref(),
    )?;

    let address = makeDictionaryDependencies(mcx, &dict)?;

    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        TSDictionaryRelationId,
        dict_oid,
        0,
    )?;

    Ok(address)
}

/// `AlterTSDictionary(stmt)` (tsearchcmds.c:492).
pub fn AlterTSDictionary<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSDictionaryStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let dict_nl = namelist(&stmt.dictname)?;
    let dict_id = get_ts_dict_oid(mcx, &dict_nl, false)?;

    /*
     * Fetch the dict tuple together with its template OID and the raw existing
     * option text (the C order: syscache fetch, ownership check, then read +
     * deserialize the existing option attribute).
     */
    let (dicttemplate, existing_opt) = seam::dict_options_and_template::call(dict_id)?;

    /* must be owner */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TSDictionaryRelationId,
        dict_id,
        backend_utils_init_miscinit_seams::get_user_id::call(),
    )? {
        let name = NameListToString(mcx, &dict_nl)?;
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_TSDICTIONARY,
            Some(name.as_str().to_string()),
        )?;
    }

    /* Deserialize the existing options (NIL when the column is NULL). */
    let mut dictoptions: Vec<DefElem> = match existing_opt {
        Some(ref txt) => deserialize_deflist(mcx, txt)?,
        None => Vec::new(),
    };

    /* Modify the options list as per specified changes */
    for node in &stmt.options {
        let defel = expect_defelem(node)?;
        let defname = def_name(defel);

        /* Remove any matches ... */
        dictoptions.retain(|oldel| def_name(oldel) != defname);

        /* and add new value if it's got one */
        if defel.arg.is_some() {
            dictoptions.push(clone_defelem(mcx, defel)?);
        }
    }

    /* Validate. (None => set dictinitoption to NULL). */
    let opttext = if !dictoptions.is_empty() {
        let refs: Vec<&DefElem> = dictoptions.iter().collect();
        Some(serialize_deflist(mcx, &refs)?)
    } else {
        None
    };
    verify_dictoptions(mcx, dicttemplate, opttext.as_deref())?;

    /* Looks good, update. */
    seam::update_dict_options::call(dict_id, opttext.as_deref())?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TSDictionaryRelationId,
        dict_id,
        0,
    )?;

    let address = addr(TSDictionaryRelationId, dict_id);

    /*
     * NOTE: because we only support altering the options, not the template,
     * there is no need to update dependencies.
     */

    Ok(address)
}

/* =========================================================================
 * TS Template commands
 * ========================================================================= */

/// `get_ts_template_func(defel, attnum)` (tsearchcmds.c:608).
fn get_ts_template_func<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem, attnum: i32) -> PgResult<Oid> {
    let func_name = func_name_components(mcx, defel)?;
    let type_id = [INTERNALOID; 4];
    let ret_type_id = INTERNALOID;
    let nargs = match attnum {
        Anum_pg_ts_template_tmplinit => 1,
        Anum_pg_ts_template_tmpllexize => 4,
        _ => {
            return elog_error(
                mcx,
                format!("unrecognized attribute for text search template: {attnum}"),
                "get_ts_template_func",
            );
        }
    };

    let arg_types: &[Oid] = &type_id[..nargs];
    let proc_oid = backend_parser_parse_func_seams::lookup_func_name::call(
        &func_name,
        nargs as i32,
        arg_types,
        false,
    )?;
    if backend_utils_cache_lsyscache_seams::get_func_rettype::call(proc_oid)? != ret_type_id {
        let sig = backend_parser_func::func_signature_string(&func_name, nargs as i32, &[], arg_types)?;
        let fmt = backend_utils_adt_format_type_seams::format_type_be::call(mcx, ret_type_id)?;
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
            .errmsg(format!(
                "function {} should return type {}",
                sig,
                fmt.as_str()
            ))
            .finish(here("get_ts_template_func"))
            .map(|()| unreachable!());
    }

    Ok(proc_oid)
}

/// `makeTSTemplateDependencies(tuple)` (tsearchcmds.c:651).
fn makeTSTemplateDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    tmpl: &TSTemplateForm,
) -> PgResult<ObjectAddress> {
    let myself = addr(TSTemplateRelationId, tmpl.oid);

    /* dependency on extension */
    backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension::call(mcx, &myself, false)?;

    let mut addrs = backend_catalog_dependency_seams::new_object_addresses::call()?;

    /* dependency on namespace */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(NamespaceRelationId, tmpl.tmplnamespace),
        &mut addrs,
    )?;

    /* dependencies on functions */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(ProcedureRelationId, tmpl.tmpllexize),
        &mut addrs,
    )?;

    if OidIsValid(tmpl.tmplinit) {
        backend_catalog_dependency_seams::add_exact_object_address::call(
            addr(ProcedureRelationId, tmpl.tmplinit),
            &mut addrs,
        )?;
    }

    backend_catalog_dependency_seams::record_object_address_dependencies::call(
        myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;

    Ok(myself)
}

/// `DefineTSTemplate(names, parameters)` (tsearchcmds.c:689).
pub fn DefineTSTemplate<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[NodePtr<'mcx>],
    parameters: &[NodePtr<'mcx>],
) -> PgResult<ObjectAddress> {
    if !backend_utils_init_miscinit_seams::superuser::call(mcx)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("must be superuser to create text search templates")
            .finish(here("DefineTSTemplate"))
            .map(|()| unreachable!());
    }

    let nl = namelist(names)?;
    let (namespaceoid, tmplname) = QualifiedNameGetCreationNamespace(mcx, &nl)?;

    let mut row = NewTSTemplate {
        tmplname: tmplname.to_string(),
        tmplnamespace: namespaceoid,
        ..Default::default()
    };

    for node in parameters {
        let defel = expect_defelem(node)?;
        let defname = def_name(defel);
        match defname {
            "init" => row.tmplinit = get_ts_template_func(mcx, defel, Anum_pg_ts_template_tmplinit)?,
            "lexize" => {
                row.tmpllexize = get_ts_template_func(mcx, defel, Anum_pg_ts_template_tmpllexize)?
            }
            _ => {
                return ereport(ERROR)
                    .errcode(ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!(
                        "text search template parameter \"{defname}\" not recognized"
                    ))
                    .finish(here("DefineTSTemplate"))
                    .map(|()| unreachable!());
            }
        }
    }

    /* Validation */
    if !OidIsValid(row.tmpllexize) {
        return required_error(
            "text search template lexize method is required",
            "DefineTSTemplate",
        );
    }

    /* Looks good, insert */
    let (tmpl_oid, tmpl) = seam::insert_ts_template::call(&row)?;

    let address = makeTSTemplateDependencies(mcx, &tmpl)?;

    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        TSTemplateRelationId,
        tmpl_oid,
        0,
    )?;

    Ok(address)
}

/* =========================================================================
 * TS Configuration commands
 * ========================================================================= */

/// `GetTSConfigTuple(names)` (tsearchcmds.c:786) — find the config row by
/// qualified name, returning its form snapshot (or `None` if no such config).
fn GetTSConfigForm<'mcx>(
    _mcx: Mcx<'mcx>,
    names: &[Option<String>],
) -> PgResult<Option<TSConfigForm>> {
    seam::get_ts_config_form::call(names)
}

/// Body for the `get_ts_config_form` seam (`GetTSConfigTuple`, tsearchcmds.c:786):
/// `get_ts_config_oid(names, missing_ok=true)`; `None` when the name resolves to
/// no config, else the `TSConfigForm` snapshot via the `config_form_by_oid`
/// projection (which raises the C "cache lookup failed" elog on a should-not-
/// happen miss).
fn get_ts_config_form_impl(names: &[Option<String>]) -> PgResult<Option<TSConfigForm>> {
    let scratch = MemoryContext::new("GetTSConfigTuple");
    let mcx = scratch.mcx();
    let cfg_id = get_ts_config_oid(mcx, names, true)?;
    if !OidIsValid(cfg_id) {
        return Ok(None);
    }
    Ok(Some(seam::config_form_by_oid::call(cfg_id)?))
}

/// `makeConfigurationDependencies(tuple, removeOld, mapRel)` (tsearchcmds.c:811).
///
/// `with_map` mirrors the C `mapRel != NULL` test: when set, the config-map
/// dictionary dependencies are gathered (after a `CommandCounterIncrement`).
fn makeConfigurationDependencies<'mcx>(
    mcx: Mcx<'mcx>,
    cfg: &TSConfigForm,
    remove_old: bool,
    with_map: bool,
) -> PgResult<ObjectAddress> {
    let myself = addr(TSConfigRelationId, cfg.oid);

    /* for ALTER case, first flush old dependencies, except extension deps */
    if remove_old {
        backend_catalog_pg_depend_seams::deleteDependencyRecordsFor::call(
            myself.classId,
            myself.objectId,
            true,
        )?;
        backend_catalog_pg_shdepend_seams::deleteSharedDependencyRecordsFor::call(
            myself.classId,
            myself.objectId,
            0,
        )?;
    }

    let mut addrs = backend_catalog_dependency_seams::new_object_addresses::call()?;

    /* dependency on namespace */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(NamespaceRelationId, cfg.cfgnamespace),
        &mut addrs,
    )?;

    /* dependency on owner */
    backend_catalog_pg_shdepend_seams::recordDependencyOnOwner::call(
        myself.classId,
        myself.objectId,
        cfg.cfgowner,
    )?;

    /* dependency on extension */
    backend_catalog_pg_depend_seams::recordDependencyOnCurrentExtension::call(
        mcx, &myself, remove_old,
    )?;

    /* dependency on parser */
    backend_catalog_dependency_seams::add_exact_object_address::call(
        addr(TSParserRelationId, cfg.cfgparser),
        &mut addrs,
    )?;

    /* dependencies on dictionaries listed in config map */
    if with_map {
        /* CCI to ensure we can see effects of caller's changes */
        backend_access_transam_xact_seams::command_counter_increment::call()?;

        for cfgmap in seam::config_map_entries::call(mcx, myself.objectId)?.iter() {
            backend_catalog_dependency_seams::add_exact_object_address::call(
                addr(TSDictionaryRelationId, cfgmap.mapdict),
                &mut addrs,
            )?;
        }
    }

    /* Record 'em (this includes duplicate elimination) */
    backend_catalog_dependency_seams::record_object_address_dependencies::call(
        myself,
        &mut addrs,
        DEPENDENCY_NORMAL,
    )?;

    Ok(myself)
}

/// `DefineTSConfiguration(names, parameters, copied)` (tsearchcmds.c:898).
///
/// `copied` is the C out-parameter: when the config is created via `… COPY`, the
/// returned `Option<ObjectAddress>` carries the source config's address.
pub fn DefineTSConfiguration<'mcx>(
    mcx: Mcx<'mcx>,
    names: &[NodePtr<'mcx>],
    parameters: &[NodePtr<'mcx>],
    copied: &mut Option<ObjectAddress>,
) -> PgResult<ObjectAddress> {
    let nl = namelist(names)?;
    let (namespaceoid, cfgname) = QualifiedNameGetCreationNamespace(mcx, &nl)?;

    /* Check we have creation rights in target namespace */
    namespace_create_aclcheck(mcx, namespaceoid)?;

    let mut source_oid = InvalidOid;
    let mut prs_oid = InvalidOid;

    for node in parameters {
        let defel = expect_defelem(node)?;
        let defname = def_name(defel);
        if defname == "parser" {
            prs_oid = get_ts_parser_oid(mcx, &def_qualified_namelist(defel)?, false)?;
        } else if defname == "copy" {
            source_oid = get_ts_config_oid(mcx, &def_qualified_namelist(defel)?, false)?;
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!(
                    "text search configuration parameter \"{defname}\" not recognized"
                ))
                .finish(here("DefineTSConfiguration"))
                .map(|()| unreachable!());
        }
    }

    if OidIsValid(source_oid) && OidIsValid(prs_oid) {
        return ereport(ERROR)
            .errcode(ERRCODE_SYNTAX_ERROR)
            .errmsg("cannot specify both PARSER and COPY options")
            .finish(here("DefineTSConfiguration"))
            .map(|()| unreachable!());
    }

    /* make copied tsconfig available to callers */
    if OidIsValid(source_oid) {
        *copied = Some(addr(TSConfigRelationId, source_oid));
    }

    /* Look up source config if given. */
    if OidIsValid(source_oid) {
        let cfg = seam::config_form_by_oid::call(source_oid)?;
        prs_oid = cfg.cfgparser; /* use source's parser */
    }

    /* Validation */
    if !OidIsValid(prs_oid) {
        return required_error("text search parser is required", "DefineTSConfiguration");
    }

    /* Looks good, build tuple and insert */
    let (cfg_oid, cfg) = seam::insert_ts_config::call(
        cfgname,
        namespaceoid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        prs_oid,
    )?;

    let with_map = OidIsValid(source_oid);
    if with_map {
        /* Copy token-dicts map from source config */
        let entries: Vec<ConfigMapEntry> =
            seam::config_map_entries::call(mcx, source_oid)?.iter().copied().collect();
        if !entries.is_empty() {
            seam::insert_config_map_entries::call(cfg_oid, &entries)?;
        }
    }

    let address = makeConfigurationDependencies(mcx, &cfg, false, with_map)?;

    backend_catalog_objectaccess_seams::invoke_object_post_create_hook::call(
        TSConfigRelationId,
        cfg_oid,
        0,
    )?;

    Ok(address)
}

/// `RemoveTSConfigurationById(cfgId)` (tsearchcmds.c:1107).
pub fn RemoveTSConfigurationById(cfg_id: Oid) -> PgResult<()> {
    /* Remove the pg_ts_config entry */
    seam::delete_ts_config_row::call(cfg_id)?;

    /* Remove any pg_ts_config_map entries */
    seam::delete_config_map_for_cfg::call(cfg_id)?;

    Ok(())
}

/// `AlterTSConfiguration(stmt)` (tsearchcmds.c:1155).
pub fn AlterTSConfiguration<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let cfg_nl = namelist(&stmt.cfgname)?;

    /* Find the configuration */
    let cfg = match GetTSConfigForm(mcx, &cfg_nl)? {
        Some(cfg) => cfg,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!(
                    "text search configuration \"{}\" does not exist",
                    NameListToString(mcx, &cfg_nl)?.as_str()
                ))
                .finish(here("AlterTSConfiguration"))
                .map(|()| unreachable!());
        }
    };

    let cfg_id = cfg.oid;

    /* must be owner */
    if !backend_catalog_aclchk_seams::object_ownercheck::call(
        TSConfigRelationId,
        cfg_id,
        backend_utils_init_miscinit_seams::get_user_id::call(),
    )? {
        let name = NameListToString(mcx, &cfg_nl)?;
        backend_catalog_aclchk_seams::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            OBJECT_TSCONFIGURATION,
            Some(name.as_str().to_string()),
        )?;
    }

    /* Add or drop mappings */
    if !stmt.dicts.is_empty() {
        MakeConfigurationMapping(mcx, stmt, &cfg)?;
    } else if !stmt.tokentype.is_empty() {
        DropConfigurationMapping(mcx, stmt, &cfg)?;
    }

    /* Update dependencies */
    makeConfigurationDependencies(mcx, &cfg, true, true)?;

    backend_catalog_objectaccess_seams::invoke_object_post_alter_hook::call(
        TSConfigRelationId,
        cfg_id,
        0,
    )?;

    Ok(addr(TSConfigRelationId, cfg_id))
}

/// One `(num, name)` token-type item — the in-crate `TSTokenTypeItem`.
#[derive(Clone, Debug)]
struct TokenTypeItem {
    num: i32,
    name: String,
}

/// `tstoken_list_member(token_name, tokens)` (tsearchcmds.c:1203).
fn tstoken_list_member(token_name: &str, tokens: &[TokenTypeItem]) -> bool {
    tokens.iter().any(|ts| ts.name == token_name)
}

/// `getTokenTypes(prsId, tokennames)` (tsearchcmds.c:1228).
fn getTokenTypes<'mcx>(
    mcx: Mcx<'mcx>,
    prs_id: Oid,
    tokennames: &[NodePtr<'mcx>],
) -> PgResult<Vec<TokenTypeItem>> {
    let names = string_list(tokennames)?;
    let mut result: Vec<TokenTypeItem> = Vec::new();

    if names.is_empty() {
        return Ok(result);
    }

    let lextype_oid = seam::parser_lextype_oid::call(prs_id)?;
    if !OidIsValid(lextype_oid) {
        return elog_error(
            mcx,
            format!("method lextype isn't defined for text search parser {prs_id}"),
            "getTokenTypes",
        );
    }

    /* lextype takes one dummy argument */
    let list: Vec<LexDescr> = seam::call_parser_lextype::call(mcx, lextype_oid)?.iter().cloned().collect();

    for val in &names {
        /* Skip if this token is already in the result */
        if tstoken_list_member(val, &result) {
            continue;
        }

        let mut found = false;
        for d in &list {
            if d.lexid == 0 {
                break;
            }
            if *val == d.alias {
                result.push(TokenTypeItem {
                    num: d.lexid,
                    name: val.clone(),
                });
                found = true;
                break;
            }
        }
        if !found {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("token type \"{val}\" does not exist"))
                .finish(here("getTokenTypes"))
                .map(|()| unreachable!());
        }
    }

    Ok(result)
}

/// `MakeConfigurationMapping(stmt, tup, relMap)` (tsearchcmds.c:1287).
fn MakeConfigurationMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
    cfg: &TSConfigForm,
) -> PgResult<()> {
    let cfg_id = cfg.oid;
    let prs_id = cfg.cfgparser;

    let tokens = getTokenTypes(mcx, prs_id, &stmt.tokentype)?;
    let ntoken = tokens.len();

    if stmt.override_ {
        /* delete maps for tokens if they exist and command was ALTER */
        for ts in &tokens {
            seam::delete_config_map_for_token::call(cfg_id, ts.num)?;
        }
    }

    /* Convert list of dictionary names to array of dict OIDs */
    let ndict = stmt.dicts.len();
    let mut dict_ids: Vec<Oid> = Vec::with_capacity(ndict);
    for names in stmt.dicts.iter() {
        let nl = name_sublist(names)?;
        dict_ids.push(get_ts_dict_oid(mcx, &nl, false)?);
    }

    if stmt.replace {
        /* Replace a specific dictionary in existing entries */
        let dict_old = dict_ids[0];
        let dict_new = dict_ids[1];
        let token_nums: Vec<i32> = tokens.iter().map(|t| t.num).collect();
        seam::replace_config_map_dict::call(cfg_id, &token_nums, dict_old, dict_new)?;
    } else {
        /* Insertion of new entries */
        let mut entries: Vec<ConfigMapEntry> = Vec::with_capacity(ntoken * ndict);
        for ts in &tokens {
            for (j, dict) in dict_ids.iter().enumerate() {
                entries.push(ConfigMapEntry {
                    maptokentype: ts.num,
                    mapseqno: (j + 1) as i32,
                    mapdict: *dict,
                });
            }
        }
        if !entries.is_empty() {
            seam::insert_config_map_entries::call(cfg_id, &entries)?;
        }
    }

    seam::event_trigger_collect_alter_ts_config::call(cfg_id, &dict_ids)?;

    Ok(())
}

/// `DropConfigurationMapping(stmt, tup, relMap)` (tsearchcmds.c:1490).
fn DropConfigurationMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
    cfg: &TSConfigForm,
) -> PgResult<()> {
    let cfg_id = cfg.oid;
    let prs_id = cfg.cfgparser;

    let tokens = getTokenTypes(mcx, prs_id, &stmt.tokentype)?;

    for ts in &tokens {
        let deleted = seam::delete_config_map_for_token::call(cfg_id, ts.num)?;
        let found = deleted > 0;

        if !found {
            if !stmt.missing_ok {
                return ereport(ERROR)
                    .errcode(ERRCODE_UNDEFINED_OBJECT)
                    .errmsg(format!(
                        "mapping for token type \"{}\" does not exist",
                        ts.name
                    ))
                    .finish(here("DropConfigurationMapping"))
                    .map(|()| ());
            } else {
                ereport(NOTICE)
                    .errmsg(format!(
                        "mapping for token type \"{}\" does not exist, skipping",
                        ts.name
                    ))
                    .finish(here("DropConfigurationMapping"))?;
            }
        }
    }

    seam::event_trigger_collect_alter_ts_config::call(cfg_id, &[])?;

    Ok(())
}

/* =========================================================================
 * (de)serialization of dictionary option lists
 * ========================================================================= */

/// `ESCAPE_STRING_SYNTAX` ('E') — the E-string prefix.
const ESCAPE_STRING_SYNTAX: char = 'E';

/// `serialize_deflist(deflist)` (tsearchcmds.c:1564).  Returns the option text
/// as a Rust `String` (the seam wraps it into a TEXT datum on insert/update).
pub fn serialize_deflist<'mcx>(mcx: Mcx<'mcx>, deflist: &[&DefElem<'mcx>]) -> PgResult<String> {
    let mut buf = String::new();

    let n = deflist.len();
    for (i, &defel) in deflist.iter().enumerate() {
        let val = def_get_string(mcx, defel)?;

        buf.push_str(&quote_identifier(def_name(defel)));
        buf.push_str(" = ");

        /*
         * If the value is a T_Integer or T_Float, emit it without quotes,
         * otherwise with quotes.
         */
        if defel
            .arg
            .as_deref()
            .is_some_and(|n| n.is_integer() || n.is_float())
        {
            buf.push_str(&val);
        } else {
            /* If backslashes appear, force E syntax to quote them safely */
            if val.contains('\\') {
                buf.push(ESCAPE_STRING_SYNTAX);
            }
            buf.push('\'');
            /*
             * C (tsearchcmds.c:1594-1601) appends each raw byte verbatim,
             * doubling only SQL_STR_DOUBLE bytes ('\'' and '\\', both ASCII).
             * A UTF-8 continuation/lead byte is never 0x27/0x5C, so per-char
             * doubling is byte-identical to C for valid UTF-8.
             */
            for ch in val.chars() {
                if ch == '\'' || ch == '\\' {
                    buf.push(ch);
                }
                buf.push(ch);
            }
            buf.push('\'');
        }
        if i + 1 != n {
            buf.push_str(", ");
        }
    }

    Ok(buf)
}

/// `deserialize_deflist(txt)` (tsearchcmds.c:1620).  The text state machine is
/// fully in-crate; DefElem nodes are built via [`buildDefItem`].
pub fn deserialize_deflist<'mcx>(mcx: Mcx<'mcx>, txt: &str) -> PgResult<Vec<DefElem<'mcx>>> {
    #[derive(Clone, Copy, PartialEq)]
    enum DsState {
        WaitKey,
        InKey,
        InQKey,
        WaitEq,
        WaitValue,
        InSqValue,
        InDqValue,
        InWValue,
    }
    use DsState::*;

    let bytes = txt.as_bytes();
    let len = bytes.len();
    let mut result: Vec<DefElem> = Vec::new();
    let mut workspace = alloc::vec![0u8; len + 1];
    let mut wsptr: usize = 0;
    let mut startvalue: usize = 0;
    let mut state = WaitKey;

    let mut ptr = 0usize;
    while ptr < len {
        let c = bytes[ptr];
        match state {
            WaitKey => {
                if is_space(c) || c == b',' {
                    ptr += 1;
                    continue;
                }
                if c == b'"' {
                    wsptr = 0;
                    state = InQKey;
                } else {
                    wsptr = 0;
                    workspace[wsptr] = c;
                    wsptr += 1;
                    state = InKey;
                }
            }
            InKey => {
                if is_space(c) {
                    workspace[wsptr] = 0;
                    wsptr += 1;
                    state = WaitEq;
                } else if c == b'=' {
                    workspace[wsptr] = 0;
                    wsptr += 1;
                    state = WaitValue;
                } else {
                    workspace[wsptr] = c;
                    wsptr += 1;
                }
            }
            InQKey => {
                if c == b'"' {
                    if ptr + 1 < len && bytes[ptr + 1] == b'"' {
                        /* copy only one of the two quotes */
                        workspace[wsptr] = c;
                        wsptr += 1;
                        ptr += 1;
                    } else {
                        workspace[wsptr] = 0;
                        wsptr += 1;
                        state = WaitEq;
                    }
                } else {
                    workspace[wsptr] = c;
                    wsptr += 1;
                }
            }
            WaitEq => {
                if c == b'=' {
                    state = WaitValue;
                } else if !is_space(c) {
                    return invalid_param_list(txt, "deserialize_deflist");
                }
            }
            WaitValue => {
                if c == b'\'' {
                    startvalue = wsptr;
                    state = InSqValue;
                } else if c == b'E' && ptr + 1 < len && bytes[ptr + 1] == b'\'' {
                    ptr += 1;
                    startvalue = wsptr;
                    state = InSqValue;
                } else if c == b'"' {
                    startvalue = wsptr;
                    state = InDqValue;
                } else if !is_space(c) {
                    startvalue = wsptr;
                    workspace[wsptr] = c;
                    wsptr += 1;
                    state = InWValue;
                }
            }
            InSqValue => {
                if c == b'\'' {
                    if ptr + 1 < len && bytes[ptr + 1] == b'\'' {
                        /* copy only one of the two quotes */
                        workspace[wsptr] = c;
                        wsptr += 1;
                        ptr += 1;
                    } else {
                        workspace[wsptr] = 0;
                        wsptr += 1;
                        build_and_append(mcx, &mut result, &workspace, startvalue, true)?;
                        state = WaitKey;
                    }
                } else if c == b'\\' {
                    if ptr + 1 < len && bytes[ptr + 1] == b'\\' {
                        /* copy only one of the two backslashes */
                        workspace[wsptr] = c;
                        wsptr += 1;
                        ptr += 1;
                    } else {
                        workspace[wsptr] = c;
                        wsptr += 1;
                    }
                } else {
                    workspace[wsptr] = c;
                    wsptr += 1;
                }
            }
            InDqValue => {
                if c == b'"' {
                    if ptr + 1 < len && bytes[ptr + 1] == b'"' {
                        /* copy only one of the two quotes */
                        workspace[wsptr] = c;
                        wsptr += 1;
                        ptr += 1;
                    } else {
                        workspace[wsptr] = 0;
                        wsptr += 1;
                        build_and_append(mcx, &mut result, &workspace, startvalue, true)?;
                        state = WaitKey;
                    }
                } else {
                    workspace[wsptr] = c;
                    wsptr += 1;
                }
            }
            InWValue => {
                if c == b',' || is_space(c) {
                    workspace[wsptr] = 0;
                    wsptr += 1;
                    build_and_append(mcx, &mut result, &workspace, startvalue, false)?;
                    state = WaitKey;
                } else {
                    workspace[wsptr] = c;
                    wsptr += 1;
                }
            }
        }
        ptr += 1;
    }

    if state == InWValue {
        workspace[wsptr] = 0;
        build_and_append(mcx, &mut result, &workspace, startvalue, false)?;
    } else if state != WaitKey {
        return invalid_param_list(txt, "deserialize_deflist");
    }

    Ok(result)
}

/// Helper for [`deserialize_deflist`]: extract the NUL-terminated key (start of
/// `workspace`) and value (from `startvalue`), build a DefElem via
/// [`buildDefItem`], and push it onto `result`.
fn build_and_append<'mcx>(
    mcx: Mcx<'mcx>,
    result: &mut Vec<DefElem<'mcx>>,
    workspace: &[u8],
    startvalue: usize,
    was_quoted: bool,
) -> PgResult<()> {
    let name = cstr_at(workspace, 0);
    let val = cstr_at(workspace, startvalue);
    result.push(buildDefItem(mcx, &name, &val, was_quoted)?);
    Ok(())
}

/// `buildDefItem(name, val, was_quoted)` (tsearchcmds.c:1833).
fn buildDefItem<'mcx>(
    mcx: Mcx<'mcx>,
    name: &str,
    val: &str,
    was_quoted: bool,
) -> PgResult<DefElem<'mcx>> {
    /* If input was quoted, always emit as string */
    if !was_quoted && !val.is_empty() {
        /* Try to parse as an integer (full consumption) */
        if let Some(v) = parse_full_int(val) {
            return make_def_elem(mcx, name, Node::mk_integer(mcx, Integer { ival: v })?);
        }
        /* Nope, how about as a float? (full consumption) */
        if parse_full_float(val) {
            let fval = PgString::from_str_in(val, mcx)?;
            return make_def_elem(mcx, name, Node::mk_float(mcx, Float { fval })?);
        }

        if val == "true" {
            return make_def_elem(mcx, name, Node::mk_boolean(mcx, Boolean { boolval: true })?);
        }
        if val == "false" {
            return make_def_elem(mcx, name, Node::mk_boolean(mcx, Boolean { boolval: false })?);
        }
    }
    /* Just make it a string */
    let sval = PgString::from_str_in(val, mcx)?;
    make_def_elem(mcx, name, Node::mk_string(mcx, StringNode { sval })?)
}

/* =========================================================================
 * small helpers
 * ========================================================================= */

/// Build an `ObjectAddress { classId, objectId, objectSubId: 0 }`.
fn addr(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// `makeDefElem(pstrdup(name), arg, -1)` — build an owned `DefElem` value node.
fn make_def_elem<'mcx>(mcx: Mcx<'mcx>, name: &str, arg: Node<'mcx>) -> PgResult<DefElem<'mcx>> {
    Ok(DefElem {
        defnamespace: None,
        defname: Some(PgString::from_str_in(name, mcx)?),
        arg: Some(mcx::alloc_in(mcx, arg)?),
        defaction: DEFELEM_UNSPEC,
        location: -1,
    })
}

/// `def->defname` — the option name (the owned tree keeps it as
/// `Option<PgString>`; an absent name renders empty, as a NULL `char *` would).
fn def_name<'a>(defel: &'a DefElem<'_>) -> &'a str {
    defel.defname.as_ref().map(|s| s.as_str()).unwrap_or("")
}

/// `(DefElem *) lfirst(pl)` — the cell must be a `Node::DefElem` (the grammar
/// guarantees it).
fn expect_defelem<'a, 'mcx>(node: &'a NodePtr<'mcx>) -> PgResult<&'a DefElem<'mcx>> {
    let node = &**node;
    match node.node_tag() {
        ntag::T_DefElem => Ok(node.expect_defelem()),
        _ => Err(PgError::error(format!(
            "unexpected node type in option list: {}",
            node.node_tag()
        ))),
    }
}

/// `defGetString(defel)` (define.c:43) over the owned `'mcx` `DefElem` arg.
fn def_get_string<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem<'mcx>) -> PgResult<String> {
    let arg = defel.arg.as_deref().ok_or_else(|| {
        syntax_error(format!("{} requires a parameter", def_name(defel)))
    })?;
    let s = match arg.node_tag() {
        ntag::T_Integer => arg.expect_integer().ival.to_string(),
        ntag::T_Float => arg.expect_float().fval.as_str().to_string(),
        ntag::T_Boolean => {
            let b = arg.expect_boolean();
            if b.boolval { "true" } else { "false" }.to_string()
        }
        ntag::T_String => arg.expect_string().sval.as_str().to_string(),
        /* case T_TypeName: return TypeNameToString((TypeName *) def->arg); */
        ntag::T_TypeName => type_name_to_string(arg.expect_typename())?,
        /* case T_List: return NameListToString((List *) def->arg); */
        ntag::T_List => name_list_to_string(arg.expect_list())?,
        /* case T_A_Star: return pstrdup("*"); */
        ntag::T_A_Star => "*".to_string(),
        _ => {
            return Err(ereport(ERROR)
                .errmsg_internal(format!("unrecognized node type: {}", arg.node_tag()))
                .into_error())
        }
    };
    let _ = mcx;
    Ok(s)
}

/// `TypeNameToString(typeName)` / `appendTypeNameToBuffer` (parse_type.c:433),
/// for the `defGetString` `T_TypeName` case. A `DefElem` option arg's `TypeName`
/// always carries `names` (it is a parsed identifier, never an internal
/// `typeOid`-only node), so the `format_type_be` fallback branch is unreachable
/// here and guarded loudly rather than reaching the unported renderer.
fn type_name_to_string(tn: &types_nodes::rawnodes::TypeName) -> PgResult<String> {
    let mut out = String::new();
    if !tn.names.is_empty() {
        /* Emit possibly-qualified name as-is. */
        for (i, name) in tn.names.iter().enumerate() {
            if i != 0 {
                out.push('.');
            }
            let node = &**name;
            match node.node_tag() {
                ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
                _ => return Err(unexpected_node_in_name_list()),
            }
        }
    } else {
        return Err(ereport(ERROR)
            .errmsg_internal(
                "text search dictionary option TypeName carries no name \
                 (internal typeOid-only form unsupported here)",
            )
            .into_error());
    }

    /* Decoration considered by LookupTypeName. */
    if tn.pct_type {
        out.push_str("%TYPE");
    }
    if !tn.arrayBounds.is_empty() {
        out.push_str("[]");
    }
    Ok(out)
}

/// `NameListToString(names)` (namespace.c) for the `defGetString` `T_List` case:
/// `'.'`-joined `String` cells, `A_Star` rendered as `*`.
fn name_list_to_string(names: &[NodePtr]) -> PgResult<String> {
    let mut out = String::new();
    for (i, node) in names.iter().enumerate() {
        if i != 0 {
            out.push('.');
        }
        let node = &**node;
        match node.node_tag() {
            ntag::T_String => out.push_str(node.expect_string().sval.as_str()),
            ntag::T_A_Star => out.push('*'),
            _ => return Err(unexpected_node_in_name_list()),
        }
    }
    Ok(out)
}

/// `defGetQualifiedName(defel)` (define.c:238), specialized to yield the name
/// list ready for the namespace-crate / function lookups.
fn def_qualified_namelist(defel: &DefElem) -> PgResult<Vec<Option<String>>> {
    let arg = defel.arg.as_deref().ok_or_else(|| {
        syntax_error(format!("{} requires a parameter", def_name(defel)))
    })?;
    match arg.node_tag() {
        // case T_TypeName: return ((TypeName *) def->arg)->names;
        ntag::T_TypeName => nodes_to_namelist(&arg.expect_typename().names),
        // case T_String: /* quoted name */ return list_make1(def->arg);
        ntag::T_String => Ok(alloc::vec![Some(arg.expect_string().sval.as_str().to_string())]),
        // case T_List: return (List *) def->arg;
        ntag::T_List => {
            /*
             * The TS grammar never produces a bare `List` for these args
             * (function names arrive as func_type/TypeName, quoted names as
             * String); guard loudly rather than walk a form that cannot occur.
             */
            Err(ereport(ERROR)
                .errmsg_internal(
                    "text search command name argument arrived as a bare List \
                     (unsupported owned-tree form)",
                )
                .into_error())
        }
        // default: ereport(... "argument of %s must be a name" ...);
        _ => Err(syntax_error(format!(
            "argument of {} must be a name",
            def_name(defel)
        ))),
    }
}

/// `defGetQualifiedName(defel)` reduced to a plain function-name component list
/// (the `PgString` components `LookupFuncName` / `func_signature_string` take),
/// which never take an `A_Star` (`*`) component.  Any `*` is a name-list error.
fn func_name_components<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem) -> PgResult<Vec<PgString<'mcx>>> {
    let names = def_qualified_namelist(defel)?;
    let mut out: Vec<PgString<'mcx>> = Vec::with_capacity(names.len());
    for n in names {
        match n {
            Some(s) => out.push(PgString::from_str_in(&s, mcx)?),
            None => return Err(unexpected_node_in_name_list()),
        }
    }
    Ok(out)
}

/// Convert a `TypeName.names` list (`PgVec<NodePtr>` of `Node::String` /
/// `Node::A_Star`) into a `Vec<Option<String>>` name list.
fn nodes_to_namelist(nodes: &[NodePtr]) -> PgResult<Vec<Option<String>>> {
    let mut out: Vec<Option<String>> = Vec::with_capacity(nodes.len());
    for node in nodes {
        let node = &**node;
        match node.node_tag() {
            ntag::T_String => out.push(Some(node.expect_string().sval.as_str().to_string())),
            // `*` markers (A_Star) become `None` in a name list.
            ntag::T_A_Star => out.push(None),
            _ => return Err(unexpected_node_in_name_list()),
        }
    }
    Ok(out)
}

/// Convert a qualified-name list (a statement's `dictname` / `cfgname`, a
/// `PgVec<NodePtr>` of `String` nodes) into the `Vec<Option<String>>` NameList
/// the namespace crate takes.
fn namelist(names: &[NodePtr]) -> PgResult<Vec<Option<String>>> {
    nodes_to_namelist(names)
}

/// One element of `stmt->dicts` (a `List *` of `List *` of String) — convert a
/// single inner name list (a `Node::List`) to a NameList.
fn name_sublist(node: &NodePtr) -> PgResult<Vec<Option<String>>> {
    let node = &**node;
    match node.node_tag() {
        ntag::T_List => nodes_to_namelist(node.expect_list()),
        _ => Err(unexpected_node_in_name_list()),
    }
}

/// Read a `List *` of String nodes (e.g. `stmt->tokentype`) as `Vec<String>`
/// (each `strVal(val)`).
fn string_list(list: &[NodePtr]) -> PgResult<Vec<String>> {
    let mut out: Vec<String> = Vec::with_capacity(list.len());
    for node in list {
        let node = &**node;
        match node.node_tag() {
            ntag::T_String => out.push(node.expect_string().sval.as_str().to_string()),
            _ => {
                return Err(PgError::error(format!(
                    "unexpected node type in token list: {}",
                    node.node_tag()
                )))
            }
        }
    }
    Ok(out)
}

/// Deep-copy a `DefElem` into `mcx` (the C `lappend(dictoptions, defel)` keeps
/// the stmt's node; the owned model copies it so it outlives the borrow).
fn clone_defelem<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem<'mcx>) -> PgResult<DefElem<'mcx>> {
    defel.clone_in(mcx)
}

/// `object_aclcheck(NamespaceRelationId, nsp, GetUserId(), ACL_CREATE)` + the
/// `aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name(nsp))` on a
/// non-OK result (shared by DefineTSDictionary / DefineTSConfiguration).
fn namespace_create_aclcheck<'mcx>(mcx: Mcx<'mcx>, namespaceoid: Oid) -> PgResult<()> {
    let aclresult: AclResult = backend_catalog_aclchk_seams::object_aclcheck::call(
        NamespaceRelationId,
        namespaceoid,
        backend_utils_init_miscinit_seams::get_user_id::call(),
        ACL_CREATE,
    )?;
    if aclresult != ACLCHECK_OK {
        let nspname = backend_utils_cache_lsyscache_seams::get_namespace_name::call(mcx, namespaceoid)?;
        backend_catalog_aclchk_seams::aclcheck_error::call(
            aclresult,
            types_nodes::parsenodes::OBJECT_SCHEMA,
            nspname.map(|s| s.as_str().to_string()),
        )?;
    }
    Ok(())
}

/// `deserialize_deflist` reduced to the `(defname, value-as-string)` pairs the
/// dict init method reads (`DefElemString`): the value is rendered exactly as
/// `defGetString` would (Integer/Float/Boolean/String).
fn deserialize_deflist_strings<'mcx>(
    mcx: Mcx<'mcx>,
    txt: &str,
) -> PgResult<Vec<DefElemString<'mcx>>> {
    let list = deserialize_deflist(mcx, txt)?;
    let mut out: Vec<DefElemString> = Vec::with_capacity(list.len());
    for de in &list {
        let defname = PgString::from_str_in(def_name(de), mcx)?;
        let val = def_get_string(mcx, de)?;
        let arg = PgString::from_str_in(&val, mcx)?;
        out.push(DefElemString { defname, arg });
    }
    Ok(out)
}

/// `quote_identifier(ident)` (utils/adt/ruleutils.c) — quote an identifier for
/// emission into the serialized option text.
///
/// The serialized option list only ever round-trips through
/// `deserialize_deflist` (which accepts both quoted and unquoted keys), so the
/// conservative rule here — quote unless the identifier is purely
/// `[a-z_][a-z0-9_]*` — is byte-faithful for the option-name domain.
fn quote_identifier(ident: &str) -> String {
    let safe = !ident.is_empty()
        && {
            let first = ident.as_bytes()[0];
            first == b'_' || first.is_ascii_lowercase()
        }
        && ident
            .bytes()
            .all(|b| b == b'_' || b.is_ascii_lowercase() || b.is_ascii_digit());
    if safe {
        ident.to_string()
    } else {
        let mut out = String::with_capacity(ident.len() + 2);
        out.push('"');
        for ch in ident.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
        out
    }
}

/// `elog(ERROR, msg)` — internal error (ERRCODE_INTERNAL_ERROR by default).
fn elog_error<'mcx, T>(_mcx: Mcx<'mcx>, msg: String, funcname: &'static str) -> PgResult<T> {
    ereport(ERROR)
        .errmsg_internal(msg)
        .finish(here(funcname))
        .map(|()| unreachable!())
}

/// `ereport(ERROR, ERRCODE_INVALID_OBJECT_DEFINITION, msg)` for "X is required".
fn required_error<T>(msg: &'static str, funcname: &'static str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_INVALID_OBJECT_DEFINITION)
        .errmsg(msg)
        .finish(here(funcname))
        .map(|()| unreachable!())
}

/// `ereport(ERROR, ERRCODE_SYNTAX_ERROR, "invalid parameter list format: %s")`.
fn invalid_param_list<T>(input: &str, funcname: &'static str) -> PgResult<T> {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(format!("invalid parameter list format: \"{input}\""))
        .finish(here(funcname))
        .map(|()| unreachable!())
}

/// `ereport(ERROR, errcode(ERRCODE_SYNTAX_ERROR), errmsg(msg))`.
fn syntax_error(message: String) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_SYNTAX_ERROR)
        .errmsg(message)
        .into_error()
}

/// `elog(ERROR, "unexpected node type in name list: %d", ...)`.
fn unexpected_node_in_name_list() -> PgError {
    ereport(ERROR)
        .errmsg_internal("unexpected node type in name list")
        .into_error()
}

/// `isspace((unsigned char) c)` for the C locale.
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// Read a NUL-terminated C string starting at `off` in `buf` as a `String`.
fn cstr_at(buf: &[u8], off: usize) -> String {
    let mut end = off;
    while end < buf.len() && buf[end] != 0 {
        end += 1;
    }
    String::from_utf8_lossy(&buf[off..end]).into_owned()
}

/// `strtoint(val, &endptr, 10)` with full consumption — parse the *entire*
/// string as a base-10 `int32`.
fn parse_full_int(val: &str) -> Option<i32> {
    val.parse::<i32>().ok()
}

/// `strtod(val, &endptr)` with full consumption — does the entire string parse
/// as a double?
fn parse_full_float(val: &str) -> bool {
    val.parse::<f64>().is_ok()
}

/// `pub fn init_seams()` — install the two inward seams other crates call
/// across a cycle: `deserialize_deflist` (ts_cache) and
/// `RemoveTSConfigurationById` (dependency.c `doDeletion`).
/// `case T_AlterTSDictionaryStmt: AlterTSDictionary(stmt)` (utility.c).
fn alter_ts_dictionary_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress> {
    match stmt.node_tag() {
        ntag::T_AlterTSDictionaryStmt => AlterTSDictionary(mcx, stmt.expect_altertsdictionarystmt()),
        _ => panic!("alter_ts_dictionary: parse tree is not an AlterTSDictionaryStmt"),
    }
}

/// `case T_AlterTSConfigurationStmt: AlterTSConfiguration(stmt)` (utility.c).
fn alter_ts_configuration_arm<'mcx>(mcx: Mcx<'mcx>, stmt: &Node<'mcx>) -> PgResult<ObjectAddress> {
    match stmt.node_tag() {
        ntag::T_AlterTSConfigurationStmt => {
            AlterTSConfiguration(mcx, stmt.expect_altertsconfigurationstmt())
        }
        _ => panic!("alter_ts_configuration: parse tree is not an AlterTSConfigurationStmt"),
    }
}

pub fn init_seams() {
    backend_commands_tsearchcmds_seams::deserialize_deflist::set(deserialize_deflist_seam);
    backend_commands_tsearchcmds_seams::RemoveTSConfigurationById::set(RemoveTSConfigurationById);
    // GetTSConfigTuple (tsearchcmds.c:786): get_ts_config_oid + config_form_by_oid.
    backend_commands_tsearchcmds_seams::get_ts_config_form::set(get_ts_config_form_impl);

    // ProcessUtilitySlow dispatch arms (utility.c ALTER TEXT SEARCH DICTIONARY /
    // CONFIGURATION).
    backend_tcop_utility_out_seams::alter_ts_dictionary::set(alter_ts_dictionary_arm);
    backend_tcop_utility_out_seams::alter_ts_configuration::set(alter_ts_configuration_arm);
}

/// Owner-side installer for the `deserialize_deflist` inward seam: the verbatim
/// `dictinitoption` varlena bytes a `SysCacheGetAttr` read produced cross in;
/// the owner does the C `TextDatumGetCString` detoast + conversion, then runs
/// the in-crate state machine and projects each DefElem to its `(name, value)`
/// strings.
fn deserialize_deflist_seam<'mcx>(
    mcx: Mcx<'mcx>,
    txt: &[u8],
) -> PgResult<mcx::PgVec<'mcx, DefElemString<'mcx>>> {
    use types_tuple::backend_access_common_heaptuple::Datum;

    /* TextDatumGetCString(opt): wrap the verbatim varlena bytes as a by-ref
     * Datum and detoast + copy out the payload. */
    let mut bytes = mcx::vec_with_capacity_in::<u8>(mcx, txt.len())?;
    bytes.extend_from_slice(txt);
    let d = Datum::ByRef(bytes);
    let s = backend_utils_adt_varlena_seams::text_to_cstring_v::call(mcx, &d)?;

    let pairs = deserialize_deflist_strings(mcx, s.as_str())?;
    let mut out = mcx::vec_with_capacity_in::<DefElemString>(mcx, pairs.len())?;
    for p in pairs {
        out.push(p);
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
