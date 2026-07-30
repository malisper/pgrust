// tsearchcmds.c. LOUD divergences: event-trigger collection, pg_shdepend flush
// on ALTER (owner is pinned, so C's delete+re-record of shared deps is a no-op
// here).
#![allow(non_snake_case, non_upper_case_globals)]

pub mod deflist;
#[cfg(test)]
mod tests;

use datum::Datum;
use mcx::{Mcx, PgVec};
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{AttrNumber, InvalidOid, Oid, NAMESPACE_RELATION_ID};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_OBJECT_DEFINITION, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, NOTICE,
};
use types_nodes::parsenodes::{DefElem, ObjectType, ACL_CREATE};
use types_nodes::parsenodes::DefineStmt;
use types_nodes::rawnodes::{AlterTSConfigurationStmt, AlterTSDictionaryStmt};
use types_nodes::NodeList;
use types_rel::{Relation, RowExclusiveLock};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::NameData;

use cache_syscache::{
    ReleaseSysCache, SearchSysCache1, SysCacheGetAttr, SysCacheKey, TSCONFIGOID, TSDICTOID,
    TSPARSEROID, TSTEMPLATEOID,
};
use deflist::{def_item_from_defelem, def_value_string, DefItem};

pub const TSDictionaryRelationId: Oid = 3600;
pub const TSDictionaryNameNspIndexId: Oid = 3604;
pub const TSDictionaryOidIndexId: Oid = 3605;
pub const TSParserRelationId: Oid = 3601;
pub const TSConfigRelationId: Oid = 3602;
pub const TSConfigNameNspIndexId: Oid = 3608;
pub const TSConfigOidIndexId: Oid = 3712;
pub const TSConfigMapRelationId: Oid = 3603;
pub const TSConfigMapIndexId: Oid = 3609;
pub const TSTemplateRelationId: Oid = 3764;

const Anum_pg_ts_dict_oid: usize = 1;
const Anum_pg_ts_dict_dictname: usize = 2;
const Anum_pg_ts_dict_dictnamespace: usize = 3;
const Anum_pg_ts_dict_dictowner: usize = 4;
const Anum_pg_ts_dict_dicttemplate: usize = 5;
const Anum_pg_ts_dict_dictinitoption: usize = 6;
const Natts_pg_ts_dict: usize = 6;

pub const TSParserOidIndexId: Oid = 3607;
const Anum_pg_ts_parser_oid: i32 = 1;
const Anum_pg_ts_parser_prsname: i32 = 2;
const Anum_pg_ts_parser_prsnamespace: i32 = 3;
const Anum_pg_ts_parser_prsstart: i32 = 4;
const Anum_pg_ts_parser_prstoken: i32 = 5;
const Anum_pg_ts_parser_prsend: i32 = 6;
const Anum_pg_ts_parser_prsheadline: i32 = 7;
const Anum_pg_ts_parser_prslextype: i32 = 8;
const Natts_pg_ts_parser: usize = 8;

pub const TSTemplateOidIndexId: Oid = 3767;
const Anum_pg_ts_template_oid: i32 = 1;
const Anum_pg_ts_template_tmplname: i32 = 2;
const Anum_pg_ts_template_tmplnamespace: i32 = 3;
const Anum_pg_ts_template_tmplinit: i32 = 4;
const Anum_pg_ts_template_tmpllexize: i32 = 5;
const Natts_pg_ts_template: usize = 5;

const Anum_pg_ts_config_oid: usize = 1;
const Anum_pg_ts_config_cfgname: usize = 2;
const Anum_pg_ts_config_cfgnamespace: usize = 3;
const Anum_pg_ts_config_cfgowner: usize = 4;
const Anum_pg_ts_config_cfgparser: usize = 5;
const Natts_pg_ts_config: usize = 5;

const Anum_pg_ts_config_map_mapcfg: usize = 1;
const Anum_pg_ts_config_map_maptokentype: usize = 2;
const Anum_pg_ts_config_map_mapseqno: usize = 3;
const Anum_pg_ts_config_map_mapdict: usize = 4;
const Natts_pg_ts_config_map: usize = 4;

fn oid_key(attno: usize, value: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(value);
    key
}

fn int4_key(attno: usize, value: i32) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_INT4EQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_INT4EQ) failed: {e:?}"));
    key.sk_argument = Datum::from_i32(value);
    key
}

fn name_list_parts<'mcx>(mcx: Mcx<'mcx>, names: &NodeList<'mcx>) -> PgVec<'mcx, &'mcx str> {
    let mut v = PgVec::new_in(mcx);
    for n in names.iter() {
        v.push(n.as_string().expect("name list holds Strings").sval);
    }
    v
}

fn name_list_to_string(names: &NodeList<'_>) -> String {
    let mut out = String::new();
    for (i, n) in names.iter().enumerate() {
        if i > 0 {
            out.push('.');
        }
        out.push_str(n.as_string().expect("name list holds Strings").sval);
    }
    out
}

#[track_caller]
#[cold]
fn undefined_ts_object(noun: &str, name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("text search {noun} \"{name}\" does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
    )
}

// get_ts_parser_oid / get_ts_dict_oid / get_ts_template_oid / get_ts_config_oid
// (namespace.c): explicit schema or first search-path hit, temp excluded.
fn get_ts_object_oid(cache_id: i32, noun: &str, names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    let (schemaname, objname) = catalog_namespace::DeconstructQualifiedName(names)?;
    let mut found = InvalidOid;
    if let Some(schemaname) = schemaname {
        let namespace_id = catalog_namespace::LookupExplicitNamespace(schemaname, missing_ok)?;
        if namespace_id != InvalidOid {
            found = cache_syscache::GetSysCacheOid(
                cache_id,
                1,
                SysCacheKey::Str(objname),
                SysCacheKey::Value(Datum::from_oid(namespace_id)),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )?;
        }
    } else {
        let mut path = [InvalidOid; 64];
        let n = catalog_namespace::fetch_search_path_array(&mut path)?;
        for &nsp in &path[..n] {
            found = cache_syscache::GetSysCacheOid(
                cache_id,
                1,
                SysCacheKey::Str(objname),
                SysCacheKey::Value(Datum::from_oid(nsp)),
                SysCacheKey::UNUSED,
                SysCacheKey::UNUSED,
            )?;
            if found != InvalidOid {
                break;
            }
        }
    }
    if found == InvalidOid && !missing_ok {
        return Err(undefined_ts_object(noun, &names.join(".")));
    }
    Ok(found)
}

pub fn get_ts_dict_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid(cache_syscache::TSDICTNAMENSP, "dictionary", names, missing_ok)
}

pub fn get_ts_template_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid(cache_syscache::TSTEMPLATENAMENSP, "template", names, missing_ok)
}

pub fn get_ts_config_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid(cache_syscache::TSCONFIGNAMENSP, "configuration", names, missing_ok)
}

pub fn get_ts_parser_oid(names: &[&str], missing_ok: bool) -> PgResult<Oid> {
    get_ts_object_oid(cache_syscache::TSPARSERNAMENSP, "parser", names, missing_ok)
}

// QualifiedNameGetCreationNamespace via the RangeVar walk (functioncmds shape).
fn qualified_name_get_creation_namespace<'mcx>(
    mcx: Mcx<'mcx>,
    names: &NodeList<'mcx>,
) -> PgResult<(Oid, &'mcx str)> {
    let parts = name_list_parts(mcx, names);
    let (schemaname, objname) = catalog_namespace::DeconstructQualifiedName(&parts)?;
    let rv = rel_vocab::RangeVar {
        catalogname: None,
        schemaname,
        relname: objname,
        inh: true,
        relpersistence: b'p',
        location: -1,
    };
    let nsid = catalog_namespace::RangeVarGetCreationNamespace(mcx, &rv)?;
    Ok((nsid, objname))
}

fn check_create_in_namespace(mcx: Mcx<'_>, namespaceoid: Oid) -> PgResult<()> {
    let aclresult = aclchk::object_aclcheck(
        NAMESPACE_RELATION_ID,
        namespaceoid,
        miscinit::GetUserId(),
        ACL_CREATE,
    )?;
    if aclresult != aclchk::ACLCHECK_OK {
        let nspname = lsyscache::get_namespace_name(mcx, namespaceoid)?
            .map(|s| s.to_string())
            .unwrap_or_default();
        aclchk::aclcheck_error(aclresult, ObjectType::OBJECT_SCHEMA, &nspname)?;
    }
    Ok(())
}

// object_ownercheck (aclchk.c): superuser fast path, per typecmds precedent.
fn ownercheck_or_loud(name: &str) -> PgResult<()> {
    if !superuser::superuser()? {
        // unported: object_ownercheck for non-superusers (aclchk lane)
        let _ = name;
        return Err(Box::new(
            types_error::PgError::error(
                "altering text search objects as a non-superuser is not supported yet",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    Ok(())
}

fn defGetQualifiedName<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem<'mcx>) -> PgVec<'mcx, &'mcx str> {
    let arg = defel.arg.expect("option requires an argument");
    if let Some(t) = arg.as_type_name() {
        name_list_parts(mcx, &t.names)
    } else if let Some(l) = arg.as_list() {
        name_list_parts(mcx, l)
    } else if let Some(s) = arg.as_string() {
        let mut v = PgVec::new_in(mcx);
        v.push(s.sval);
        v
    } else {
        panic!(
            "defGetQualifiedName: argument of {} must be a name",
            defel.defname.unwrap_or("")
        )
    }
}

// verify_dictoptions (tsearchcmds.c). DIVERGENCE: C suppresses the check in a
// standalone backend for initdb; this server is always under the postmaster.
fn verify_dictoptions<'mcx>(
    mcx: Mcx<'mcx>,
    tmplId: Oid,
    dictoptions: &[DefItem<'mcx>],
) -> PgResult<()> {
    let Some(tup) = SearchSysCache1(TSTEMPLATEOID, SysCacheKey::Value(Datum::from_oid(tmplId)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for text search template {tmplId}"
        ))));
    };
    let (tmplname_d, _) = SysCacheGetAttr(TSTEMPLATEOID, &tup, Anum_pg_ts_template_tmplname)?;
    let mut tmplname = NameData::default();
    // SAFETY: name-column datum points at NAMEDATALEN bytes in the tuple image.
    unsafe {
        core::ptr::copy_nonoverlapping(
            tmplname_d.as_usize() as *const u8,
            tmplname.data.as_mut_ptr(),
            types_core::NAMEDATALEN as usize,
        )
    };
    let (initmethod_d, initnull) =
        SysCacheGetAttr(TSTEMPLATEOID, &tup, Anum_pg_ts_template_tmplinit)?;
    let initmethod = if initnull { InvalidOid } else { initmethod_d.as_oid() };
    ReleaseSysCache(tup);

    if initmethod == InvalidOid {
        if !dictoptions.is_empty() {
            return Err(Box::new(
                PgError::error(format!(
                    "text search template \"{}\" does not accept options",
                    String::from_utf8_lossy(tmplname.name_str())
                ))
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
            ));
        }
        return Ok(());
    }
    call_template_init(mcx, initmethod, dictoptions).map(|_| ())
}

// OidFunctionCall1(initmethod, dictoptions) over the dict_api marshal.
pub fn call_template_init<'mcx>(
    mcx: Mcx<'mcx>,
    initmethod: Oid,
    dictoptions: &[DefItem<'mcx>],
) -> PgResult<Datum> {
    let mut options: PgVec<'mcx, (PgVec<'mcx, u8>, PgVec<'mcx, u8>)> = PgVec::new_in(mcx);
    let mut int_options: PgVec<'mcx, Option<i64>> = PgVec::new_in(mcx);
    for item in dictoptions {
        let val = def_value_string(mcx, item)?;
        let mut n: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        mcx::vec_append_bytes(&mut n, item.name.as_bytes())?;
        let mut v: PgVec<'mcx, u8> = PgVec::new_in(mcx);
        mcx::vec_append_bytes(&mut v, val.as_bytes())?;
        options.push((n, v));
        int_options.push(match item.value {
            Some(crate::deflist::DefValue::Int(i)) => Some(i as i64),
            _ => None,
        });
    }
    let initdata = ts_locale::dict_api::DictInitData { mcx, dict_options: options, int_options };
    let mut flinfo = fmgr_seams::fmgr_info::call(initmethod)?;
    types_fmgr::function_call1_coll(
        &mut flinfo,
        InvalidOid,
        Datum::from_usize(&initdata as *const _ as usize),
    )
}

fn make_dictionary_dependencies(
    mcx: Mcx<'_>,
    dictOid: Oid,
    namespaceoid: Oid,
    owner: Oid,
    templId: Oid,
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddress::set(TSDictionaryRelationId, dictOid);
    pg_depend::recordDependencyOnOwner(mcx, myself.classId, myself.objectId, owner)?;
    let mut refs = [
        ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid),
        ObjectAddress::set(TSTemplateRelationId, templId),
    ];
    pg_depend::record_object_address_dependencies(mcx, &myself, &mut refs, DependencyType::Normal)?;
    Ok(myself)
}

// CREATE TEXT SEARCH DICTIONARY
pub fn DefineTSDictionary<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &DefineStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (namespaceoid, dictname) = qualified_name_get_creation_namespace(mcx, &stmt.defnames)?;
    check_create_in_namespace(mcx, namespaceoid)?;

    let mut templId = InvalidOid;
    let mut dictoptions: PgVec<'mcx, DefItem<'mcx>> = PgVec::new_in(mcx);
    for n in stmt.definition.iter() {
        let defel = n.as_def_elem().expect("definition holds DefElems");
        if defel.defname == Some("template") {
            templId = get_ts_template_oid(&defGetQualifiedName(mcx, defel), false)?;
        } else {
            dictoptions.push(def_item_from_defelem(mcx, defel)?);
        }
    }

    if templId == InvalidOid {
        return Err(Box::new(
            PgError::error("text search template is required".to_string())
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }
    verify_dictoptions(mcx, templId, &dictoptions)?;

    let dictRel = table::table_open(mcx, TSDictionaryRelationId, RowExclusiveLock)?;
    let dictOid = catalog::GetNewOidWithIndex(
        mcx,
        &dictRel,
        TSDictionaryOidIndexId,
        Anum_pg_ts_dict_oid as AttrNumber,
    )?;

    let mut values = [Datum::null(); Natts_pg_ts_dict];
    let mut nulls = [false; Natts_pg_ts_dict];
    let mut dname = NameData::default();
    dname.namestrcpy(dictname);
    values[Anum_pg_ts_dict_oid - 1] = Datum::from_oid(dictOid);
    values[Anum_pg_ts_dict_dictname - 1] = Datum::from_usize(dname.data.as_ptr() as usize);
    values[Anum_pg_ts_dict_dictnamespace - 1] = Datum::from_oid(namespaceoid);
    values[Anum_pg_ts_dict_dictowner - 1] = Datum::from_oid(miscinit::GetUserId());
    values[Anum_pg_ts_dict_dicttemplate - 1] = Datum::from_oid(templId);
    let opt_text;
    if !dictoptions.is_empty() {
        let serialized = deflist::serialize_deflist(mcx, &dictoptions)?;
        opt_text = varlena::cstring_to_text(mcx, &serialized)?;
        values[Anum_pg_ts_dict_dictinitoption - 1] =
            Datum::from_usize(opt_text.as_bytes().as_ptr() as usize);
    } else {
        nulls[Anum_pg_ts_dict_dictinitoption - 1] = true;
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, dictRel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &dictRel, &mut tup)?;
    let address =
        make_dictionary_dependencies(mcx, dictOid, namespaceoid, miscinit::GetUserId(), templId)?;
    dictRel.close(RowExclusiveLock)?;
    Ok(address)
}

const TSQUERYOID: Oid = 3615;

#[cold]
fn func_wrong_rettype(funcname: &[&str], argtypes: &[Oid], rettype: Oid) -> PgResult<Box<PgError>> {
    let mut sig = funcname.join(".");
    sig.push('(');
    for (i, t) in argtypes.iter().enumerate() {
        if i > 0 {
            sig.push_str(", ");
        }
        sig.push_str(&format_type::format_type_be(*t)?);
    }
    sig.push(')');
    Ok(Box::new(
        PgError::error(format!(
            "function {sig} should return type {}",
            format_type::format_type_be(rettype)?
        ))
        .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
    ))
}

// get_ts_parser_func (tsearchcmds.c): signature-checked regproc lookup.
fn get_ts_parser_func<'mcx>(mcx: Mcx<'mcx>, defel: &DefElem<'mcx>, attnum: i32) -> PgResult<Oid> {
    use types_core::catalog::{INTERNALOID, INT4OID, VOIDOID};
    let funcname = defGetQualifiedName(mcx, defel);
    let mut ret_type = INTERNALOID;
    let mut type_id = [INTERNALOID; 3];
    let nargs: i16 = match attnum {
        Anum_pg_ts_parser_prsstart => {
            type_id[1] = INT4OID;
            2
        }
        Anum_pg_ts_parser_prstoken => 3,
        Anum_pg_ts_parser_prsend => {
            ret_type = VOIDOID;
            1
        }
        Anum_pg_ts_parser_prsheadline => {
            type_id[2] = TSQUERYOID;
            3
        }
        Anum_pg_ts_parser_prslextype => 1,
        other => panic!("unrecognized attribute for text search parser: {other}"),
    };
    let argtypes = &type_id[..nargs as usize];
    let proc_oid = parse_func_seams::LookupFuncName::call(&funcname, nargs, argtypes, false)?;
    if lsyscache::get_func_rettype(proc_oid)? != ret_type {
        return Err(func_wrong_rettype(&funcname, argtypes, ret_type)?);
    }
    Ok(proc_oid)
}

fn make_parser_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    prsOid: Oid,
    namespaceoid: Oid,
    values: &[Datum; Natts_pg_ts_parser],
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddress::set(TSParserRelationId, prsOid);
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;
    let mut refs: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    refs.push(ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid));
    for attnum in [
        Anum_pg_ts_parser_prsstart,
        Anum_pg_ts_parser_prstoken,
        Anum_pg_ts_parser_prsend,
        Anum_pg_ts_parser_prslextype,
        Anum_pg_ts_parser_prsheadline,
    ] {
        let func = values[attnum as usize - 1].as_oid();
        if attnum != Anum_pg_ts_parser_prsheadline || func != InvalidOid {
            refs.push(ObjectAddress::set(types_core::PROCEDURE_RELATION_ID, func));
        }
    }
    pg_depend::record_object_address_dependencies(mcx, &myself, &mut refs, DependencyType::Normal)?;
    Ok(myself)
}

// CREATE TEXT SEARCH PARSER
pub fn DefineTSParser<'mcx>(mcx: Mcx<'mcx>, stmt: &DefineStmt<'mcx>) -> PgResult<ObjectAddress> {
    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error("must be superuser to create text search parsers".to_string())
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    let (namespaceoid, prsname) = qualified_name_get_creation_namespace(mcx, &stmt.defnames)?;

    let prsRel = table::table_open(mcx, TSParserRelationId, RowExclusiveLock)?;
    let prsOid = catalog::GetNewOidWithIndex(
        mcx,
        &prsRel,
        TSParserOidIndexId,
        Anum_pg_ts_parser_oid as AttrNumber,
    )?;

    let mut values = [Datum::null(); Natts_pg_ts_parser];
    let nulls = [false; Natts_pg_ts_parser];
    let mut pname = NameData::default();
    pname.namestrcpy(prsname);
    values[Anum_pg_ts_parser_oid as usize - 1] = Datum::from_oid(prsOid);
    values[Anum_pg_ts_parser_prsname as usize - 1] =
        Datum::from_usize(pname.data.as_ptr() as usize);
    values[Anum_pg_ts_parser_prsnamespace as usize - 1] = Datum::from_oid(namespaceoid);

    for n in stmt.definition.iter() {
        let defel = n.as_def_elem().expect("definition holds DefElems");
        let attnum = match defel.defname {
            Some("start") => Anum_pg_ts_parser_prsstart,
            Some("gettoken") => Anum_pg_ts_parser_prstoken,
            Some("end") => Anum_pg_ts_parser_prsend,
            Some("headline") => Anum_pg_ts_parser_prsheadline,
            Some("lextypes") => Anum_pg_ts_parser_prslextype,
            other => {
                return Err(Box::new(
                    PgError::error(format!(
                        "text search parser parameter \"{}\" not recognized",
                        other.unwrap_or("")
                    ))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        };
        values[attnum as usize - 1] = Datum::from_oid(get_ts_parser_func(mcx, defel, attnum)?);
    }

    for (attnum, what) in [
        (Anum_pg_ts_parser_prsstart, "start"),
        (Anum_pg_ts_parser_prstoken, "gettoken"),
        (Anum_pg_ts_parser_prsend, "end"),
        (Anum_pg_ts_parser_prslextype, "lextypes"),
    ] {
        if values[attnum as usize - 1].as_oid() == InvalidOid {
            return Err(Box::new(
                PgError::error(format!("text search parser {what} method is required"))
                    .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
            ));
        }
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, prsRel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &prsRel, &mut tup)?;
    let address = make_parser_dependencies(mcx, prsOid, namespaceoid, &values)?;
    prsRel.close(RowExclusiveLock)?;
    Ok(address)
}

// get_ts_template_func (tsearchcmds.c).
fn get_ts_template_func<'mcx>(
    mcx: Mcx<'mcx>,
    defel: &DefElem<'mcx>,
    attnum: i32,
) -> PgResult<Oid> {
    use types_core::catalog::INTERNALOID;
    let funcname = defGetQualifiedName(mcx, defel);
    let type_id = [INTERNALOID; 4];
    let nargs: i16 = match attnum {
        Anum_pg_ts_template_tmplinit => 1,
        Anum_pg_ts_template_tmpllexize => 4,
        other => panic!("unrecognized attribute for text search template: {other}"),
    };
    let argtypes = &type_id[..nargs as usize];
    let proc_oid = parse_func_seams::LookupFuncName::call(&funcname, nargs, argtypes, false)?;
    if lsyscache::get_func_rettype(proc_oid)? != INTERNALOID {
        return Err(func_wrong_rettype(&funcname, argtypes, INTERNALOID)?);
    }
    Ok(proc_oid)
}

fn make_ts_template_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    tmplOid: Oid,
    namespaceoid: Oid,
    tmplinit: Oid,
    tmpllexize: Oid,
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddress::set(TSTemplateRelationId, tmplOid);
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;
    let mut refs: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    refs.push(ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid));
    refs.push(ObjectAddress::set(types_core::PROCEDURE_RELATION_ID, tmpllexize));
    if tmplinit != InvalidOid {
        refs.push(ObjectAddress::set(types_core::PROCEDURE_RELATION_ID, tmplinit));
    }
    pg_depend::record_object_address_dependencies(mcx, &myself, &mut refs, DependencyType::Normal)?;
    Ok(myself)
}

// CREATE TEXT SEARCH TEMPLATE
pub fn DefineTSTemplate<'mcx>(mcx: Mcx<'mcx>, stmt: &DefineStmt<'mcx>) -> PgResult<ObjectAddress> {
    if !superuser::superuser()? {
        return Err(Box::new(
            PgError::error("must be superuser to create text search templates".to_string())
                .with_sqlstate(types_error::ERRCODE_INSUFFICIENT_PRIVILEGE),
        ));
    }
    let (namespaceoid, tmplname) = qualified_name_get_creation_namespace(mcx, &stmt.defnames)?;

    let tmplRel = table::table_open(mcx, TSTemplateRelationId, RowExclusiveLock)?;
    let tmplOid = catalog::GetNewOidWithIndex(
        mcx,
        &tmplRel,
        TSTemplateOidIndexId,
        Anum_pg_ts_template_oid as AttrNumber,
    )?;

    let mut values = [Datum::null(); Natts_pg_ts_template];
    let nulls = [false; Natts_pg_ts_template];
    let mut tname = NameData::default();
    tname.namestrcpy(tmplname);
    values[Anum_pg_ts_template_oid as usize - 1] = Datum::from_oid(tmplOid);
    values[Anum_pg_ts_template_tmplname as usize - 1] =
        Datum::from_usize(tname.data.as_ptr() as usize);
    values[Anum_pg_ts_template_tmplnamespace as usize - 1] = Datum::from_oid(namespaceoid);

    for n in stmt.definition.iter() {
        let defel = n.as_def_elem().expect("definition holds DefElems");
        let attnum = match defel.defname {
            Some("init") => Anum_pg_ts_template_tmplinit,
            Some("lexize") => Anum_pg_ts_template_tmpllexize,
            other => {
                return Err(Box::new(
                    PgError::error(format!(
                        "text search template parameter \"{}\" not recognized",
                        other.unwrap_or("")
                    ))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        };
        values[attnum as usize - 1] = Datum::from_oid(get_ts_template_func(mcx, defel, attnum)?);
    }

    if values[Anum_pg_ts_template_tmpllexize as usize - 1].as_oid() == InvalidOid {
        return Err(Box::new(
            PgError::error("text search template lexize method is required".to_string())
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, tmplRel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &tmplRel, &mut tup)?;
    let address = make_ts_template_dependencies(
        mcx,
        tmplOid,
        namespaceoid,
        values[Anum_pg_ts_template_tmplinit as usize - 1].as_oid(),
        values[Anum_pg_ts_template_tmpllexize as usize - 1].as_oid(),
    )?;
    tmplRel.close(RowExclusiveLock)?;
    Ok(address)
}

// Payload of a non-null in-tuple text datum (dictinitoption is never
// external at the sizes CREATE accepts; a toast pointer is loud).
fn text_datum_bytes<'mcx>(mcx: Mcx<'mcx>, d: Datum) -> PgResult<PgVec<'mcx, u8>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: heap_getattr returned a live in-tuple varlena pointer.
    let total = unsafe {
        let b0 = *p;
        if b0 == 0x01 {
            panic!("tsearchcmds: external/toasted dictinitoption unhandled");
        } else if b0 & 0x01 == 0x01 {
            ((b0 >> 1) & 0x7F) as usize
        } else {
            datum::VarlenaRef::from_ptr(p).varsize()
        }
    };
    // SAFETY: total is the image's own declared size.
    let image = unsafe { core::slice::from_raw_parts(p, total) };
    let payload = varlena::open_image(mcx, image)?;
    let mut out = mcx::vec_with_capacity_in(mcx, payload.as_bytes().len())?;
    mcx::vec_append_bytes(&mut out, payload.as_bytes())?;
    Ok(out)
}

// ALTER TEXT SEARCH DICTIONARY
pub fn AlterTSDictionary<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSDictionaryStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let dictId = get_ts_dict_oid(&name_list_parts(mcx, &stmt.dictname), false)?;
    let rel = table::table_open(mcx, TSDictionaryRelationId, RowExclusiveLock)?;
    let Some(tup) = SearchSysCache1(TSDICTOID, SysCacheKey::Value(Datum::from_oid(dictId)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for text search dictionary {dictId}"
        ))));
    };
    ownercheck_or_loud(&name_list_to_string(&stmt.dictname))?;

    let (opt, isnull) = SysCacheGetAttr(TSDICTOID, &tup, Anum_pg_ts_dict_dictinitoption as i32)?;
    let mut dictoptions: PgVec<'mcx, DefItem<'mcx>> = if isnull {
        PgVec::new_in(mcx)
    } else {
        deflist::deserialize_deflist(mcx, &text_datum_bytes(mcx, opt)?)?
    };

    for n in stmt.options.iter() {
        let defel = n.as_def_elem().expect("options hold DefElems");
        let name = defel.defname.unwrap_or("");
        dictoptions.retain(|old| old.name != name);
        if defel.arg.is_some() {
            dictoptions.push(def_item_from_defelem(mcx, defel)?);
        }
    }

    let dicttemplate =
        SysCacheGetAttr(TSDICTOID, &tup, Anum_pg_ts_dict_dicttemplate as i32)?.0.as_oid();
    verify_dictoptions(mcx, dicttemplate, &dictoptions)?;

    let mut repl_val = [Datum::null(); Natts_pg_ts_dict];
    let mut repl_null = [false; Natts_pg_ts_dict];
    let mut repl_repl = [false; Natts_pg_ts_dict];
    let opt_text;
    if !dictoptions.is_empty() {
        let serialized = deflist::serialize_deflist(mcx, &dictoptions)?;
        opt_text = varlena::cstring_to_text(mcx, &serialized)?;
        repl_val[Anum_pg_ts_dict_dictinitoption - 1] =
            Datum::from_usize(opt_text.as_bytes().as_ptr() as usize);
    } else {
        repl_null[Anum_pg_ts_dict_dictinitoption - 1] = true;
    }
    repl_repl[Anum_pg_ts_dict_dictinitoption - 1] = true;

    let old = tup.tuple();
    let otid = old.t_self;
    let mut newtup =
        heaptuple::heap_modify_tuple(mcx, &old, rel.descr(), &repl_val, &repl_null, &repl_repl)?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
    ReleaseSysCache(tup);
    rel.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(TSDictionaryRelationId, dictId))
}

fn make_configuration_dependencies<'mcx>(
    mcx: Mcx<'mcx>,
    cfgOid: Oid,
    namespaceoid: Oid,
    owner: Oid,
    prsOid: Oid,
    remove_old: bool,
    mapRel: Option<&Relation<'mcx>>,
) -> PgResult<ObjectAddress> {
    let myself = ObjectAddress::set(TSConfigRelationId, cfgOid);
    if remove_old {
        pg_depend::deleteDependencyRecordsFor(mcx, myself.classId, myself.objectId, true)?;
        // C also flushes pg_shdepend; the pinned owner recorded nothing there.
    }
    pg_depend::recordDependencyOnOwner(mcx, myself.classId, myself.objectId, owner)?;

    let mut refs: PgVec<'mcx, ObjectAddress> = PgVec::new_in(mcx);
    refs.push(ObjectAddress::set(NAMESPACE_RELATION_ID, namespaceoid));
    refs.push(ObjectAddress::set(TSParserRelationId, prsOid));
    if let Some(mapRel) = mapRel {
        xact::CommandCounterIncrement()?;
        let keys = [oid_key(Anum_pg_ts_config_map_mapcfg, cfgOid)];
        let mut scan =
            genam::systable_beginscan(mcx, mapRel, TSConfigMapIndexId, true, None, &keys)?;
        while let Some(t) = genam::systable_getnext(mcx, &mut scan)? {
            let mut isnull = false;
            // SAFETY: mapdict is a fixed NOT NULL pg_ts_config_map column.
            let mapdict = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_mapdict as i32,
                    mapRel.descr(),
                    &mut isnull,
                )
            }
            .as_oid();
            refs.push(ObjectAddress::set(TSDictionaryRelationId, mapdict));
        }
        genam::systable_endscan(mcx, scan)?;
    }
    pg_depend::record_object_address_dependencies(mcx, &myself, &mut refs, DependencyType::Normal)?;
    Ok(myself)
}

// CREATE TEXT SEARCH CONFIGURATION
pub fn DefineTSConfiguration<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &DefineStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let (namespaceoid, cfgname) = qualified_name_get_creation_namespace(mcx, &stmt.defnames)?;
    check_create_in_namespace(mcx, namespaceoid)?;

    let mut sourceOid = InvalidOid;
    let mut prsOid = InvalidOid;
    for n in stmt.definition.iter() {
        let defel = n.as_def_elem().expect("definition holds DefElems");
        match defel.defname {
            Some("parser") => prsOid = get_ts_parser_oid(&defGetQualifiedName(mcx, defel), false)?,
            Some("copy") => sourceOid = get_ts_config_oid(&defGetQualifiedName(mcx, defel), false)?,
            other => {
                return Err(Box::new(
                    PgError::error(format!(
                        "text search configuration parameter \"{}\" not recognized",
                        other.unwrap_or("")
                    ))
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR),
                ))
            }
        }
    }
    if sourceOid != InvalidOid && prsOid != InvalidOid {
        return Err(Box::new(
            PgError::error("cannot specify both PARSER and COPY options".to_string())
                .with_sqlstate(ERRCODE_SYNTAX_ERROR),
        ));
    }
    if sourceOid != InvalidOid {
        let Some(tup) =
            SearchSysCache1(TSCONFIGOID, SysCacheKey::Value(Datum::from_oid(sourceOid)))?
        else {
            return Err(Box::new(PgError::error(format!(
                "cache lookup failed for text search configuration {sourceOid}"
            ))));
        };
        prsOid = SysCacheGetAttr(TSCONFIGOID, &tup, Anum_pg_ts_config_cfgparser as i32)?
            .0
            .as_oid();
        ReleaseSysCache(tup);
    }
    if prsOid == InvalidOid {
        return Err(Box::new(
            PgError::error("text search parser is required".to_string())
                .with_sqlstate(ERRCODE_INVALID_OBJECT_DEFINITION),
        ));
    }

    let cfgRel = table::table_open(mcx, TSConfigRelationId, RowExclusiveLock)?;
    let cfgOid = catalog::GetNewOidWithIndex(
        mcx,
        &cfgRel,
        TSConfigOidIndexId,
        Anum_pg_ts_config_oid as AttrNumber,
    )?;

    let mut values = [Datum::null(); Natts_pg_ts_config];
    let nulls = [false; Natts_pg_ts_config];
    let mut cname = NameData::default();
    cname.namestrcpy(cfgname);
    values[Anum_pg_ts_config_oid - 1] = Datum::from_oid(cfgOid);
    values[Anum_pg_ts_config_cfgname - 1] = Datum::from_usize(cname.data.as_ptr() as usize);
    values[Anum_pg_ts_config_cfgnamespace - 1] = Datum::from_oid(namespaceoid);
    values[Anum_pg_ts_config_cfgowner - 1] = Datum::from_oid(miscinit::GetUserId());
    values[Anum_pg_ts_config_cfgparser - 1] = Datum::from_oid(prsOid);
    let mut tup = heaptuple::heap_form_tuple(mcx, cfgRel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &cfgRel, &mut tup)?;

    let mut mapRel: Option<Relation<'mcx>> = None;
    if sourceOid != InvalidOid {
        // C batches through CatalogTuplesMultiInsertWithInfo; per-row inserts
        // with shared index state write identical rows (pg_enum precedent).
        let rel = table::table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;
        let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, &rel)?;
        let keys = [oid_key(Anum_pg_ts_config_map_mapcfg, sourceOid)];
        let mut scan = genam::systable_beginscan(mcx, &rel, TSConfigMapIndexId, true, None, &keys)?;
        let mut rows: PgVec<'mcx, (i32, i32, Oid)> = PgVec::new_in(mcx);
        while let Some(t) = genam::systable_getnext(mcx, &mut scan)? {
            let mut isnull = false;
            // SAFETY (each): fixed NOT NULL pg_ts_config_map columns.
            let toktype = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_maptokentype as i32,
                    rel.descr(),
                    &mut isnull,
                )
            }
            .as_i32();
            let seqno = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_mapseqno as i32,
                    rel.descr(),
                    &mut isnull,
                )
            }
            .as_i32();
            let dict = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_mapdict as i32,
                    rel.descr(),
                    &mut isnull,
                )
            }
            .as_oid();
            rows.push((toktype, seqno, dict));
        }
        genam::systable_endscan(mcx, scan)?;
        for (toktype, seqno, dict) in rows.iter() {
            insert_map_row(mcx, &rel, &mut indstate, cfgOid, *toktype, *seqno, *dict)?;
        }
        catalog_indexing::CatalogCloseIndexes(indstate)?;
        mapRel = Some(rel);
    }

    let address = make_configuration_dependencies(
        mcx,
        cfgOid,
        namespaceoid,
        miscinit::GetUserId(),
        prsOid,
        false,
        mapRel.as_ref(),
    )?;
    if let Some(rel) = mapRel {
        rel.close(RowExclusiveLock)?;
    }
    cfgRel.close(RowExclusiveLock)?;
    Ok(address)
}

fn insert_map_row<'mcx>(
    mcx: Mcx<'mcx>,
    relMap: &Relation<'mcx>,
    indstate: &mut catalog_indexing::CatalogIndexState<'mcx>,
    cfgId: Oid,
    toktype: i32,
    seqno: i32,
    dict: Oid,
) -> PgResult<()> {
    let mut values = [Datum::null(); Natts_pg_ts_config_map];
    let nulls = [false; Natts_pg_ts_config_map];
    values[Anum_pg_ts_config_map_mapcfg - 1] = Datum::from_oid(cfgId);
    values[Anum_pg_ts_config_map_maptokentype - 1] = Datum::from_i32(toktype);
    values[Anum_pg_ts_config_map_mapseqno - 1] = Datum::from_i32(seqno);
    values[Anum_pg_ts_config_map_mapdict - 1] = Datum::from_oid(dict);
    let mut tup = heaptuple::heap_form_tuple(mcx, relMap.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsertWithInfo(mcx, relMap, &mut tup, indstate)
}

struct TokenType<'a> {
    num: i32,
    name: &'a str,
}

// getTokenTypes (tsearchcmds.c). DIVERGENCE: C resolves the parser through
// lookup_ts_parser_cache; this cold DDL path reads pg_ts_parser directly.
// Contract with wparser_def: prslextype returns a pointer Datum to a
// caller-owned Vec<LexDescr>.
fn getTokenTypes<'mcx>(
    mcx: Mcx<'mcx>,
    prsId: Oid,
    tokennames: &NodeList<'mcx>,
) -> PgResult<PgVec<'mcx, TokenType<'mcx>>> {
    let mut result: PgVec<'mcx, TokenType<'mcx>> = PgVec::new_in(mcx);
    if tokennames.is_nil() {
        return Ok(result);
    }
    let Some(tup) = SearchSysCache1(TSPARSEROID, SysCacheKey::Value(Datum::from_oid(prsId)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for text search parser {prsId}"
        ))));
    };
    let lextype = SysCacheGetAttr(TSPARSEROID, &tup, Anum_pg_ts_parser_prslextype)?.0.as_oid();
    ReleaseSysCache(tup);
    if lextype == InvalidOid {
        return Err(Box::new(PgError::error(format!(
            "method lextype isn't defined for text search parser {prsId}"
        ))));
    }
    let mut flinfo = fmgr_seams::fmgr_info::call(lextype)?;
    let list = types_fmgr::function_call1_coll(&mut flinfo, InvalidOid, Datum::from_i64(0))?;
    // wparser_def contract: pointer Datum to a caller-owned Vec<LexDescr>.
    // SAFETY: produced by fc_prsd_lextype's Box::into_raw immediately above.
    let descrs: Box<Vec<ts_locale::LexDescr>> =
        unsafe { Box::from_raw(list.as_usize() as *mut Vec<ts_locale::LexDescr>) };

    'names: for tn in tokennames.iter() {
        let val = tn.as_string().expect("tokentype list holds Strings").sval;
        if result.iter().any(|t| t.name == val) {
            continue;
        }
        for entry in descrs.iter() {
            if entry.lexid != 0 && entry.alias == val {
                result.push(TokenType { num: entry.lexid, name: val });
                continue 'names;
            }
        }
        return Err(Box::new(
            PgError::error(format!("token type \"{val}\" does not exist"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    Ok(result)
}

// ALTER TEXT SEARCH CONFIGURATION ADD/ALTER MAPPING
fn MakeConfigurationMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
    cfgId: Oid,
    prsId: Oid,
    relMap: &Relation<'mcx>,
) -> PgResult<()> {
    let tokens = getTokenTypes(mcx, prsId, &stmt.tokentype)?;

    if stmt.r#override {
        for ts in &tokens {
            let keys = [
                oid_key(Anum_pg_ts_config_map_mapcfg, cfgId),
                int4_key(Anum_pg_ts_config_map_maptokentype, ts.num),
            ];
            let mut scan =
                genam::systable_beginscan(mcx, relMap, TSConfigMapIndexId, true, None, &keys)?;
            while let Some(t) = genam::systable_getnext(mcx, &mut scan)? {
                let tid = t.t_self;
                catalog_indexing::CatalogTupleDelete(relMap, &tid)?;
            }
            genam::systable_endscan(mcx, scan)?;
        }
    }

    let mut dictIds: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    for c in stmt.dicts.iter() {
        let names = c.as_list().expect("dicts holds name Lists");
        dictIds.push(get_ts_dict_oid(&name_list_parts(mcx, names), false)?);
    }

    let mut indstate = catalog_indexing::CatalogOpenIndexes(mcx, relMap)?;
    if stmt.replace {
        let dictOld = dictIds[0];
        let dictNew = dictIds[1];
        let keys = [oid_key(Anum_pg_ts_config_map_mapcfg, cfgId)];
        let mut scan =
            genam::systable_beginscan(mcx, relMap, TSConfigMapIndexId, true, None, &keys)?;
        let mut updates: PgVec<'mcx, (types_tuple::ItemPointerData, i32, i32)> = PgVec::new_in(mcx);
        while let Some(t) = genam::systable_getnext(mcx, &mut scan)? {
            let mut isnull = false;
            // SAFETY (each): fixed NOT NULL pg_ts_config_map columns.
            let toktype = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_maptokentype as i32,
                    relMap.descr(),
                    &mut isnull,
                )
            }
            .as_i32();
            let seqno = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_mapseqno as i32,
                    relMap.descr(),
                    &mut isnull,
                )
            }
            .as_i32();
            let mapdict = unsafe {
                types_tuple::heap_getattr(
                    t,
                    Anum_pg_ts_config_map_mapdict as i32,
                    relMap.descr(),
                    &mut isnull,
                )
            }
            .as_oid();
            if !tokens.is_empty() && !tokens.iter().any(|ts| ts.num == toktype) {
                continue;
            }
            if mapdict == dictOld {
                updates.push((t.t_self, toktype, seqno));
            }
        }
        genam::systable_endscan(mcx, scan)?;
        for (tid, toktype, seqno) in updates.iter() {
            // C heap_modify_tuple replaces one column; rebuilding the 4-column
            // fixed-width row writes the identical image.
            let mut values = [Datum::null(); Natts_pg_ts_config_map];
            let nulls = [false; Natts_pg_ts_config_map];
            values[Anum_pg_ts_config_map_mapcfg - 1] = Datum::from_oid(cfgId);
            values[Anum_pg_ts_config_map_maptokentype - 1] = Datum::from_i32(*toktype);
            values[Anum_pg_ts_config_map_mapseqno - 1] = Datum::from_i32(*seqno);
            values[Anum_pg_ts_config_map_mapdict - 1] = Datum::from_oid(dictNew);
            let mut newtup = heaptuple::heap_form_tuple(mcx, relMap.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleUpdateWithInfo(
                mcx,
                relMap,
                tid,
                &mut newtup,
                &mut indstate,
            )?;
        }
    } else {
        for ts in &tokens {
            for (j, dict) in dictIds.iter().enumerate() {
                insert_map_row(mcx, relMap, &mut indstate, cfgId, ts.num, (j + 1) as i32, *dict)?;
            }
        }
    }
    catalog_indexing::CatalogCloseIndexes(indstate)?;
    Ok(())
}

// ALTER TEXT SEARCH CONFIGURATION DROP MAPPING
fn DropConfigurationMapping<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
    cfgId: Oid,
    prsId: Oid,
    relMap: &Relation<'mcx>,
) -> PgResult<()> {
    let tokens = getTokenTypes(mcx, prsId, &stmt.tokentype)?;
    for ts in &tokens {
        let keys = [
            oid_key(Anum_pg_ts_config_map_mapcfg, cfgId),
            int4_key(Anum_pg_ts_config_map_maptokentype, ts.num),
        ];
        let mut scan =
            genam::systable_beginscan(mcx, relMap, TSConfigMapIndexId, true, None, &keys)?;
        let mut found = false;
        while let Some(t) = genam::systable_getnext(mcx, &mut scan)? {
            let tid = t.t_self;
            catalog_indexing::CatalogTupleDelete(relMap, &tid)?;
            found = true;
        }
        genam::systable_endscan(mcx, scan)?;
        if !found {
            if !stmt.missing_ok {
                return Err(Box::new(
                    PgError::error(format!(
                        "mapping for token type \"{}\" does not exist",
                        ts.name
                    ))
                    .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
                ));
            }
            elog_seams::ereport_msg::call(
                NOTICE,
                format!("mapping for token type \"{}\" does not exist, skipping", ts.name),
                None,
            )?;
        }
    }
    Ok(())
}

// ALTER TEXT SEARCH CONFIGURATION
pub fn AlterTSConfiguration<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterTSConfigurationStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let cfgId = get_ts_config_oid(&name_list_parts(mcx, &stmt.cfgname), true)?;
    if cfgId == InvalidOid {
        return Err(Box::new(
            PgError::error(format!(
                "text search configuration \"{}\" does not exist",
                name_list_to_string(&stmt.cfgname)
            ))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    ownercheck_or_loud(&name_list_to_string(&stmt.cfgname))?;

    let Some(tup) = SearchSysCache1(TSCONFIGOID, SysCacheKey::Value(Datum::from_oid(cfgId)))?
    else {
        return Err(Box::new(PgError::error(format!(
            "cache lookup failed for text search configuration {cfgId}"
        ))));
    };
    let prsId = SysCacheGetAttr(TSCONFIGOID, &tup, Anum_pg_ts_config_cfgparser as i32)?.0.as_oid();
    let cfgnamespace =
        SysCacheGetAttr(TSCONFIGOID, &tup, Anum_pg_ts_config_cfgnamespace as i32)?.0.as_oid();
    let cfgowner =
        SysCacheGetAttr(TSCONFIGOID, &tup, Anum_pg_ts_config_cfgowner as i32)?.0.as_oid();
    ReleaseSysCache(tup);

    let relMap = table::table_open(mcx, TSConfigMapRelationId, RowExclusiveLock)?;
    if !stmt.dicts.is_nil() {
        MakeConfigurationMapping(mcx, stmt, cfgId, prsId, &relMap)?;
    } else if !stmt.tokentype.is_nil() {
        DropConfigurationMapping(mcx, stmt, cfgId, prsId, &relMap)?;
    }

    make_configuration_dependencies(mcx, cfgId, cfgnamespace, cfgowner, prsId, true, Some(&relMap))?;
    relMap.close(RowExclusiveLock)?;
    Ok(ObjectAddress::set(TSConfigRelationId, cfgId))
}

