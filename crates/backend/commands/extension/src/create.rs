// CREATE EXTENSION worker + pg_extension row DML (extension.c:1784-2322).
use elog::ereport;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, EXTENSION_RELATION_ID, NAMESPACE_RELATION_ID};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_RECURSION,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_UNDEFINED_OBJECT, ERRCODE_UNDEFINED_SCHEMA,
    ERROR, NOTICE,
};
use types_nodes::parsenodes::{CreateSchemaStmt, DefElem};
use types_nodes::rawnodes::CreateExtensionStmt;
use types_rel::RowExclusiveLock;

use datum::Datum;
use pg_depend::ObjectAddress;
use types_tuple::NameData;

use crate::control::{
    get_extension_script_filename, read_extension_aux_control_file, read_extension_control_file,
};
use crate::graph::{find_install_path, get_ext_ver_info, get_ext_ver_list};
use crate::script::execute_extension_script;
use crate::{
    check_valid_extension_name, check_valid_version_name, get_extension_oid, get_extension_schema,
    Anum_pg_extension_oid, ExtensionOidIndexId, Natts_pg_extension,
};

#[track_caller]
fn here(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

fn str_in<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<&'mcx str> {
    let bytes = mcx::slice_borrow_in(mcx, s.as_bytes())?;
    // SAFETY: byte-for-byte copy of a &str.
    Ok(unsafe { core::str::from_utf8_unchecked(bytes) })
}

pub struct CreateExtensionOptions<'a> {
    pub schema_name: Option<&'a str>,
    pub version_name: Option<&'a str>,
    pub cascade: bool,
}

// CreateExtensionInternal (extension.c:1784-2017). `parents` guards CASCADE
// recursion cycles.
fn CreateExtensionInternal(
    mcx: Mcx<'_>,
    extension_name: &str,
    schema_name: Option<&str>,
    version_name: Option<&str>,
    cascade: bool,
    parents: &[&str],
    is_create: bool,
) -> PgResult<ObjectAddress> {
    let orig_schema_name = schema_name;
    let mut schema_oid = InvalidOid;
    let extowner = miscinit::GetUserId();

    let pcontrol = read_extension_control_file(extension_name)?;

    let version_name = match version_name {
        Some(v) => v.to_string(),
        None => match &pcontrol.default_version {
            Some(v) => v.clone(),
            None => {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg("version to install must be specified")
                    .into_error()
                    .into());
            }
        },
    };
    check_valid_version_name(&version_name)?;

    let filename = get_extension_script_filename(&pcontrol, None, &version_name);
    let (version_name, update_versions) = if std::fs::metadata(&filename).is_ok() {
        (version_name, Vec::new())
    } else {
        let mut evi_list = get_ext_ver_list(&pcontrol)?;
        let evi_target = get_ext_ver_info(&version_name, &mut evi_list);
        let mut update_versions = Vec::new();
        let Some(evi_start) = find_install_path(&mut evi_list, evi_target, &mut update_versions)
        else {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "extension \"{}\" has no installation script nor update path for version \"{version_name}\"",
                    pcontrol.name
                ))
                .into_error()
                .into());
        };
        (evi_list[evi_start].name.clone(), update_versions)
    };

    let control = read_extension_aux_control_file(&pcontrol, &version_name)?;

    let mut schema_name: Option<String> = schema_name.map(str::to_string);
    if let Some(name) = &schema_name {
        schema_oid = catalog_namespace::get_namespace_oid(name, false)?;
    }

    if let Some(control_schema) = &control.schema {
        if let Some(name) = &schema_name {
            if control_schema != name && !cascade {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg(format!(
                        "extension \"{}\" must be installed in schema \"{control_schema}\"",
                        control.name
                    ))
                    .into_error()
                    .into());
            }
        }

        schema_name = Some(control_schema.clone());
        let name = schema_name.as_ref().expect("just set");
        schema_oid = catalog_namespace::get_namespace_oid(name, true)?;

        if schema_oid == InvalidOid {
            let stmt = CreateSchemaStmt {
                schemaname: Some(str_in(mcx, name)?),
                authrole: None,
                schemaElts: types_nodes::NodeList::default(),
                if_not_exists: false,
            };
            // No elements; event-trigger collection of the generated schema
            // stays with the extension collection lane (pre-existing scope).
            schemacmds::CreateSchemaCommand(mcx, &stmt, &mut |_, _, _| Ok(()))?;
            // CreateSchemaCommand includes CommandCounterIncrement.
            schema_oid = catalog_namespace::get_namespace_oid(name, false)?;
        }
    } else if schema_oid == InvalidOid {
        let search_path = catalog_namespace::fetch_search_path(mcx, false)?;
        if search_path.is_empty() {
            return Err(no_schema_selected());
        }
        schema_oid = search_path[0];
        let name = lsyscache::get_namespace_name(mcx, schema_oid)?;
        match name {
            Some(n) => schema_name = Some(n.as_str().to_string()),
            None => return Err(no_schema_selected()),
        }
    }
    let schema_name = schema_name.expect("resolved above");

    if catalog_namespace::isTempNamespace(schema_oid) {
        xact::OrMyXactFlags(types_core::xact::XACT_FLAGS_ACCESSEDTEMPNAMESPACE);
    }

    let mut required_extensions: Vec<Oid> = Vec::new();
    let mut required_schemas: Vec<Oid> = Vec::new();
    for curreq in &control.requires {
        let reqext = get_required_extension(
            mcx,
            curreq,
            extension_name,
            orig_schema_name,
            cascade,
            parents,
            is_create,
        )?;
        let reqschema = get_extension_schema(reqext)?;
        required_extensions.push(reqext);
        required_schemas.push(reqschema);
    }

    let address = InsertExtensionTuple(
        mcx,
        &control.name,
        extowner,
        schema_oid,
        control.relocatable,
        &version_name,
        None,
        None,
        &required_extensions,
    )?;
    let extension_oid = address.objectId;

    if let Some(comment) = &control.comment {
        commands_comment::CreateComments(
            mcx,
            extension_oid,
            EXTENSION_RELATION_ID,
            0,
            Some(comment),
        )?;
    }

    execute_extension_script(
        mcx,
        extension_oid,
        &control,
        None,
        &version_name,
        &required_schemas,
        &schema_name,
    )?;

    crate::alter::ApplyExtensionUpdates(
        mcx,
        extension_oid,
        &pcontrol,
        &version_name,
        &update_versions,
        orig_schema_name,
        cascade,
        is_create,
    )?;

    Ok(address)
}

#[cold]
fn no_schema_selected() -> Box<types_error::PgError> {
    Box::new(
        ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_SCHEMA)
            .errmsg("no schema has been selected to create in")
            .into_error(),
    )
}

// get_required_extension (extension.c:2023-2088).
pub(crate) fn get_required_extension(
    mcx: Mcx<'_>,
    req_extension_name: &str,
    extension_name: &str,
    orig_schema_name: Option<&str>,
    cascade: bool,
    parents: &[&str],
    is_create: bool,
) -> PgResult<Oid> {
    let req_extension_oid = get_extension_oid(req_extension_name, true)?;
    if req_extension_oid != InvalidOid {
        return Ok(req_extension_oid);
    }

    if !cascade {
        let mut b = ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("required extension \"{req_extension_name}\" is not installed"));
        if is_create {
            b = b.errhint("Use CREATE EXTENSION ... CASCADE to install required extensions too.");
        }
        return Err(b.into_error().into());
    }

    check_valid_extension_name(req_extension_name)?;

    for pname in parents {
        if *pname == req_extension_name {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_INVALID_RECURSION)
                .errmsg(format!(
                    "cyclic dependency detected between extensions \"{req_extension_name}\" and \"{extension_name}\""
                ))
                .into_error()
                .into());
        }
    }

    ereport(NOTICE)
        .errmsg(format!("installing required extension \"{req_extension_name}\""))
        .finish(here("get_required_extension"))?;

    let mut cascade_parents: Vec<&str> = parents.to_vec();
    cascade_parents.push(extension_name);

    let addr = CreateExtensionInternal(
        mcx,
        req_extension_name,
        orig_schema_name,
        None,
        cascade,
        &cascade_parents,
        is_create,
    )?;
    Ok(addr.objectId)
}

// CreateExtension (extension.c:2094-2176).
pub fn CreateExtension<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &CreateExtensionStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let extname = stmt.extname.expect("grammar always supplies extname");
    check_valid_extension_name(extname)?;

    if get_extension_oid(extname, true)? != InvalidOid {
        if stmt.if_not_exists {
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("extension \"{extname}\" already exists, skipping"))
                .finish(here("CreateExtension"))?;
            return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
        }
        return Err(ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("extension \"{extname}\" already exists"))
            .into_error()
            .into());
    }

    if pg_depend::creating_extension() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("nested CREATE EXTENSION is not supported")
            .into_error()
            .into());
    }

    let mut d_schema: Option<&DefElem<'_>> = None;
    let mut d_new_version: Option<&DefElem<'_>> = None;
    let mut d_cascade: Option<&DefElem<'_>> = None;
    let mut schema_name: Option<&str> = None;
    let mut version_name: Option<&str> = None;
    let mut cascade = false;

    for opt in stmt.options.iter() {
        let defel = opt.as_def_elem().expect("options are DefElems");
        match defel.defname.unwrap_or("") {
            "schema" => {
                if d_schema.is_some() {
                    return Err(conflicting_def_elem(defel));
                }
                d_schema = Some(defel);
                schema_name = Some(explain::defGetString(mcx, defel)?);
            }
            "new_version" => {
                if d_new_version.is_some() {
                    return Err(conflicting_def_elem(defel));
                }
                d_new_version = Some(defel);
                version_name = Some(explain::defGetString(mcx, defel)?);
            }
            "cascade" => {
                if d_cascade.is_some() {
                    return Err(conflicting_def_elem(defel));
                }
                d_cascade = Some(defel);
                cascade = explain::defGetBoolean(defel)?;
            }
            other => panic!("unrecognized option: {other}"),
        }
    }

    CreateExtensionInternal(mcx, extname, schema_name, version_name, cascade, &[], true)
}

pub(crate) fn conflicting_def_elem(defel: &DefElem<'_>) -> Box<types_error::PgError> {
    let mut e = ereport(ERROR)
        .errcode(types_error::ERRCODE_SYNTAX_ERROR)
        .errmsg("conflicting or redundant options")
        .into_error();
    if defel.location >= 0 {
        e.cursor_position = Some(defel.location + 1);
    }
    Box::new(e)
}

// InsertExtensionTuple (extension.c:2192-2271). extConfig/extCondition are
// array datums or Datum::null().
#[allow(clippy::too_many_arguments)]
pub fn InsertExtensionTuple(
    mcx: Mcx<'_>,
    ext_name: &str,
    ext_owner: Oid,
    schema_oid: Oid,
    relocatable: bool,
    ext_version: &str,
    ext_config: Option<Datum>,
    ext_condition: Option<Datum>,
    required_extensions: &[Oid],
) -> PgResult<ObjectAddress> {
    let rel = table::table_open(mcx, EXTENSION_RELATION_ID, RowExclusiveLock)?;

    let extension_oid = catalog::GetNewOidWithIndex(
        mcx,
        &rel,
        ExtensionOidIndexId,
        Anum_pg_extension_oid as types_core::AttrNumber,
    )?;

    let mut name = NameData::default();
    name.namestrcpy(ext_name);
    let version_text = varlena::cstring_to_text(mcx, ext_version.as_bytes())?;

    let mut values = [Datum::null(); Natts_pg_extension];
    let mut nulls = [false; Natts_pg_extension];
    values[0] = Datum::from_oid(extension_oid);
    values[1] = Datum::from_usize(name.data.as_ptr() as usize);
    values[2] = Datum::from_oid(ext_owner);
    values[3] = Datum::from_oid(schema_oid);
    values[4] = Datum::from_bool(relocatable);
    values[5] = Datum::from_usize(version_text.as_bytes().as_ptr() as usize);
    match ext_config {
        Some(d) => values[6] = d,
        None => nulls[6] = true,
    }
    match ext_condition {
        Some(d) => values[7] = d,
        None => nulls[7] = true,
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
    rel.close(RowExclusiveLock)?;

    pg_depend::recordDependencyOnOwner(mcx, EXTENSION_RELATION_ID, extension_oid, ext_owner)?;

    let myself = ObjectAddress::set(EXTENSION_RELATION_ID, extension_oid);
    let mut refobjs: Vec<ObjectAddress> = Vec::with_capacity(1 + required_extensions.len());
    refobjs.push(ObjectAddress::set(NAMESPACE_RELATION_ID, schema_oid));
    for &reqext in required_extensions {
        refobjs.push(ObjectAddress::set(EXTENSION_RELATION_ID, reqext));
    }
    pg_depend::record_object_address_dependencies(
        mcx,
        &myself,
        &mut refobjs,
        pg_depend::DependencyType::Normal,
    )?;
    // InvokeObjectPostCreateHook: object-access hooks are elided repo-wide.

    Ok(myself)
}

// RemoveExtensionById (extension.c:2280-2322).
pub fn RemoveExtensionById(mcx: Mcx<'_>, ext_id: Oid) -> PgResult<()> {
    // An extension open for insertion must not be dropped: later
    // recordDependencyOnCurrentExtension calls would leave dangling pg_depend
    // rows.
    if ext_id == pg_depend::CurrentExtensionObject() {
        let name = crate::get_extension_name(mcx, ext_id)?
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        return Err(ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg(format!("cannot drop extension \"{name}\" because it is being modified"))
            .into_error()
            .into());
    }

    let rel = table::table_open(mcx, EXTENSION_RELATION_ID, RowExclusiveLock)?;
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_extension_oid as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(ext_id);
    let mut scan = genam::systable_beginscan(mcx, &rel, ExtensionOidIndexId, true, None, &[key])?;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}
