// ALTER EXTENSION UPDATE (extension.c:3408-3702). ALTER EXTENSION ADD/DROP
// and SET SCHEMA are loud.
use elog::ereport;
use mcx::Mcx;
use types_core::{InvalidOid, Oid, EXTENSION_RELATION_ID};
use types_error::{
    PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_UNDEFINED_OBJECT, ERROR, NOTICE,
};
use types_nodes::parsenodes::DefElem;
use types_nodes::rawnodes::AlterExtensionStmt;
use types_rel::{AccessShareLock, RowExclusiveLock};

use datum::Datum;
use pg_depend::ObjectAddress;

use crate::control::{read_extension_aux_control_file, read_extension_control_file, ExtensionControlFile};
use crate::create::{conflicting_def_elem, get_required_extension};
use crate::graph::identify_update_path;
use crate::script::execute_extension_script;
use crate::{
    check_valid_version_name, get_extension_schema, unported, Anum_pg_extension_extname,
    Anum_pg_extension_extnamespace, Anum_pg_extension_extrelocatable,
    Anum_pg_extension_extversion, Anum_pg_extension_oid, ExtensionNameIndexId,
    ExtensionOidIndexId, Natts_pg_extension,
};

fn oid_key(attno: i32, oid: Oid) -> types_scan::scankey::ScanKeyData {
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = attno as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

// ExecAlterExtensionStmt (extension.c:3408-3544).
pub fn ExecAlterExtensionStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &AlterExtensionStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let extname = stmt.extname.expect("grammar always supplies extname");

    if pg_depend::creating_extension() {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("nested ALTER EXTENSION is not supported")
            .into_error()
            .into());
    }

    let ext_rel = table::table_open(mcx, EXTENSION_RELATION_ID, AccessShareLock)?;

    let mut name = types_tuple::NameData::default();
    name.namestrcpy(extname);
    let mut key = types_scan::scankey::ScanKeyData::empty();
    key.sk_attno = Anum_pg_extension_extname as types_core::AttrNumber;
    key.sk_strategy = types_scan::scankey::BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_NAMEEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_NAMEEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_usize(name.data.as_ptr() as usize);

    let mut scan =
        genam::systable_beginscan(mcx, &ext_rel, ExtensionNameIndexId, true, None, &[key])?;
    let Some(tup) = genam::systable_getnext(mcx, &mut scan)? else {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("extension \"{extname}\" does not exist"))
            .into_error()
            .into());
    };

    let desc = ext_rel.descr();
    let mut isnull = false;
    // SAFETY (each): fixed pg_extension columns under the relation's
    // descriptor; oid is NOT NULL, extversion is BKI_FORCE_NOT_NULL.
    let extension_oid =
        unsafe { types_tuple::heap_getattr(tup, Anum_pg_extension_oid, desc, &mut isnull) }
            .as_oid();
    let version_d =
        unsafe { types_tuple::heap_getattr(tup, Anum_pg_extension_extversion, desc, &mut isnull) };
    if isnull {
        panic!("extversion is null");
    }
    let old_version_name = text_datum_str(mcx, version_d)?;

    genam::systable_endscan(mcx, scan)?;
    ext_rel.close(AccessShareLock)?;

    // object_ownercheck (aclchk.c): superuser fast path; role ACL walks are
    // the aclchk lane.
    if !superuser::superuser()? {
        unported("ExecAlterExtensionStmt: object_ownercheck for non-superusers");
    }

    let control = read_extension_control_file(extname)?;

    let mut d_new_version: Option<&DefElem<'_>> = None;
    for opt in stmt.options.iter() {
        let defel = opt.as_def_elem().expect("options are DefElems");
        match defel.defname.unwrap_or("") {
            "new_version" => {
                if d_new_version.is_some() {
                    return Err(conflicting_def_elem(defel));
                }
                d_new_version = Some(defel);
            }
            other => panic!("unrecognized option: {other}"),
        }
    }

    let version_name: String = match d_new_version.filter(|d| d.arg.is_some()) {
        Some(d) => explain::defGetString(mcx, d)?.to_string(),
        None => match &control.default_version {
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

    if old_version_name == version_name {
        ereport(NOTICE)
            .errmsg(format!(
                "version \"{version_name}\" of extension \"{extname}\" is already installed"
            ))
            .finish(types_error::ErrorLocation::new(
                "extension.c",
                0,
                "ExecAlterExtensionStmt",
            ))?;
        return Ok(ObjectAddress::set(InvalidOid, InvalidOid));
    }

    let update_versions = identify_update_path(&control, &old_version_name, &version_name)?;

    ApplyExtensionUpdates(
        mcx,
        extension_oid,
        &control,
        &old_version_name,
        &update_versions,
        None,
        false,
        false,
    )?;

    Ok(ObjectAddress::set(EXTENSION_RELATION_ID, extension_oid))
}

fn text_datum_str(mcx: Mcx<'_>, d: Datum) -> PgResult<String> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena attr datum from the live scan slot; the image
    // spans its header-declared size.
    let src = unsafe {
        let b0 = *p;
        let len = if b0 == 0x01 {
            2 + types_tuple::varatt::vartag_size(*p.add(1))
        } else if b0 & 0x01 != 0 {
            (b0 as usize >> 1) & 0x7F
        } else {
            (u32::from_ne_bytes(*(p as *const [u8; 4])) >> 2) as usize
        };
        core::slice::from_raw_parts(p, len)
    };
    let img = detoast::detoast_attr(mcx, src)?;
    let (skip, total) = if img[0] & 0x01 != 0 {
        (1usize, (img[0] as usize >> 1) & 0x7F)
    } else {
        (4usize, (u32::from_ne_bytes([img[0], img[1], img[2], img[3]]) >> 2) as usize)
    };
    Ok(core::str::from_utf8(&img[skip..total])
        .expect("extversion is server-encoding text")
        .to_string())
}

// ApplyExtensionUpdates (extension.c:3554-3702).
#[allow(clippy::too_many_arguments)]
pub(crate) fn ApplyExtensionUpdates(
    mcx: Mcx<'_>,
    extension_oid: Oid,
    pcontrol: &ExtensionControlFile,
    initial_version: &str,
    update_versions: &[String],
    orig_schema_name: Option<&str>,
    cascade: bool,
    is_create: bool,
) -> PgResult<()> {
    let mut old_version_name = initial_version.to_string();

    for version_name in update_versions {
        let control = read_extension_aux_control_file(pcontrol, version_name)?;

        let ext_rel = table::table_open(mcx, EXTENSION_RELATION_ID, RowExclusiveLock)?;
        let key = oid_key(Anum_pg_extension_oid, extension_oid);
        let mut scan =
            genam::systable_beginscan(mcx, &ext_rel, ExtensionOidIndexId, true, None, &[key])?;
        let Some(ext_tup) = genam::systable_getnext(mcx, &mut scan)? else {
            panic!("could not find tuple for extension {extension_oid}");
        };

        let desc = ext_rel.descr();
        let mut isnull = false;
        // SAFETY: extnamespace is a fixed NOT NULL pg_extension column.
        let schema_oid = unsafe {
            types_tuple::heap_getattr(ext_tup, Anum_pg_extension_extnamespace, desc, &mut isnull)
        }
        .as_oid();
        let schema_name = lsyscache::get_namespace_name(mcx, schema_oid)?
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();

        let version_text = varlena::cstring_to_text(mcx, version_name.as_bytes())?;
        let mut repl_values = [Datum::null(); Natts_pg_extension];
        let repl_nulls = [false; Natts_pg_extension];
        let mut repl = [false; Natts_pg_extension];
        repl_values[Anum_pg_extension_extrelocatable as usize - 1] =
            Datum::from_bool(control.relocatable);
        repl[Anum_pg_extension_extrelocatable as usize - 1] = true;
        repl_values[Anum_pg_extension_extversion as usize - 1] =
            Datum::from_usize(version_text.as_bytes().as_ptr() as usize);
        repl[Anum_pg_extension_extversion as usize - 1] = true;

        let tid = ext_tup.t_self;
        let mut new_tup = heaptuple::heap_modify_tuple(
            mcx,
            ext_tup,
            desc,
            &repl_values,
            &repl_nulls,
            &repl,
        )?;
        catalog_indexing::CatalogTupleUpdate(mcx, &ext_rel, &tid, &mut new_tup)?;

        genam::systable_endscan(mcx, scan)?;
        ext_rel.close(RowExclusiveLock)?;

        let mut required_extensions: Vec<Oid> = Vec::new();
        let mut required_schemas: Vec<Oid> = Vec::new();
        for curreq in &control.requires {
            let reqext = get_required_extension(
                mcx,
                curreq,
                &control.name,
                orig_schema_name,
                cascade,
                &[],
                is_create,
            )?;
            let reqschema = get_extension_schema(reqext)?;
            required_extensions.push(reqext);
            required_schemas.push(reqschema);
        }

        pg_depend::deleteDependencyRecordsForClass(
            mcx,
            EXTENSION_RELATION_ID,
            extension_oid,
            EXTENSION_RELATION_ID,
            pg_depend::DependencyType::Normal,
        )?;

        let myself = ObjectAddress::set(EXTENSION_RELATION_ID, extension_oid);
        for &reqext in &required_extensions {
            let otherext = ObjectAddress::set(EXTENSION_RELATION_ID, reqext);
            pg_depend::recordDependencyOn(
                mcx,
                &myself,
                &otherext,
                pg_depend::DependencyType::Normal,
            )?;
        }
        // InvokeObjectPostAlterHook: object-access hooks are elided repo-wide.

        execute_extension_script(
            mcx,
            extension_oid,
            &control,
            Some(&old_version_name),
            version_name,
            &required_schemas,
            &schema_name,
        )?;

        old_version_name = version_name.clone();
    }

    Ok(())
}
