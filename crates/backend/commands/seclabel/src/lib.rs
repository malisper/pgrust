//! seclabel.c: SECURITY LABEL execution, pg_seclabel/pg_shseclabel upserts,
//! and the in-process label-provider registry.

#![allow(non_snake_case, non_upper_case_globals, non_camel_case_types)]

use std::sync::Mutex;

use datum::Datum;
use mcx::Mcx;
use pg_depend::ObjectAddress;
use types_core::fmgr::{F_INT4EQ, F_OIDEQ, F_TEXTEQ};
use types_core::{AttrNumber, RegProcedure};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_NAME,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_nodes::parsenodes::{ObjectType, SecLabelStmt};
use types_rel::{
    AccessShareLock, NoLock, RowExclusiveLock, ShareUpdateExclusiveLock,
    RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_VIEW,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

const Natts_pg_seclabel: usize = 5;
const Anum_pg_seclabel_label: usize = 5;
const Natts_pg_shseclabel: usize = 4;
const Anum_pg_shseclabel_label: usize = 4;
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_COMPOSITE_TYPE: u8 = b'c';
const RELKIND_FOREIGN_TABLE: u8 = b'f';

pub type check_object_relabel_type =
    fn(object: &ObjectAddress, seclabel: Option<&str>) -> PgResult<()>;

#[derive(Clone, Copy, Debug)]
struct LabelProvider {
    name: &'static str,
    hook: check_object_relabel_type,
}

const MAX_LABEL_PROVIDERS: usize = 8;

struct ProviderRegistry {
    providers: [Option<LabelProvider>; MAX_LABEL_PROVIDERS],
}

#[track_caller]
#[cold]
fn invalid_parameter(message: String) -> Box<PgError> {
    Box::new(PgError::new(ERROR, message).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

impl ProviderRegistry {
    const fn new() -> Self {
        Self { providers: [None; MAX_LABEL_PROVIDERS] }
    }

    fn register(&mut self, name: &'static str, hook: check_object_relabel_type) {
        for slot in &mut self.providers {
            if slot.is_none() {
                *slot = Some(LabelProvider { name, hook });
                return;
            }
        }
        panic!("security label provider registry full ({MAX_LABEL_PROVIDERS} slots)");
    }

    fn resolve(&self, requested: Option<&str>) -> PgResult<LabelProvider> {
        let mut loaded = self.providers.iter().flatten();
        match requested {
            None => {
                let Some(first) = loaded.next() else {
                    return Err(invalid_parameter(
                        "no security label providers have been loaded".into(),
                    ));
                };
                if loaded.next().is_some() {
                    return Err(invalid_parameter(
                        "must specify provider when multiple security label providers \
                         have been loaded"
                            .into(),
                    ));
                }
                Ok(*first)
            }
            Some(name) => loaded.find(|p| p.name == name).copied().ok_or_else(|| {
                invalid_parameter(format!(
                    "security label provider \"{name}\" is not loaded"
                ))
            }),
        }
    }
}

// C keeps the provider list in TopMemoryContext of the postmaster (loaded via
// shared_preload_libraries before backends exist); backends here are threads,
// so the process-global registry is the same visibility.
static LABEL_PROVIDERS: Mutex<ProviderRegistry> = Mutex::new(ProviderRegistry::new());

pub fn register_label_provider(provider_name: &'static str, hook: check_object_relabel_type) {
    LABEL_PROVIDERS.lock().unwrap().register(provider_name, hook);
}

// C loads dummy_seclabel via shared_preload_libraries; the env gate is our
// analogue (no dynamic module loading).
pub fn init() {
    if std::env::var_os("PGRUST_DUMMY_SECLABEL").is_some_and(|v| v == "1") {
        register_label_provider("dummy", dummy_object_relabel);
    }
}

// dummy_seclabel.c dummy_object_relabel.
fn dummy_object_relabel(_object: &ObjectAddress, seclabel: Option<&str>) -> PgResult<()> {
    let Some(label) = seclabel else { return Ok(()) };
    match label {
        "unclassified" | "classified" => Ok(()),
        "secret" | "top secret" => {
            if !superuser::superuser()? {
                return Err(Box::new(
                    PgError::new(ERROR, format!("only superuser can set '{label}' label"))
                        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE),
                ));
            }
            Ok(())
        }
        _ => Err(Box::new(
            PgError::new(ERROR, format!("'{label}' is not a valid security label"))
                .with_sqlstate(ERRCODE_INVALID_NAME),
        )),
    }
}

pub fn SecLabelSupportsObjectType(objtype: ObjectType) -> bool {
    use ObjectType::*;
    match objtype {
        OBJECT_AGGREGATE | OBJECT_COLUMN | OBJECT_DATABASE | OBJECT_DOMAIN
        | OBJECT_EVENT_TRIGGER | OBJECT_FOREIGN_TABLE | OBJECT_FUNCTION | OBJECT_LANGUAGE
        | OBJECT_LARGEOBJECT | OBJECT_MATVIEW | OBJECT_PROCEDURE | OBJECT_PUBLICATION
        | OBJECT_ROLE | OBJECT_ROUTINE | OBJECT_SCHEMA | OBJECT_SEQUENCE
        | OBJECT_SUBSCRIPTION | OBJECT_TABLE | OBJECT_TABLESPACE | OBJECT_TYPE
        | OBJECT_VIEW => true,

        OBJECT_ACCESS_METHOD | OBJECT_AMOP | OBJECT_AMPROC | OBJECT_ATTRIBUTE | OBJECT_CAST
        | OBJECT_COLLATION | OBJECT_CONVERSION | OBJECT_DEFAULT | OBJECT_DEFACL
        | OBJECT_DOMCONSTRAINT | OBJECT_EXTENSION | OBJECT_FDW | OBJECT_FOREIGN_SERVER
        | OBJECT_INDEX | OBJECT_OPCLASS | OBJECT_OPERATOR | OBJECT_OPFAMILY
        | OBJECT_PARAMETER_ACL | OBJECT_POLICY | OBJECT_PUBLICATION_NAMESPACE
        | OBJECT_PUBLICATION_REL | OBJECT_RULE | OBJECT_STATISTIC_EXT | OBJECT_TABCONSTRAINT
        | OBJECT_TRANSFORM | OBJECT_TRIGGER | OBJECT_TSCONFIGURATION | OBJECT_TSDICTIONARY
        | OBJECT_TSPARSER | OBJECT_TSTEMPLATE | OBJECT_USER_MAPPING => false,
    }
}

pub fn ExecSecLabelStmt<'mcx>(
    mcx: Mcx<'mcx>,
    stmt: &SecLabelStmt<'mcx>,
) -> PgResult<ObjectAddress> {
    let provider = LABEL_PROVIDERS.lock().unwrap().resolve(stmt.provider)?;

    if !SecLabelSupportsObjectType(stmt.objtype) {
        return Err(Box::new(
            PgError::new(ERROR, "security labels are not supported for this type of object")
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    let object = stmt.object.expect("grammar always supplies the object");
    let (addr, relation) = objectaddress_seams::get_object_address::call(
        mcx,
        stmt.objtype,
        object,
        ShareUpdateExclusiveLock,
        false,
    )?;

    objectaddress_seams::check_object_ownership::call(
        mcx,
        miscinit::GetUserId(),
        stmt.objtype,
        addr,
        object,
        relation.as_ref(),
    )?;

    if stmt.objtype == ObjectType::OBJECT_COLUMN {
        let rel = relation.as_ref().expect("column security label carries its relation");
        let relkind = rel.rd_rel.relkind;
        if !matches!(
            relkind,
            RELKIND_RELATION
                | RELKIND_VIEW
                | RELKIND_MATVIEW
                | RELKIND_COMPOSITE_TYPE
                | RELKIND_FOREIGN_TABLE
                | RELKIND_PARTITIONED_TABLE
        ) {
            let detail = pg_class_seams::errdetail_relkind_not_supported::call(relkind)?;
            return Err(Box::new(
                PgError::new(
                    ERROR,
                    format!("cannot set security label on relation \"{}\"", rel.name()),
                )
                .with_detail(detail)
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
            ));
        }
    }

    let address = ObjectAddress::sub_set(addr.classId, addr.objectId, addr.objectSubId);
    (provider.hook)(&address, stmt.label)?;
    SetSecurityLabel(mcx, &address, provider.name, stmt.label)?;

    // C keeps the ShareUpdateExclusiveLock until commit.
    if let Some(rel) = relation {
        rel.close(NoLock)?;
    }
    Ok(address)
}

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<(datum::Varlena<'mcx>, Datum)> {
    let t = varlena::cstring_to_text(mcx, s.as_bytes())?;
    let d = Datum::from_usize(t.as_bytes().as_ptr() as usize);
    Ok((t, d))
}

fn label_column<'mcx>(
    mcx: Mcx<'mcx>,
    tup: &types_tuple::HeapTupleData<'_>,
    descr: &types_tuple::TupleDescData<'_>,
    attnum: usize,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let mut isnull = false;
    // SAFETY: pg_seclabel/pg_shseclabel label attr under its own descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum as i32, descr, &mut isnull) };
    if isnull {
        return Ok(None);
    }
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null text column: live varlena image through its extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    let payload = varlena::open_image(mcx, image)?;
    let s = core::str::from_utf8(payload.as_bytes()).expect("security label UTF-8");
    Ok(Some(mcx::PgString::from_str_in(s, mcx)?))
}

fn GetSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    let rel = table::table_open(mcx, catalog::SharedSecLabelRelationId, AccessShareLock)?;
    let (_ptext, pdatum) = text_datum(mcx, provider)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(object.objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(object.classId)),
        eq_key(3, F_TEXTEQ, pdatum),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedSecLabelObjectIndexId,
        relcache_seams::critical_shared_relcaches_built::call(),
        None,
        &keys,
    )?;
    let label = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => label_column(mcx, tup, rel.descr(), Anum_pg_shseclabel_label)?,
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(label)
}

pub fn GetSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
) -> PgResult<Option<mcx::PgString<'mcx>>> {
    if catalog::IsSharedRelation(object.classId) {
        return GetSharedSecurityLabel(mcx, object, provider);
    }
    let rel = table::table_open(mcx, catalog::SecLabelRelationId, AccessShareLock)?;
    let (_ptext, pdatum) = text_datum(mcx, provider)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(object.objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(object.classId)),
        eq_key(3, F_INT4EQ, Datum::from_i32(object.objectSubId)),
        eq_key(4, F_TEXTEQ, pdatum),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, catalog::SecLabelObjectIndexId, true, None, &keys)?;
    let label = match genam::systable_getnext(mcx, &mut scan)? {
        Some(tup) => label_column(mcx, tup, rel.descr(), Anum_pg_seclabel_label)?,
        None => None,
    };
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(label)
}

fn SetSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
    label: Option<&str>,
) -> PgResult<()> {
    let rel = table::table_open(mcx, catalog::SharedSecLabelRelationId, RowExclusiveLock)?;
    let (_ptext, pdatum) = text_datum(mcx, provider)?;
    let ltext = match label {
        Some(l) => Some(text_datum(mcx, l)?),
        None => None,
    };
    let ldatum = ltext.as_ref().map(|(_, d)| *d);

    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(object.objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(object.classId)),
        eq_key(3, F_TEXTEQ, pdatum),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedSecLabelObjectIndexId,
        true,
        None,
        &keys,
    )?;
    let old = genam::systable_getnext(mcx, &mut scan)?;
    match (old, ldatum) {
        (Some(oldtup), None) => {
            let tid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
        }
        (Some(oldtup), Some(d)) => {
            let mut values = [Datum::null(); Natts_pg_shseclabel];
            let isnull = [false; Natts_pg_shseclabel];
            let mut replace = [false; Natts_pg_shseclabel];
            replace[Anum_pg_shseclabel_label - 1] = true;
            values[Anum_pg_shseclabel_label - 1] = d;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, oldtup, rel.descr(), &values, &isnull, &replace)?;
            let otid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
        }
        (None, Some(d)) => {
            genam::systable_endscan(mcx, scan)?;
            let values = [
                Datum::from_oid(object.objectId),
                Datum::from_oid(object.classId),
                pdatum,
                d,
            ];
            let nulls = [false; Natts_pg_shseclabel];
            let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
        }
        (None, None) => genam::systable_endscan(mcx, scan)?,
    }
    rel.close(RowExclusiveLock)
}

pub fn SetSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    object: &ObjectAddress,
    provider: &str,
    label: Option<&str>,
) -> PgResult<()> {
    if catalog::IsSharedRelation(object.classId) {
        return SetSharedSecurityLabel(mcx, object, provider, label);
    }
    let rel = table::table_open(mcx, catalog::SecLabelRelationId, RowExclusiveLock)?;
    let (_ptext, pdatum) = text_datum(mcx, provider)?;
    let ltext = match label {
        Some(l) => Some(text_datum(mcx, l)?),
        None => None,
    };
    let ldatum = ltext.as_ref().map(|(_, d)| *d);

    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(object.objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(object.classId)),
        eq_key(3, F_INT4EQ, Datum::from_i32(object.objectSubId)),
        eq_key(4, F_TEXTEQ, pdatum),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, catalog::SecLabelObjectIndexId, true, None, &keys)?;
    let old = genam::systable_getnext(mcx, &mut scan)?;
    match (old, ldatum) {
        (Some(oldtup), None) => {
            let tid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
        }
        (Some(oldtup), Some(d)) => {
            let mut values = [Datum::null(); Natts_pg_seclabel];
            let isnull = [false; Natts_pg_seclabel];
            let mut replace = [false; Natts_pg_seclabel];
            replace[Anum_pg_seclabel_label - 1] = true;
            values[Anum_pg_seclabel_label - 1] = d;
            let mut newtup =
                heaptuple::heap_modify_tuple(mcx, oldtup, rel.descr(), &values, &isnull, &replace)?;
            let otid = oldtup.t_self;
            genam::systable_endscan(mcx, scan)?;
            catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtup)?;
        }
        (None, Some(d)) => {
            genam::systable_endscan(mcx, scan)?;
            let values = [
                Datum::from_oid(object.objectId),
                Datum::from_oid(object.classId),
                Datum::from_i32(object.objectSubId),
                pdatum,
                d,
            ];
            let nulls = [false; Natts_pg_seclabel];
            let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
            catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
        }
        (None, None) => genam::systable_endscan(mcx, scan)?,
    }
    rel.close(RowExclusiveLock)
}

pub fn DeleteSharedSecurityLabel<'mcx>(
    mcx: Mcx<'mcx>,
    objectId: types_core::Oid,
    classId: types_core::Oid,
) -> PgResult<()> {
    let rel = table::table_open(mcx, catalog::SharedSecLabelRelationId, RowExclusiveLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(classId)),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedSecLabelObjectIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

pub fn DeleteSecurityLabel<'mcx>(mcx: Mcx<'mcx>, object: &ObjectAddress) -> PgResult<()> {
    if catalog::IsSharedRelation(object.classId) {
        debug_assert!(object.objectSubId == 0);
        return DeleteSharedSecurityLabel(mcx, object.objectId, object.classId);
    }
    let rel = table::table_open(mcx, catalog::SecLabelRelationId, RowExclusiveLock)?;
    let keys = [
        eq_key(1, F_OIDEQ, Datum::from_oid(object.objectId)),
        eq_key(2, F_OIDEQ, Datum::from_oid(object.classId)),
        eq_key(3, F_INT4EQ, Datum::from_i32(object.objectSubId)),
    ];
    let nkeys = if object.objectSubId != 0 { 3 } else { 2 };
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SecLabelObjectIndexId,
        true,
        None,
        &keys[..nkeys],
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_hook(_object: &ObjectAddress, _label: Option<&str>) -> PgResult<()> {
        Ok(())
    }

    fn other_hook(_object: &ObjectAddress, _label: Option<&str>) -> PgResult<()> {
        Ok(())
    }

    #[test]
    fn whitelist_matches_c() {
        use ObjectType::*;
        let supported = [
            OBJECT_AGGREGATE, OBJECT_COLUMN, OBJECT_DATABASE, OBJECT_DOMAIN,
            OBJECT_EVENT_TRIGGER, OBJECT_FOREIGN_TABLE, OBJECT_FUNCTION, OBJECT_LANGUAGE,
            OBJECT_LARGEOBJECT, OBJECT_MATVIEW, OBJECT_PROCEDURE, OBJECT_PUBLICATION,
            OBJECT_ROLE, OBJECT_ROUTINE, OBJECT_SCHEMA, OBJECT_SEQUENCE, OBJECT_SUBSCRIPTION,
            OBJECT_TABLE, OBJECT_TABLESPACE, OBJECT_TYPE, OBJECT_VIEW,
        ];
        assert_eq!(supported.len(), 21);
        for t in supported {
            assert!(SecLabelSupportsObjectType(t), "{t:?}");
        }
        for t in [
            OBJECT_ACCESS_METHOD, OBJECT_ATTRIBUTE, OBJECT_CAST, OBJECT_COLLATION,
            OBJECT_DOMCONSTRAINT, OBJECT_EXTENSION, OBJECT_INDEX, OBJECT_OPERATOR,
            OBJECT_PARAMETER_ACL, OBJECT_POLICY, OBJECT_RULE, OBJECT_TABCONSTRAINT,
            OBJECT_TRIGGER, OBJECT_TSPARSER, OBJECT_USER_MAPPING,
        ] {
            assert!(!SecLabelSupportsObjectType(t), "{t:?}");
        }
    }

    #[test]
    fn resolve_none_loaded() {
        let reg = ProviderRegistry::new();
        let err = reg.resolve(None).unwrap_err();
        assert_eq!(err.message, "no security label providers have been loaded");
        assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
        let err = reg.resolve(Some("dummy")).unwrap_err();
        assert_eq!(err.message, "security label provider \"dummy\" is not loaded");
        assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
    }

    #[test]
    fn resolve_single_and_named() {
        let mut reg = ProviderRegistry::new();
        reg.register("dummy", ok_hook);
        assert_eq!(reg.resolve(None).unwrap().name, "dummy");
        assert_eq!(reg.resolve(Some("dummy")).unwrap().name, "dummy");
        let err = reg.resolve(Some("selinux")).unwrap_err();
        assert_eq!(err.message, "security label provider \"selinux\" is not loaded");
    }

    #[test]
    fn resolve_multiple_requires_name() {
        let mut reg = ProviderRegistry::new();
        reg.register("dummy", ok_hook);
        reg.register("selinux", other_hook);
        let err = reg.resolve(None).unwrap_err();
        assert_eq!(
            err.message,
            "must specify provider when multiple security label providers have been loaded"
        );
        assert_eq!(err.sqlstate, ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(reg.resolve(Some("selinux")).unwrap().name, "selinux");
        assert_eq!(reg.resolve(Some("dummy")).unwrap().name, "dummy");
    }

    #[test]
    fn dummy_provider_accepts_and_rejects() {
        let addr = ObjectAddress::set(0, 0);
        assert!(dummy_object_relabel(&addr, None).is_ok());
        assert!(dummy_object_relabel(&addr, Some("unclassified")).is_ok());
        assert!(dummy_object_relabel(&addr, Some("classified")).is_ok());
        let err = dummy_object_relabel(&addr, Some("bogus")).unwrap_err();
        assert_eq!(err.message, "'bogus' is not a valid security label");
        assert_eq!(err.sqlstate, ERRCODE_INVALID_NAME);
    }
}
