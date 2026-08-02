//! pg_collation.c. InvokeObjectPostCreateHook is the repo-wide objectaccess
//! carve; everything else in CollationCreate is C-exact.

#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use elog::ereport;
use mcx::Mcx;
use pg_depend::{DependencyType, ObjectAddress};
use types_core::{InvalidOid, Oid, OidIsValid, COLLATION_RELATION_ID, NAMESPACE_RELATION_ID};
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_DUPLICATE_OBJECT, ERROR, NOTICE};
use types_rel::{NoLock, ShareRowExclusiveLock};
use types_tuple::NameData;

pub const CollationOidIndexId: Oid = 3085;
pub const Anum_pg_collation_oid: types_core::AttrNumber = 1;
pub const Natts_pg_collation: usize = 12;

const SRC: &str = "src/backend/catalog/pg_collation.c";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn existing_oid(collname: &str, encoding: i32, nsp: Oid) -> PgResult<Oid> {
    Ok(syscache_seams::lookup_pg_collation_by_name_enc_nsp::call(collname, encoding, nsp)?
        .map_or(InvalidOid, |row| row.oid))
}

pub struct CollationForm<'a> {
    pub collprovider: u8,
    pub collisdeterministic: bool,
    pub collencoding: i32,
    pub collcollate: Option<&'a str>,
    pub collctype: Option<&'a str>,
    pub colllocale: Option<&'a str>,
    pub collicurules: Option<&'a str>,
    pub collversion: Option<&'a str>,
}

pub fn CollationCreate<'mcx>(
    mcx: Mcx<'mcx>,
    collname: &str,
    collnamespace: Oid,
    collowner: Oid,
    form: &CollationForm<'_>,
    if_not_exists: bool,
    quiet: bool,
) -> PgResult<Oid> {
    assert!(
        (form.collprovider == pg_database_seams::COLLPROVIDER_LIBC
            && form.collcollate.is_some()
            && form.collctype.is_some()
            && form.colllocale.is_none())
            || (form.collprovider != pg_database_seams::COLLPROVIDER_LIBC
                && form.collcollate.is_none()
                && form.collctype.is_none()
                && form.colllocale.is_some())
    );

    let oid = existing_oid(collname, form.collencoding, collnamespace)?;
    if OidIsValid(oid) {
        if quiet {
            return Ok(InvalidOid);
        }
        if if_not_exists {
            // In an extension script, insist the pre-existing object be a
            // member of the extension, to avoid security risks.
            let myself = ObjectAddress::set(COLLATION_RELATION_ID, oid);
            pg_depend::checkMembershipInCurrentExtension(mcx, &myself)?;

            let msg = if form.collencoding == -1 {
                format!("collation \"{collname}\" already exists, skipping")
            } else {
                let encname = mbutils::pg_encoding_to_char(form.collencoding);
                format!("collation \"{collname}\" for encoding \"{encname}\" already exists, skipping")
            };
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(msg)
                .finish(loc("CollationCreate"))?;
            return Ok(InvalidOid);
        }
        let msg = if form.collencoding == -1 {
            format!("collation \"{collname}\" already exists")
        } else {
            let encname = mbutils::pg_encoding_to_char(form.collencoding);
            format!("collation \"{collname}\" for encoding \"{encname}\" already exists")
        };
        return Err(Box::new(
            PgError::new(ERROR, msg).with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }

    let rel = table::table_open(mcx, COLLATION_RELATION_ID, ShareRowExclusiveLock)?;

    let shadow_enc =
        if form.collencoding == -1 { mbutils::GetDatabaseEncoding() } else { -1 };
    let oid = existing_oid(collname, shadow_enc, collnamespace)?;
    if OidIsValid(oid) {
        if quiet {
            rel.close(NoLock)?;
            return Ok(InvalidOid);
        }
        if if_not_exists {
            // In an extension script, insist the pre-existing object be a
            // member of the extension, to avoid security risks.
            let myself = ObjectAddress::set(COLLATION_RELATION_ID, oid);
            pg_depend::checkMembershipInCurrentExtension(mcx, &myself)?;

            rel.close(NoLock)?;
            ereport(NOTICE)
                .errcode(ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("collation \"{collname}\" already exists, skipping"))
                .finish(loc("CollationCreate"))?;
            return Ok(InvalidOid);
        }
        return Err(Box::new(
            PgError::new(ERROR, format!("collation \"{collname}\" already exists"))
                .with_sqlstate(ERRCODE_DUPLICATE_OBJECT),
        ));
    }

    let mut name = NameData::default();
    name.namestrcpy(collname);
    let oid =
        catalog::GetNewOidWithIndex(mcx, &rel, CollationOidIndexId, Anum_pg_collation_oid)?;

    let mut values = [Datum::null(); Natts_pg_collation];
    let mut nulls = [false; Natts_pg_collation];
    values[0] = Datum::from_oid(oid);
    values[1] = Datum::from_usize(name.data.as_ptr() as usize);
    values[2] = Datum::from_oid(collnamespace);
    values[3] = Datum::from_oid(collowner);
    values[4] = Datum::from_u8(form.collprovider);
    values[5] = Datum::from_bool(form.collisdeterministic);
    values[6] = Datum::from_i32(form.collencoding);
    let texts = [
        (7, form.collcollate),
        (8, form.collctype),
        (9, form.colllocale),
        (10, form.collicurules),
        (11, form.collversion),
    ];
    let mut images: [Option<datum::Varlena<'mcx>>; 5] = [None, None, None, None, None];
    for (slot, (i, v)) in texts.into_iter().enumerate() {
        match v {
            Some(s) => {
                let t = varlena::cstring_to_text(mcx, s.as_bytes())?;
                images[slot] = Some(t);
                values[i] = Datum::from_usize(
                    images[slot].as_ref().unwrap().as_bytes().as_ptr() as usize,
                );
            }
            None => nulls[i] = true,
        }
    }

    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;

    let myself = ObjectAddress::set(COLLATION_RELATION_ID, oid);
    let referenced = ObjectAddress::set(NAMESPACE_RELATION_ID, collnamespace);
    pg_depend::recordDependencyOn(mcx, &myself, &referenced, DependencyType::Normal)?;
    pg_depend::recordDependencyOnOwner(mcx, COLLATION_RELATION_ID, oid, collowner)?;
    pg_depend::recordDependencyOnCurrentExtension(mcx, &myself, false)?;

    rel.close(NoLock)?;
    Ok(oid)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::*;
    use mcx::PgVec;
    use types_core::{INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
    use types_rel::{
        FormData_pg_class, LockInfoData, LockRelId, Relation, RelationData, RELKIND_RELATION,
        REPLICA_IDENTITY_DEFAULT,
    };
    use types_tuple::TupleDescData;

    const EXISTING_COLLATION: Oid = 88001;
    const SHADOW_COLLATION: Oid = 88002;
    const CURRENT_EXTENSION: Oid = 88100;
    const NSP: Oid = 2200;
    const OWNER: Oid = 10;
    // DependRelationId (pg_depend.h): the relation getExtensionOfObject scans.
    const DEPEND_RELATION_ID: Oid = 2608;

    // Just enough of pg_collation for the second duplicate lane to hold the
    // relation open across its membership check; nothing reads the tupdesc.
    fn fake_pg_collation_rel(mcx: Mcx<'_>) -> Relation<'_> {
        let mut relname = NameData::default();
        relname.namestrcpy("pg_collation");
        let data = RelationData { rd_locator: Default::default(), rd_smgr: Default::default(),
            rd_id: COLLATION_RELATION_ID,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: COLLATION_RELATION_ID, dbId: 5 },
            },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 11,
                reltype: 0,
                relowner: 10,
                relam: 2,
                relfilenode: COLLATION_RELATION_ID,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: true,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: RELKIND_RELATION,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: REPLICA_IDENTITY_DEFAULT,
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            },
            rd_att: Rc::new(TupleDescData {
                natts: 0,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: 1,
                constr: None,
                compact_attrs: PgVec::new_in(mcx),
                attrs: PgVec::new_in(mcx),
            }),
            rd_index: None,
            rd_opcintype: PgVec::new_in(mcx),
            rd_opfamily: PgVec::new_in(mcx),
            rd_indoption: PgVec::new_in(mcx),
            rd_indcollation: PgVec::new_in(mcx),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: core::cell::Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(), rd_amcache_gin: Default::default(), rd_amcache_spgist: Default::default(),
            rd_support: PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false, rd_hasrules: false,
        };
        Relation::open(data, None)
    }

    fn install_seams() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            syscache_seams::lookup_pg_collation_by_name_enc_nsp::set(|name, enc, nsp| {
                let row = |oid| syscache_seams::PgCollationNameEncNspRow {
                    oid,
                    collprovider: pg_database_seams::COLLPROVIDER_LIBC,
                };
                if name == "dupe" && nsp == NSP {
                    return Ok(Some(row(EXISTING_COLLATION)));
                }
                // Visible only at a real encoding, so a collencoding=-1
                // request reaches the any-encoding shadow probe's lane.
                if name == "shadowdupe" && nsp == NSP && enc != -1 {
                    return Ok(Some(row(SHADOW_COLLATION)));
                }
                Ok(None)
            });
            // pg_collation opens succeed (the second duplicate lane sits past
            // table_open); any other open — the first step of
            // getExtensionOfObject's pg_depend membership scan — stops with a
            // distinctive error the extension-script tests assert on.
            relation_seams::relation_open::set(|mcx, oid, _lockmode| {
                if oid == COLLATION_RELATION_ID {
                    return Ok(fake_pg_collation_rel(mcx));
                }
                Err(PgError::error(format!("test probe: relation {oid} opened")).into())
            });
        });
    }

    fn with_extension_script<T>(f: impl FnOnce() -> T) -> T {
        pg_depend::set_creating_extension(true);
        pg_depend::set_current_extension_object(CURRENT_EXTENSION);
        let r = f();
        pg_depend::set_creating_extension(false);
        pg_depend::set_current_extension_object(InvalidOid);
        r
    }

    fn libc_form() -> CollationForm<'static> {
        CollationForm {
            collprovider: pg_database_seams::COLLPROVIDER_LIBC,
            collisdeterministic: true,
            collencoding: -1,
            collcollate: Some("C"),
            collctype: Some("C"),
            colllocale: None,
            collicurules: None,
            collversion: None,
        }
    }

    // IF NOT EXISTS over an existing collation outside an extension script:
    // checkMembershipInCurrentExtension is a no-op (creating_extension is
    // false) and the NOTICE lane returns InvalidOid, as in C.
    #[test]
    fn if_not_exists_membership_gate_open_outside_extension() {
        install_seams();
        assert!(!pg_depend::creating_extension());
        let ctx = mcx::MemoryContext::new("t");
        let oid =
            CollationCreate(ctx.mcx(), "dupe", NSP, OWNER, &libc_form(), true, false).unwrap();
        assert_eq!(oid, InvalidOid);
    }

    // The quiet lane returns before the membership check, as in C.
    #[test]
    fn quiet_lane_skips_membership_check() {
        install_seams();
        let ctx = mcx::MemoryContext::new("t");
        let oid =
            CollationCreate(ctx.mcx(), "dupe", NSP, OWNER, &libc_form(), false, true).unwrap();
        assert_eq!(oid, InvalidOid);
    }

    // Inside an extension script the IF NOT EXISTS duplicate lane must
    // consult the pre-existing collation's extension membership (C's
    // checkMembershipInCurrentExtension) instead of skipping with a NOTICE:
    // the membership probe scans pg_depend, where the harness stops it.
    #[test]
    fn if_not_exists_inside_extension_script_checks_membership() {
        install_seams();
        let ctx = mcx::MemoryContext::new("t");
        let e = with_extension_script(|| {
            CollationCreate(ctx.mcx(), "dupe", NSP, OWNER, &libc_form(), true, false)
        })
        .unwrap_err();
        assert_eq!(e.message, format!("test probe: relation {DEPEND_RELATION_ID} opened"));
    }

    // Same gate in the any-encoding duplicate lane (the shadow probe past
    // table_open).
    #[test]
    fn shadow_encoding_lane_inside_extension_script_checks_membership() {
        install_seams();
        let ctx = mcx::MemoryContext::new("t");
        let e = with_extension_script(|| {
            CollationCreate(ctx.mcx(), "shadowdupe", NSP, OWNER, &libc_form(), true, false)
        })
        .unwrap_err();
        assert_eq!(e.message, format!("test probe: relation {DEPEND_RELATION_ID} opened"));
    }
}
