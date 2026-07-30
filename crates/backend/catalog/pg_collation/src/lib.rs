//! pg_collation.c. checkMembershipInCurrentExtension, extension deps and the
//! post-create hook are unported no-ops (extensions unported repo-wide).

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

    rel.close(NoLock)?;
    Ok(oid)
}
