//! `backend-bootstrap-catalog-data` — the genbki-generated bootstrap catalog
//! schema data (`catalog/schemapg.h`).
//!
//! `genbki.pl` emits, for every catalog whose relcache entry must exist before
//! any catalog can be read, a `Schema_pg_*[]` array of hardcoded
//! `FormData_pg_attribute` rows. `relcache.c`'s `formrdesc` consumes those rows
//! to "nail" the bootstrap catalogs (`pg_class`, `pg_attribute`, `pg_proc`,
//! `pg_type`, and the shared set `pg_database`/`pg_authid`/`pg_auth_members`/
//! `pg_shseclabel`/`pg_subscription`) without ever touching the catalogs.
//!
//! This crate owns those static arrays (faithful field-for-field to
//! `schemapg.h`) and installs the `catalog_schema_attrs` seam relcache calls
//! from `RelationCacheInitializePhase2`/`Phase3`.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use relcache_entry::BootstrapCatalogSchema;

pub mod data;

/* ----------------------------------------------------------------------------
 * Row-type OIDs (`*Relation_Rowtype_Id`) of the nailed bootstrap catalogs.
 *
 * `formrdesc` (and thus the `catalog_schema_attrs` seam) is keyed by the
 * catalog's COMPOSITE-type OID (its `pg_class.reltype`), the value
 * `RelationCacheInitializePhase2`/`Phase3` pass — NOT the relation OID.
 * (Mirrors `src/include/catalog/pg_*.h`'s `*Relation_Rowtype_Id` macros.)
 * ------------------------------------------------------------------------- */

/// `RelationRelation_Rowtype_Id` — `pg_class`'s composite type OID.
pub const RELATION_RELATION_ROWTYPE_ID: Oid = 83;
/// `AttributeRelation_Rowtype_Id` — `pg_attribute`'s composite type OID.
pub const ATTRIBUTE_RELATION_ROWTYPE_ID: Oid = 75;
/// `ProcedureRelation_Rowtype_Id` — `pg_proc`'s composite type OID.
pub const PROCEDURE_RELATION_ROWTYPE_ID: Oid = 81;
/// `TypeRelation_Rowtype_Id` — `pg_type`'s composite type OID.
pub const TYPE_RELATION_ROWTYPE_ID: Oid = 71;
/// `DatabaseRelation_Rowtype_Id` — `pg_database`'s composite type OID.
pub const DATABASE_RELATION_ROWTYPE_ID: Oid = 1248;
/// `AuthIdRelation_Rowtype_Id` — `pg_authid`'s composite type OID.
pub const AUTHID_RELATION_ROWTYPE_ID: Oid = 2842;
/// `AuthMemRelation_Rowtype_Id` — `pg_auth_members`'s composite type OID.
pub const AUTHMEM_RELATION_ROWTYPE_ID: Oid = 2843;
/// `SharedSecLabelRelation_Rowtype_Id` — `pg_shseclabel`'s composite type OID.
pub const SHAREDSECLABEL_RELATION_ROWTYPE_ID: Oid = 4066;
/// `SubscriptionRelation_Rowtype_Id` — `pg_subscription`'s composite type OID.
pub const SUBSCRIPTION_RELATION_ROWTYPE_ID: Oid = 6101;

/// `catalog_schema_attrs(reltype)`: the hardcoded `Schema_pg_*[]` rows for the
/// nailed catalog whose row-type OID is `reltype`, plus its catalog relation
/// OID (for `formrdesc`'s `rd_id`). Mirrors the genbki dispatch `formrdesc` does
/// by passing `Desc_pg_*` per catalog.
pub fn catalog_schema_attrs(reltype: Oid) -> BootstrapCatalogSchema {
    let (relid, attrs) = match reltype {
        RELATION_RELATION_ROWTYPE_ID => (data::PG_CLASS_RELID, data::schema_pg_class()),
        ATTRIBUTE_RELATION_ROWTYPE_ID => (data::PG_ATTRIBUTE_RELID, data::schema_pg_attribute()),
        PROCEDURE_RELATION_ROWTYPE_ID => (data::PG_PROC_RELID, data::schema_pg_proc()),
        TYPE_RELATION_ROWTYPE_ID => (data::PG_TYPE_RELID, data::schema_pg_type()),
        DATABASE_RELATION_ROWTYPE_ID => (data::PG_DATABASE_RELID, data::schema_pg_database()),
        AUTHID_RELATION_ROWTYPE_ID => (data::PG_AUTHID_RELID, data::schema_pg_authid()),
        AUTHMEM_RELATION_ROWTYPE_ID => {
            (data::PG_AUTH_MEMBERS_RELID, data::schema_pg_auth_members())
        }
        SHAREDSECLABEL_RELATION_ROWTYPE_ID => {
            (data::PG_SHSECLABEL_RELID, data::schema_pg_shseclabel())
        }
        SUBSCRIPTION_RELATION_ROWTYPE_ID => {
            (data::PG_SUBSCRIPTION_RELID, data::schema_pg_subscription())
        }
        other => panic!(
            "catalog_schema_attrs: no bootstrap Schema_pg_* data for row-type OID {other} \
             (not a nailed catalog formrdesc builds)"
        ),
    };
    BootstrapCatalogSchema { relid, attrs }
}

/// Install the `catalog_schema_attrs` seam relcache's bootstrap path calls.
pub fn init_seams() {
    relcache_seams::catalog_schema_attrs::set(catalog_schema_attrs);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each nailed catalog's schema must have the genbki `Natts_pg_*` row count
    /// and carry the right catalog relation OID (`attrelid`).
    #[test]
    fn schema_shapes_match_genbki() {
        let cases: &[(Oid, Oid, usize)] = &[
            (RELATION_RELATION_ROWTYPE_ID, data::PG_CLASS_RELID, 34),
            (ATTRIBUTE_RELATION_ROWTYPE_ID, data::PG_ATTRIBUTE_RELID, 25),
            (PROCEDURE_RELATION_ROWTYPE_ID, data::PG_PROC_RELID, 30),
            (TYPE_RELATION_ROWTYPE_ID, data::PG_TYPE_RELID, 32),
            (DATABASE_RELATION_ROWTYPE_ID, data::PG_DATABASE_RELID, 18),
            (AUTHID_RELATION_ROWTYPE_ID, data::PG_AUTHID_RELID, 12),
            (AUTHMEM_RELATION_ROWTYPE_ID, data::PG_AUTH_MEMBERS_RELID, 7),
            (SHAREDSECLABEL_RELATION_ROWTYPE_ID, data::PG_SHSECLABEL_RELID, 4),
            (SUBSCRIPTION_RELATION_ROWTYPE_ID, data::PG_SUBSCRIPTION_RELID, 18),
        ];
        for &(reltype, relid, natts) in cases {
            let s = catalog_schema_attrs(reltype);
            assert_eq!(s.relid, relid, "relid for reltype {reltype}");
            assert_eq!(s.attrs.len(), natts, "natts for reltype {reltype}");
            // attnum is 1-based and contiguous (the genbki column order).
            for (i, a) in s.attrs.iter().enumerate() {
                assert_eq!(a.attnum as usize, i + 1, "attnum order in reltype {reltype}");
            }
        }
    }

    /// Spot-check the `FLOAT8PASSBYVAL` rows (8-byte by-value on 64-bit).
    #[test]
    fn float8passbyval_rows() {
        let authid = catalog_schema_attrs(AUTHID_RELATION_ROWTYPE_ID);
        let rolvaliduntil = authid.attrs.iter().find(|a| a.attname == "rolvaliduntil").unwrap();
        assert_eq!((rolvaliduntil.atttypid, rolvaliduntil.attlen, rolvaliduntil.attbyval), (1184, 8, true));
        let sub = catalog_schema_attrs(SUBSCRIPTION_RELATION_ROWTYPE_ID);
        let subskiplsn = sub.attrs.iter().find(|a| a.attname == "subskiplsn").unwrap();
        assert_eq!((subskiplsn.atttypid, subskiplsn.attlen, subskiplsn.attbyval), (3220, 8, true));
    }
}
