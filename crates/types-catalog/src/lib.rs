//! Catalog vocabulary shared across catalog-layer ports: genbki-assigned
//! catalog relation OIDs, pg_class relkind codes, and the object-address /
//! dependency types (`catalog/objectaddress.h`, `catalog/dependency.h`,
//! `catalog/pg_depend.h`).

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod catalog;
pub mod catalog_dependency;
pub mod pg_cast;
pub mod pg_constraint;
pub mod pg_conversion;
pub mod pg_enum;
pub mod pg_extension;
pub mod pg_type;
pub mod pg_inherits;
pub mod pg_language;
pub mod pg_range;
pub mod pg_statistic_ext;
pub mod pg_sequence;
pub mod object_access;
pub mod pg_aggregate;
pub mod pg_operator;
pub mod catalog_shdepend;
pub mod opclasscmds_catalog;
pub mod pg_database;
pub mod pg_db_role_setting;
pub mod pg_publication;
