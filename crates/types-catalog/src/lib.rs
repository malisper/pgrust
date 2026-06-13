//! Catalog vocabulary shared across catalog-layer ports: genbki-assigned
//! catalog relation OIDs, pg_class relkind codes, and the object-address /
//! dependency types (`catalog/objectaddress.h`, `catalog/dependency.h`,
//! `catalog/pg_depend.h`).

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod catalog;
pub mod catalog_dependency;
pub mod pg_database;
pub mod pg_publication;
