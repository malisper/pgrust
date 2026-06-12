//! Catalog vocabulary shared across catalog-layer ports: genbki-assigned
//! catalog relation OIDs, the object-address / dependency types
//! (`catalog/objectaddress.h`, `catalog/dependency.h`, `catalog/pg_depend.h`),
//! and the pg_depend scan-cursor value types that cross the systable-scan
//! seam.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

pub mod backend_catalog_pg_depend;
pub mod catalog;
pub mod catalog_dependency;
