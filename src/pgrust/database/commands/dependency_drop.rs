// :HACK: Preserve the root database command path while dependency graph
// helpers live with the portable catalog store.
pub(super) use pgrust_catalog_store::{CatalogDependencyGraph, DropBehavior, ObjectAddress};
