//! `collation-constraint-language-cast` family — `lsyscache.c` lookups keyed
//! on `pg_collation`, `pg_constraint`, `pg_language`, `pg_cast` and the
//! transform helpers.
//!
//! SCAFFOLD STAGE. The C entry points for this family
//! (`get_collation_isdeterministic`, `get_collation_name`,
//! `get_constraint_name`, `get_constraint_index`, `get_constraint_type`,
//! `get_language_name`, `get_cast_oid`, `get_transform_fromsql` /
//! `get_transform_tosql`, ...) have no `backend-utils-cache-lsyscache-seams`
//! declaration yet — their fan-in consumers reach them by direct dependency
//! rather than a seam. The family module exists so this C section has a home
//! once its logic (and any future seam decls) lands; there are currently no
//! seam adapters to install for it in [`crate::init_seams`].
