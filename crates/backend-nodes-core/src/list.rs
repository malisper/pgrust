//! Family: **list** — `nodes/list.c`, the generic `List` (T_List / T_IntList /
//! T_OidList / T_XidList).
//!
//! In the owned-tree model a `List *` is a typed owned `Vec`-backed structure;
//! the ~75 `list_*` / `lappend*` / `lcons*` / `list_concat*` / `list_member*` /
//! `list_delete*` / `list_union*` / `list_difference*` / `list_sort` operations
//! port over it. Allocating ops take `Mcx` and return `PgResult` (C: palloc in
//! `CurrentMemoryContext`); pure reads borrow.
//!
//! Depends on the keystone only for shared node identity. Skeleton: the
//! concrete `List` carrier type is authored when this family is filled (the
//! plan/parse trees thread `List` pervasively; the type lands in `types_nodes`
//! to keep it acyclic).

#![allow(unused)]

// The ~75 list.c functions (new_list/lappend{,_int,_oid,_xid}/lcons{,_int,_oid}/
// list_concat{,_copy,_unique*}/list_member{,_ptr,_int,_oid,_xid}/list_delete*/
// list_union*/list_intersection*/list_difference*/list_append_unique*/
// list_truncate/list_copy{,_head,_tail,_deep}/list_sort/list_free*/
// list_make{1..5}_impl + the internal new_head_cell/new_tail_cell/enlarge_list/
// insert_new_cell/check_list_invariants) land here when the family is filled.

/// Family marker — the list operations land here. See module docs.
pub fn list_family_unimplemented() -> ! {
    todo!("list: nodes/list.c not yet ported (decomp family)")
}
