//! Family: **multibitmapset** — `nodes/multibitmapset.c`, a `List` of
//! `Bitmapset`.
//!
//! `mbms_add_member`, `mbms_add_members`, `mbms_int_members`, `mbms_is_member`,
//! `mbms_overlap_sets`. Built directly on the keystone (`bms_*` operations) and
//! the list family (the outer `List`).
//!
//! Depends on the keystone. Skeleton: the five `mbms_*` ops land when filled.

#![allow(unused)]

/// Family marker — the multibitmapset ops land here. See module docs.
pub fn multibitmapset_family_unimplemented() -> ! {
    todo!("multibitmapset: nodes/multibitmapset.c not yet ported (decomp family)")
}
