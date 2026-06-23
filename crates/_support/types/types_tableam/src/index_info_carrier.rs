//! `IndexInfo *` at the index-AM dispatch edge — the `'mcx`-safe carrier for
//! `struct IndexInfo` (`nodes/execnodes.h`) handed to `ambuild` / `aminsert` /
//! `aminsertcleanup`.
//!
//! In C, `indexam.c` forwards an `IndexInfo *` straight through to these AM
//! callbacks without reading it; the AM (and the build code) casts/uses it as
//! the concrete struct. The owned model needs the same pass-through, but the
//! real [`nodes::execnodes::IndexInfo<'mcx>`] is pervasively `'mcx`-bound
//! (`ii_Expressions` / `ii_Predicate` are `PgVec<'mcx, Expr>`, `ii_Context` is
//! an `Mcx<'mcx>`, etc). `types-tableam` (F0) sits *below* `types-nodes` (F1),
//! so the vtable cannot name `IndexInfo<'mcx>` directly — and the type's `'mcx`
//! lifetime means it cannot be erased into a `'static` `Box<dyn Any>` either
//! (`Any: 'static`).
//!
//! [`IndexInfoCarrier`] is the faithful rendering of that `IndexInfo *`
//! argument: it holds the caller's live `&mut IndexInfo<'mcx>` behind a
//! lifetime-preserving, tag-checked erased trait object ([`IndexInfoLive`]).
//! The carrier is parameterised by `'mcx`, so no lifetime is erased to
//! `'static`; only the *concrete type* is hidden across the F0→F1 edge, exactly
//! as the AM-private [`crate::amopaque::AmOpaque`] carrier hides a `void
//! *opaque`. The downcast back to the concrete `IndexInfo<'mcx>` is checked
//! against a per-type tag (the [`AmOpaqueTag`] discipline), so the unsafe
//! reinterpretation is sound.
//!
//! The caller (index.c / executor, once ported under #334) constructs the
//! carrier around its owned `&mut IndexInfo<'mcx>` and hands it to the dispatch
//! layer; the AM adapter downcasts it back to the concrete struct.

use core::any::type_name;

use crate::amopaque::AmOpaqueTag;

/// The erased live `IndexInfo<'mcx>` payload — C's `IndexInfo *` argument.
///
/// Object-safe: the only methods take `&self` and return plain values, so
/// `dyn IndexInfoLive<'mcx>` is a valid trait object. Mirrors
/// [`crate::amopaque::AmOpaque`]: implementors use [`IndexInfoTagged`] (which
/// supplies the tag via a const and gets this trait for free through the
/// blanket impl below); the soundness of the downcast relies on tag uniqueness,
/// so the [`IndexInfoTagged::TAG`] discipline must be honored.
pub trait IndexInfoLive<'mcx>: 'mcx {
    /// The tag of the concrete `Self` behind this trait object. Plays the role
    /// `core::any::TypeId` plays for `dyn Any`, but works for the non-`'static`
    /// (`'mcx`-bearing) `IndexInfo<'mcx>`.
    fn index_info_tag(&self) -> AmOpaqueTag;

    /// A human-readable name for the concrete type, for panic/debug messages
    /// only (never used for the downcast check).
    fn live_type_name(&self) -> &'static str;
}

/// Marker implemented by every concrete type carried in an
/// [`IndexInfoCarrier`]. The blanket impl below gives such a type an
/// [`IndexInfoLive`] impl whose `index_info_tag` returns [`Self::TAG`]. Mirrors
/// [`crate::amopaque::AmOpaqueType`].
///
/// SOUNDNESS CONTRACT: [`TAG`](Self::TAG) must be unique across all types ever
/// carried in an `IndexInfoCarrier`. The tag-checked downcast assumes `tag ==
/// T::TAG` implies the payload really is a `T` (modulo lifetimes); a collision
/// would let a downcast hand back a `&mut T` aliasing bytes of a different type
/// — undefined behavior. Only the canonical `IndexInfo<'mcx>` rides here today,
/// using [`INDEX_INFO_TAG`].
pub trait IndexInfoTagged<'mcx>: 'mcx {
    /// This type's process-stable, repo-unique tag.
    const TAG: AmOpaqueTag;
}

impl<'mcx, T: IndexInfoTagged<'mcx>> IndexInfoLive<'mcx> for T {
    fn index_info_tag(&self) -> AmOpaqueTag {
        T::TAG
    }

    fn live_type_name(&self) -> &'static str {
        type_name::<T>()
    }
}

/// The tag the canonical `nodes::execnodes::IndexInfo<'mcx>` payload uses
/// for its [`IndexInfoTagged`] impl. Defined here (alongside the carrier) so
/// the one concrete type that rides in an `IndexInfoCarrier` is auditable from
/// the carrier's home, mirroring the [`crate::amopaque::tags`] registry.
/// `"idx_inf\0"` in ASCII.
pub const INDEX_INFO_TAG: AmOpaqueTag = AmOpaqueTag(0x6964_785f_696e_6600);

/// `IndexInfo *` as carried across the index-AM dispatch vtable.
///
/// Holds the caller's live `&'a mut IndexInfo<'mcx>` as a type-erased
/// [`IndexInfoLive`] trait object. Two lifetimes — exactly as C's `IndexInfo *`
/// argument is independent of the per-call memory context:
///
/// * `'a` — the *borrow* of the caller's `IndexInfo`. The caller (index.c /
///   executor) typically holds it through a shorter borrow than the row/arena
///   lifetime (e.g. `&mut indstate.index_infos[i]`), so this is distinct from
///   and shorter than `'mcx`.
/// * `'mcx` — pins the *payload type* (`IndexInfo<'mcx>`), the arena the
///   expressions/predicate/context borrow from.
///
/// Only the concrete *type* is erased across the F0→F1 edge (the tag check makes
/// the downcast sound); no lifetime is erased to `'static`. The dispatch vtable
/// names the callback as `for<'mcx, 'a> fn(..., &mut IndexInfoCarrier<'a,
/// 'mcx>)`, so each call freely chooses both lifetimes — the carrier never
/// outlives the borrow it wraps. `None`/[`empty`](Self::empty) models the C
/// `NULL`.
pub struct IndexInfoCarrier<'a, 'mcx: 'a> {
    live: Option<&'a mut (dyn IndexInfoLive<'mcx> + 'mcx)>,
}

impl<'a, 'mcx: 'a> core::fmt::Debug for IndexInfoCarrier<'a, 'mcx> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("IndexInfoCarrier")
            .field("live", &self.live.as_ref().map(|l| l.live_type_name()))
            .finish()
    }
}

impl<'a, 'mcx: 'a> IndexInfoCarrier<'a, 'mcx> {
    /// Wrap the caller's live `&'a mut IndexInfo<'mcx>` (as an erased
    /// [`IndexInfoLive`]) for hand-off to the AM dispatch layer. The concrete
    /// type stays hidden behind the trait object; its tag lets the AM adapter
    /// downcast it back. The borrow `'a` need not equal `'mcx` (C's `IndexInfo
    /// *` argument is independent of the per-call memory context). Constructed
    /// by the build/insert caller (index.c / executor under #334), which owns
    /// the underlying `IndexInfo`.
    pub fn new(live: &'a mut (dyn IndexInfoLive<'mcx> + 'mcx)) -> Self {
        IndexInfoCarrier { live: Some(live) }
    }

    /// The C `NULL` `IndexInfo *` — an empty carrier. The AM adapter that
    /// dereferences the info will fail its [`downcast_mut`](Self::downcast_mut),
    /// matching a C NULL-pointer deref being a programming error.
    pub fn empty() -> Self {
        IndexInfoCarrier { live: None }
    }

    /// True if the carried payload is a `T` (an empty carrier → false).
    pub fn is<T: IndexInfoTagged<'mcx>>(&self) -> bool {
        matches!(&self.live, Some(l) if l.index_info_tag() == T::TAG)
    }

    /// Safe, tag-checked downcast to `&mut T`. Returns `None` on an empty
    /// carrier or a tag mismatch — the analogue of `<dyn Any>::downcast_mut`
    /// for the `'mcx`-bearing `IndexInfo<'mcx>`.
    ///
    /// SAFETY ARGUMENT: identical to [`crate::amopaque::AmOpaque::downcast_mut`].
    /// The cast `*mut dyn IndexInfoLive -> *mut T` is performed only after
    /// confirming the payload's tag equals `T::TAG`. By the
    /// [`IndexInfoTagged`] uniqueness contract, equal tags imply the value was
    /// constructed as a `T` (its lifetime parameters may differ, but lifetimes
    /// are erased at runtime and do not affect layout). The data pointer of the
    /// `dyn IndexInfoLive` object points at the `T` it was unsized from, so
    /// reinterpreting it as `*mut T` yields a valid `&mut T`; `&mut self` plus
    /// the held unique `&mut` reference make this the exclusive borrow.
    pub fn downcast_mut<'b, T: IndexInfoTagged<'mcx>>(&'b mut self) -> Option<&'b mut T> {
        let live = self.live.as_deref_mut()?;
        if live.index_info_tag() == T::TAG {
            // SAFETY: tag check above proved the payload is a `T`; `&mut self`
            // plus the held unique `&mut` make this the exclusive borrow.
            Some(unsafe { &mut *(live as *mut (dyn IndexInfoLive<'mcx> + 'mcx) as *mut T) })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A `'mcx`-bearing test payload — the exact shape that defeats `dyn Any`:
    // it borrows from the `'mcx` arena and so is not `'static`.
    struct McxBearing<'mcx> {
        marker: core::marker::PhantomData<mcx::Mcx<'mcx>>,
        value: u32,
    }

    impl<'mcx> IndexInfoTagged<'mcx> for McxBearing<'mcx> {
        const TAG: AmOpaqueTag = AmOpaqueTag(0xfeed_1111_0000_0001);
    }

    // A second `'mcx` type with a different tag, to prove wrong-T → None.
    struct OtherMcx<'mcx> {
        marker: core::marker::PhantomData<mcx::Mcx<'mcx>>,
    }

    impl<'mcx> IndexInfoTagged<'mcx> for OtherMcx<'mcx> {
        const TAG: AmOpaqueTag = AmOpaqueTag(0xfeed_1111_0000_0002);
    }

    #[test]
    fn round_trip_downcast_of_mcx_bearing_type() {
        let mut payload: McxBearing<'_> = McxBearing {
            marker: core::marker::PhantomData,
            value: 0x1234_5678,
        };
        let mut carrier = IndexInfoCarrier::new(&mut payload);
        // Correct-T downcast succeeds and yields the stored value.
        assert!(carrier.is::<McxBearing>());
        assert_eq!(
            carrier.downcast_mut::<McxBearing>().expect("same T").value,
            0x1234_5678
        );
        // downcast_mut round-trips a mutation.
        carrier.downcast_mut::<McxBearing>().expect("same T").value = 0x99;
        assert_eq!(carrier.downcast_mut::<McxBearing>().unwrap().value, 0x99);
    }

    #[test]
    fn wrong_type_downcast_returns_none() {
        let mut payload: McxBearing<'_> = McxBearing {
            marker: core::marker::PhantomData,
            value: 7,
        };
        let mut carrier = IndexInfoCarrier::new(&mut payload);
        // The payload is a `McxBearing`, so an `OtherMcx` downcast (different
        // tag) must return None rather than a bogus reference.
        assert!(carrier.downcast_mut::<OtherMcx>().is_none());
        assert!(!carrier.is::<OtherMcx>());
    }

    #[test]
    fn empty_carrier_downcasts_to_none() {
        let mut carrier: IndexInfoCarrier<'_, '_> = IndexInfoCarrier::empty();
        assert!(carrier.downcast_mut::<McxBearing>().is_none());
        assert!(!carrier.is::<McxBearing>());
    }

    // The borrow lifetime `'a` may be strictly shorter than the payload type's
    // `'mcx`: a carrier built from a short `&mut` of an `IndexInfo<'long>` is
    // exactly the catalog-indexing call shape (`&mut indstate.index_infos[i]`).
    #[test]
    fn borrow_shorter_than_payload_lifetime() {
        let mut payload: McxBearing<'static> = McxBearing {
            marker: core::marker::PhantomData,
            value: 42,
        };
        {
            let mut carrier = IndexInfoCarrier::new(&mut payload);
            assert_eq!(carrier.downcast_mut::<McxBearing>().unwrap().value, 42);
        }
        // `payload` is usable again after the short-lived carrier is dropped.
        payload.value = 43;
        assert_eq!(payload.value, 43);
    }
}
