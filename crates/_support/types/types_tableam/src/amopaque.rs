//! `void *opaque` / `void *am_private` — the access-method-private payload that
//! rides in the generic scan descriptors (`IndexScanDescData.opaque`,
//! `TableScanDescData.am_private`, `IndexFetchTableData.am_private`).
//!
//! In C these are bare `void *` slots the AM allocates in `ambeginscan` /
//! `scan_begin` and casts back to its own struct on every callback. The owned
//! model needs the same inherited opacity but made `'mcx`-safe: the AM working
//! state (`nbtree`'s `BTScanOpaqueData<'mcx>`, `hash`'s scan state, heap's
//! `HeapScanDescData<'mcx>`) is pervasively `'mcx` (PgVec/PgBox/Mcx arena,
//! convention A), and `'mcx` types cannot ride in `core::any::Any` — `Any` is
//! `'static`-bound, so a `'mcx` value will not coerce to `Box<dyn Any>` and
//! `Box<dyn Any + 'mcx>` has no safe downcast.
//!
//! [`AmOpaque`] is the faithful Rust rendering of C's `void *opaque`: an
//! object-safe trait carrying a stable per-type tag, stored behind a
//! `PgBox<'mcx, dyn AmOpaque<'mcx>>`, with a tag-checked safe downcast. This is
//! NOT new opacity (no side table, no registry) — it is the same erased
//! AM-private pointer the C carries, with the unsafe cast encapsulated and
//! proven sound by the tag check.

use core::any::type_name;

/// A process-stable tag identifying the concrete type behind a `dyn
/// AmOpaque<'mcx>`. Plays the role `core::any::TypeId` plays for `dyn Any`, but
/// works for non-`'static` (`'mcx`-bearing) types because it is supplied by the
/// type itself rather than derived from a `'static` bound.
///
/// Each concrete AM-opaque type assigns itself a tag that is unique across all
/// types ever stored in an AM-opaque slot. The tag value identifies the type
/// regardless of its lifetime parameters: an `NbtScan<'a>` and an `NbtScan<'b>`
/// share one tag (they are the same type modulo lifetime, and lifetimes do not
/// affect layout), which is exactly what a sound transmute-back requires.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct AmOpaqueTag(pub u64);

/// The erased AM-private payload — C's `void *opaque` / `void *am_private`.
///
/// Object-safe: the only methods take `&self` and return plain values, so
/// `dyn AmOpaque<'mcx>` is a valid trait object.
///
/// Implementors should use [`AmOpaqueType`] (which supplies the tag via a const
/// and provides the trait methods for free); implementing `AmOpaque` directly
/// is possible but the soundness of [`downcast_ref`](#method.downcast_ref)
/// relies on tag uniqueness, so the [`AmOpaqueType::TAG`] discipline must be
/// honored.
pub trait AmOpaque<'mcx> {
    /// The tag of the concrete `Self` behind this trait object.
    fn am_opaque_tag(&self) -> AmOpaqueTag;

    /// A human-readable name for the concrete type, for panic/debug messages
    /// only (never used for the downcast check).
    fn am_opaque_type_name(&self) -> &'static str;
}

/// Marker implemented by every concrete type stored in an AM-opaque slot. The
/// blanket impl below gives such a type an [`AmOpaque`] impl whose
/// `am_opaque_tag` returns [`Self::TAG`].
///
/// SOUNDNESS CONTRACT: [`TAG`](Self::TAG) must be unique across all types that
/// are ever stored in any AM-opaque slot. The tag-checked downcast assumes
/// `tag == T::TAG` implies the payload really is a `T` (modulo lifetimes); a
/// collision would let a downcast hand back a `&T` aliasing bytes of a
/// different type — undefined behavior. Tags are assigned from the
/// [`tags`](crate::amopaque::tags) registry of constants in this module so all
/// known AM-opaque types are visible in one place and cannot collide.
pub trait AmOpaqueType<'mcx>: 'mcx {
    /// This type's process-stable, repo-unique tag.
    const TAG: AmOpaqueTag;
}

impl<'mcx, T: AmOpaqueType<'mcx>> AmOpaque<'mcx> for T {
    fn am_opaque_tag(&self) -> AmOpaqueTag {
        T::TAG
    }

    fn am_opaque_type_name(&self) -> &'static str {
        type_name::<T>()
    }
}

impl<'mcx> dyn AmOpaque<'mcx> + 'mcx {
    /// True if the payload behind this trait object is a `T`.
    pub fn is<T: AmOpaqueType<'mcx>>(&self) -> bool {
        self.am_opaque_tag() == T::TAG
    }

    /// Safe, tag-checked downcast to `&T`, returning `None` on a tag mismatch
    /// — the analogue of `<dyn Any>::downcast_ref` for `'mcx` payloads.
    ///
    /// SAFETY ARGUMENT: the cast `*const dyn AmOpaque -> *const T` is performed
    /// only after confirming `self.am_opaque_tag() == T::TAG`. By the
    /// [`AmOpaqueType`] uniqueness contract, equal tags imply the value was
    /// constructed as a `T` (its lifetime parameters may differ, but lifetimes
    /// are erased at runtime and do not affect layout). The data pointer of a
    /// `dyn AmOpaque` trait object points at the `T` it was unsized from, so
    /// reinterpreting it as `*const T` yields a valid `&T` with the borrow's
    /// lifetime. This mirrors how `core::any::Any::downcast_ref` is
    /// implemented (`*const dyn Any -> *const T` after a `TypeId` check), with
    /// `AmOpaqueTag` standing in for the `'static`-only `TypeId`.
    pub fn downcast_ref<T: AmOpaqueType<'mcx>>(&self) -> Option<&T> {
        if self.is::<T>() {
            // SAFETY: tag check above proved the payload is a `T`.
            Some(unsafe { &*(self as *const dyn AmOpaque<'mcx> as *const T) })
        } else {
            None
        }
    }

    /// Safe, tag-checked downcast to `&mut T`; see [`downcast_ref`] for the
    /// soundness argument (the data pointer and tag invariant are identical;
    /// `&mut self` guarantees exclusive access, so the produced `&mut T` is the
    /// unique borrow).
    ///
    /// [`downcast_ref`]: #method.downcast_ref
    pub fn downcast_mut<T: AmOpaqueType<'mcx>>(&mut self) -> Option<&mut T> {
        if self.is::<T>() {
            // SAFETY: tag check above proved the payload is a `T`; `&mut self`
            // is the unique borrow of the trait object.
            Some(unsafe { &mut *(self as *mut dyn AmOpaque<'mcx> as *mut T) })
        } else {
            None
        }
    }
}

/// The registry of AM-opaque type tags. Every concrete type that rides in an
/// AM-opaque slot picks its tag from here so all assignments are visible in one
/// place and uniqueness is reviewable at a glance. The numeric values are
/// arbitrary but must never be reused for a different type.
///
/// (The concrete AM types live in their own crates — `nbtree`, `hash`,
/// `heapam` — and `impl AmOpaqueType` there using these constants. Defining the
/// constants centrally here keeps the uniqueness contract auditable from the
/// carrier's home.)
pub mod tags {
    use super::AmOpaqueTag;

    /// `nbtree`'s `BTScanOpaqueData<'mcx>` / `NbtScan<'mcx>`.
    pub const NBT_SCAN: AmOpaqueTag = AmOpaqueTag(0x6e62_745f_7363_6e00); // "nbt_scn\0"
    /// `hash`'s scan state (`HashScanOpaqueData` / `HashScan<'mcx>`).
    pub const HASH_SCAN: AmOpaqueTag = AmOpaqueTag(0x6861_7368_5f73_636e); // "hash_scn"
    /// heap's `HeapScanDescData<'mcx>` (table-AM `am_private`).
    pub const HEAP_SCAN: AmOpaqueTag = AmOpaqueTag(0x6865_6170_5f73_636e); // "heap_scn"
    /// heap's `IndexFetchHeapData<'mcx>` (`IndexFetchTableData.am_private`).
    pub const HEAP_INDEX_FETCH: AmOpaqueTag = AmOpaqueTag(0x6865_6170_5f69_6678); // "heap_ifx"
    /// `brin`'s `BrinOpaque<'mcx>` (`BrinScan<'mcx>`) scan state.
    pub const BRIN_SCAN: AmOpaqueTag = AmOpaqueTag(0x6272_696e_5f73_636e); // "brin_scn"
    /// `gin`'s `GinScanOpaqueData<'mcx>` scan state (`IndexScanDescData.opaque`).
    pub const GIN_SCAN: AmOpaqueTag = AmOpaqueTag(0x6769_6e5f_7363_6e00); // "gin_scn\0"
    /// SP-GiST's `SpGistScanOpaqueData<'mcx>` scan state
    /// (`IndexScanDescData.opaque`, spgscan.c `spgbeginscan`).
    pub const SPGIST_SCAN: AmOpaqueTag = AmOpaqueTag(0x7370_675f_7363_6e00); // "spg_scn\0"
    /// GiST's `GISTScanOpaqueData<'mcx>` scan state
    /// (`IndexScanDescData.opaque`, gistscan.c `gistbeginscan`).
    pub const GIST_SCAN: AmOpaqueTag = AmOpaqueTag(0x6769_7374_5f73_636e); // "gist_scn"

    /// hash's cached `HashMetaPageData` stored in `rel->rd_amcache`
    /// (`_hash_getcachedmetap`). The SP-GiST/GIN/GiST `rd_amcache` types
    /// (`SpGistCache`/`GinState`/`GISTSTATE`) pick their own tags here as those
    /// AM campaigns land.
    pub const HASH_META: AmOpaqueTag = AmOpaqueTag(0x6861_7368_5f6d_6574); // "hash_met"
    /// SP-GiST's cached `SpGistCache` stored in `rel->rd_amcache`
    /// (spgutils.c `spgGetCache`).
    pub const SPGIST_CACHE: AmOpaqueTag = AmOpaqueTag(0x7370_675f_6361_6368); // "spg_cach"
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::{MemoryContext, Mcx, PgBox};

    // A `'mcx`-bearing test type — the exact shape that defeats `dyn Any`: it
    // borrows from the `'mcx` arena and so is not `'static`.
    struct McxBearing<'mcx> {
        marker: core::marker::PhantomData<Mcx<'mcx>>,
        value: u32,
    }

    impl<'mcx> AmOpaqueType<'mcx> for McxBearing<'mcx> {
        const TAG: AmOpaqueTag = AmOpaqueTag(0xdead_beef_0000_0001);
    }

    // A second `'mcx` type with a different tag, to prove wrong-T returns None.
    struct OtherMcx<'mcx> {
        marker: core::marker::PhantomData<Mcx<'mcx>>,
    }

    impl<'mcx> AmOpaqueType<'mcx> for OtherMcx<'mcx> {
        const TAG: AmOpaqueTag = AmOpaqueTag(0xdead_beef_0000_0002);
    }

    fn erase<'mcx>(mcx: Mcx<'mcx>) -> PgBox<'mcx, dyn AmOpaque<'mcx> + 'mcx> {
        let boxed: PgBox<'mcx, McxBearing<'mcx>> = mcx::alloc_in(
            mcx,
            McxBearing {
                marker: core::marker::PhantomData,
                value: 0x1234_5678,
            },
        )
        .unwrap();
        // The same unsize-through-raw-pointer pattern the rest of the repo uses
        // for `PgBox<'mcx, dyn Any>` (no `CoerceUnsized` on stable). `mcx::PgBox`
        // is `allocator_api2::boxed::Box`, which supports `?Sized` payloads.
        let (ptr, alloc) = PgBox::into_raw_with_allocator(boxed);
        // SAFETY: `ptr`/`alloc` came from `into_raw_with_allocator`; the cast
        // only attaches the `dyn AmOpaque` vtable.
        unsafe { PgBox::from_raw_in(ptr as *mut (dyn AmOpaque<'mcx> + 'mcx), alloc) }
    }

    #[test]
    fn round_trip_downcast_of_mcx_bearing_type() {
        let ctx = MemoryContext::new("amopaque-test");
        let mcx = ctx.mcx();
        let mut carrier = erase(mcx);
        // Correct-T downcast succeeds and yields the stored value.
        let got = (*carrier).downcast_ref::<McxBearing>().expect("same T");
        assert_eq!(got.value, 0x1234_5678);
        // downcast_mut round-trips a mutation.
        (*carrier)
            .downcast_mut::<McxBearing>()
            .expect("same T")
            .value = 0x99;
        assert_eq!((*carrier).downcast_ref::<McxBearing>().unwrap().value, 0x99);
    }

    #[test]
    fn wrong_type_downcast_returns_none() {
        let ctx = MemoryContext::new("amopaque-test");
        let mcx = ctx.mcx();
        let carrier = erase(mcx);
        // The payload is a `McxBearing`, so an `OtherMcx` downcast (different
        // tag) must return None rather than a bogus reference.
        assert!((*carrier).downcast_ref::<OtherMcx>().is_none());
    }

    #[test]
    fn is_matches_only_the_stored_type() {
        let ctx = MemoryContext::new("amopaque-test");
        let mcx = ctx.mcx();
        let carrier = erase(mcx);
        assert!((*carrier).is::<McxBearing>());
        assert!(!(*carrier).is::<OtherMcx>());
    }
}
