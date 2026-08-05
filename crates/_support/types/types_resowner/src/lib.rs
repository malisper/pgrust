#![no_std]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

use ::datum::Datum;
use ::mcx::{Mcx, PgString};
use ::types_error::PgResult;

// C's `ResourceOwnerData *` as a slot+generation arena handle; slot == u32::MAX
// is the reserved C NULL, and a stale generation is detected instead of aliasing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ResourceOwner {
    slot: u32,
    generation: u32,
}

const _: () = assert!(core::mem::size_of::<ResourceOwner>() == 8);

impl ResourceOwner {
    pub const NULL: ResourceOwner = ResourceOwner {
        slot: u32::MAX,
        generation: 0,
    };

    pub const fn from_parts(slot: u32, generation: u32) -> ResourceOwner {
        ResourceOwner { slot, generation }
    }

    pub const fn slot(self) -> u32 {
        self.slot
    }

    pub const fn generation(self) -> u32 {
        self.generation
    }

    pub const fn is_null(self) -> bool {
        self.slot == u32::MAX
    }
}

impl Default for ResourceOwner {
    fn default() -> Self {
        ResourceOwner::NULL
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
#[repr(u32)]
pub enum ResourceReleasePhase {
    RESOURCE_RELEASE_BEFORE_LOCKS = 1,
    RESOURCE_RELEASE_LOCKS = 2,
    RESOURCE_RELEASE_AFTER_LOCKS = 3,
}

pub use ResourceReleasePhase::*;

pub type ResourceReleasePriority = u32;

pub const RELEASE_PRIO_BUFFER_IOS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_BUFFER_PINS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_RELCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_DSMS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_JIT_CONTEXTS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_CRYPTOHASH_CONTEXTS: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_HMAC_CONTEXTS: ResourceReleasePriority = 700;

pub const RELEASE_PRIO_CATCACHE_REFS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_CATCACHE_LIST_REFS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_PLANCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_TUPDESC_REFS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_SNAPSHOT_REFS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_FILES: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_WAITEVENTSETS: ResourceReleasePriority = 700;

pub const RELEASE_PRIO_FIRST: ResourceReleasePriority = 1;
pub const RELEASE_PRIO_LAST: ResourceReleasePriority = u32::MAX;

// C's `void *arg` crosses as a Datum-sized word, same width as the C pointer.
pub type ResourceReleaseCallback = fn(ResourceReleasePhase, bool, bool, Datum);

pub type ResourceReleaseResourceCallback = fn(Datum);
// C DebugPrint pallocs into CurrentMemoryContext; the context is explicit here.
pub type ResourceDebugPrintCallback = for<'a> fn(Mcx<'a>, Datum) -> PgResult<PgString<'a>>;

// Kinds are file-scope statics distinguished by `&'static` identity, as in C.
#[derive(Debug)]
pub struct ResourceOwnerDesc {
    pub name: &'static str,
    pub release_phase: ResourceReleasePhase,
    pub release_priority: ResourceReleasePriority,
    pub ReleaseResource: ResourceReleaseResourceCallback,
    pub DebugPrint: Option<ResourceDebugPrintCallback>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_handle_matches_c_null_pointer_semantics() {
        assert!(ResourceOwner::NULL.is_null());
        assert!(ResourceOwner::default().is_null());
        assert_eq!(ResourceOwner::default(), ResourceOwner::NULL);

        let owner = ResourceOwner::from_parts(3, 7);
        assert!(!owner.is_null());
        assert_eq!(owner.slot(), 3);
        assert_eq!(owner.generation(), 7);
        assert_ne!(owner, ResourceOwner::from_parts(3, 8));
    }

    #[test]
    fn phases_and_priorities_match_resowner_h() {
        assert_eq!(RESOURCE_RELEASE_BEFORE_LOCKS as u32, 1);
        assert_eq!(RESOURCE_RELEASE_LOCKS as u32, 2);
        assert_eq!(RESOURCE_RELEASE_AFTER_LOCKS as u32, 3);

        assert_eq!(RELEASE_PRIO_BUFFER_IOS, 100);
        assert_eq!(RELEASE_PRIO_HMAC_CONTEXTS, 700);
        assert_eq!(RELEASE_PRIO_CATCACHE_REFS, 100);
        assert_eq!(RELEASE_PRIO_WAITEVENTSETS, 700);
        assert_eq!(RELEASE_PRIO_FIRST, 1);
        assert_eq!(RELEASE_PRIO_LAST, u32::MAX);
    }

    #[test]
    fn kind_identity_is_static_pointer_identity() {
        fn release(_d: Datum) {}
        static KIND_A: ResourceOwnerDesc = ResourceOwnerDesc {
            name: "a",
            release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
            release_priority: RELEASE_PRIO_BUFFER_PINS,
            ReleaseResource: release,
            DebugPrint: None,
        };
        static KIND_B: ResourceOwnerDesc = ResourceOwnerDesc {
            name: "b",
            release_phase: RESOURCE_RELEASE_AFTER_LOCKS,
            release_priority: RELEASE_PRIO_FILES,
            ReleaseResource: release,
            DebugPrint: None,
        };
        assert!(core::ptr::eq(&KIND_A, &KIND_A));
        assert!(!core::ptr::eq(&KIND_A, &KIND_B));
    }
}
