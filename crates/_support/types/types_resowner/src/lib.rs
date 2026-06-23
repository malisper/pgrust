//! Canonical resource-owner carrier types (`utils/resowner.h`).
//!
//! `ResourceOwner` is `typedef struct ResourceOwnerData *ResourceOwner` in C: a
//! pointer into the resowner subsystem's process-local tree of owner objects.
//! The owner bodies (`ResourceOwnerData`) live in the resowner owner crate's
//! process-local arena; this crate defines only the opaque, `Copy` handle that
//! every consumer threads around (and the `ResourceOwnerDesc` kind descriptor
//! that callers register their resource kinds with).
//!
//! Modeled as a slot+generation index into the arena, with a reserved `NULL`
//! sentinel mirroring the C null pointer. A stale handle to a freed-and-reused
//! slot is detected by the generation rather than silently aliasing.

#![allow(non_snake_case)]

use ::datum::Datum;

/// `ResourceOwner` (`utils/resowner.h`) — an opaque, `Copy` handle to a
/// resource owner object owned by the resowner subsystem's arena. The reserved
/// `NULL` value (`slot == u32::MAX`) models the C null pointer; the arena never
/// hands out that slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ResourceOwner {
    slot: u32,
    generation: u32,
}

impl ResourceOwner {
    /// The C `NULL` resource owner.
    pub const NULL: ResourceOwner = ResourceOwner {
        slot: u32::MAX,
        generation: 0,
    };

    /// Build a non-null handle from an arena slot and generation. Only the
    /// owning resowner crate should call this.
    pub const fn from_parts(slot: u32, generation: u32) -> ResourceOwner {
        ResourceOwner { slot, generation }
    }

    /// The arena slot index. Meaningless for `NULL`.
    pub const fn slot(self) -> u32 {
        self.slot
    }

    /// The slot generation.
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// `owner == NULL`.
    pub const fn is_null(self) -> bool {
        self.slot == u32::MAX
    }
}

impl Default for ResourceOwner {
    fn default() -> Self {
        ResourceOwner::NULL
    }
}

/// `typedef enum` argument passed back to a [`ResourceReleaseCallback`] — was a
/// `void *arg` in the C typedef. Modeled as an opaque owned payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceReleaseCallbackArg {}

/// `typedef int ResourceReleasePhase` value space (`utils/resowner.h`). Matches
/// the C enumerator order `RESOURCE_RELEASE_BEFORE_LOCKS = 1` etc.
pub type ResourceReleasePhase = u32;
/// `typedef uint32 ResourceReleasePriority`.
pub type ResourceReleasePriority = u32;

pub const RESOURCE_RELEASE_BEFORE_LOCKS: ResourceReleasePhase = 1;
pub const RESOURCE_RELEASE_LOCKS: ResourceReleasePhase = 2;
pub const RESOURCE_RELEASE_AFTER_LOCKS: ResourceReleasePhase = 3;

// Built-in release priorities for the BEFORE_LOCKS phase.
pub const RELEASE_PRIO_BUFFER_IOS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_BUFFER_PINS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_RELCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_DSMS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_JIT_CONTEXTS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_CRYPTOHASH_CONTEXTS: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_HMAC_CONTEXTS: ResourceReleasePriority = 700;

// Built-in release priorities for the AFTER_LOCKS phase.
pub const RELEASE_PRIO_CATCACHE_REFS: ResourceReleasePriority = 100;
pub const RELEASE_PRIO_CATCACHE_LIST_REFS: ResourceReleasePriority = 200;
pub const RELEASE_PRIO_PLANCACHE_REFS: ResourceReleasePriority = 300;
pub const RELEASE_PRIO_TUPDESC_REFS: ResourceReleasePriority = 400;
pub const RELEASE_PRIO_SNAPSHOT_REFS: ResourceReleasePriority = 500;
pub const RELEASE_PRIO_FILES: ResourceReleasePriority = 600;
pub const RELEASE_PRIO_WAITEVENTSETS: ResourceReleasePriority = 700;

pub const RELEASE_PRIO_FIRST: ResourceReleasePriority = 1;
pub const RELEASE_PRIO_LAST: ResourceReleasePriority = u32::MAX;

/// `typedef void (*ResourceReleaseCallback)(...)` — an add-on release callback.
pub type ResourceReleaseCallback =
    fn(ResourceReleasePhase, bool, bool, Option<Box<ResourceReleaseCallbackArg>>);

/// `void (*ReleaseResource)(Datum res)` — per-kind release callback.
pub type ResourceReleaseResourceCallback = fn(Datum);
/// `char *(*DebugPrint)(Datum res)` — per-kind leak-warning formatter.
pub type ResourceDebugPrintCallback = fn(Datum) -> Option<String>;

/// `typedef struct ResourceOwnerDesc` (`utils/resowner.h`) — describes a kind of
/// resource that can be remembered by a resource owner. Callers define these as
/// file-scope statics; pointer identity (`&'static`) distinguishes kinds.
#[derive(Clone, Debug)]
pub struct ResourceOwnerDesc {
    /// `const char *name` — for debug printouts.
    pub name: Option<String>,
    /// `ResourceReleasePhase release_phase`.
    pub release_phase: ResourceReleasePhase,
    /// `ResourceReleasePriority release_priority`.
    pub release_priority: ResourceReleasePriority,
    /// `void (*ReleaseResource)(Datum res)`.
    pub ReleaseResource: Option<ResourceReleaseResourceCallback>,
    /// `char *(*DebugPrint)(Datum res)`.
    pub DebugPrint: Option<ResourceDebugPrintCallback>,
}
