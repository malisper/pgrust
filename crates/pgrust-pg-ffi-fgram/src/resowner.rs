use crate::types::Datum;
use core::ffi::{c_char, c_uint, c_void};

pub type ResourceReleasePhase = c_uint;
pub type ResourceReleasePriority = u32;

pub const RESOURCE_RELEASE_BEFORE_LOCKS: ResourceReleasePhase = 1;
pub const RESOURCE_RELEASE_LOCKS: ResourceReleasePhase = 2;
pub const RESOURCE_RELEASE_AFTER_LOCKS: ResourceReleasePhase = 3;

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

pub type ResourceReleaseCallback = fn(ResourceReleasePhase, bool, bool, *mut c_void);

pub type ResourceReleaseResourceCallback = unsafe extern "C" fn(Datum);
pub type ResourceDebugPrintCallback = unsafe extern "C" fn(Datum) -> *mut c_char;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ResourceOwnerDesc {
    pub name: *const c_char,
    pub release_phase: ResourceReleasePhase,
    pub release_priority: ResourceReleasePriority,
    pub ReleaseResource: Option<ResourceReleaseResourceCallback>,
    pub DebugPrint: Option<ResourceDebugPrintCallback>,
}

unsafe impl Sync for ResourceOwnerDesc {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn resource_owner_desc_layout_is_c_compatible() {
        assert_eq!(offset_of!(ResourceOwnerDesc, name), 0);
        assert_eq!(offset_of!(ResourceOwnerDesc, release_phase), 8);
        assert_eq!(offset_of!(ResourceOwnerDesc, release_priority), 12);
        assert_eq!(offset_of!(ResourceOwnerDesc, ReleaseResource), 16);
        assert_eq!(offset_of!(ResourceOwnerDesc, DebugPrint), 24);
        assert_eq!(size_of::<ResourceOwnerDesc>(), 32);
        assert_eq!(align_of::<ResourceOwnerDesc>(), 8);
    }
}
