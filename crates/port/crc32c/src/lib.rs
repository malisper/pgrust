#[cfg(target_endian = "big")]
compile_error!("only the little-endian pg_crc32c layout is implemented");

#[cfg(target_arch = "aarch64")]
mod armv8;
mod legacy;
mod sb8;

#[cfg(target_arch = "aarch64")]
pub use armv8::pg_comp_crc32c_armv8;
pub use legacy::{legacy_crc32_lexeme, traditional_crc32};
pub use sb8::pg_comp_crc32c_sb8;

pub const CRC32C_INIT: u32 = 0xFFFF_FFFF;

#[inline(always)]
pub const fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}

// Dispatch is resolved at compile time when the target enables FEAT_CRC32
// (aarch64-apple-darwin baseline, -Ctarget-cpu=neoverse-v2 fleet builds);
// only featureless aarch64 builds pay the resolve-once indirection below.
// x86-64 hardware CRC (sse42) is not ported yet: non-aarch64 gets sb8, the
// same as a C build without USE_SSE42_CRC32C.
#[inline]
pub fn pg_comp_crc32c(crc: u32, data: &[u8]) -> u32 {
    #[cfg(all(target_arch = "aarch64", target_feature = "crc"))]
    // SAFETY: this arm only compiles when FEAT_CRC32 is a build-time target
    // feature.
    return unsafe { armv8::pg_comp_crc32c_armv8(crc, data) };
    #[cfg(all(target_arch = "aarch64", not(target_feature = "crc")))]
    return choose::comp(crc, data);
    #[cfg(not(target_arch = "aarch64"))]
    return sb8::pg_comp_crc32c_sb8(crc, data);
}

#[cfg(all(target_arch = "aarch64", not(target_feature = "crc")))]
mod choose {
    use std::sync::OnceLock;

    type Comp = fn(u32, &[u8]) -> u32;

    static COMP: OnceLock<Comp> = OnceLock::new();

    fn armv8_detected(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: installed only after runtime detection of FEAT_CRC32.
        unsafe { crate::armv8::pg_comp_crc32c_armv8(crc, data) }
    }

    pub fn comp(crc: u32, data: &[u8]) -> u32 {
        let f = *COMP.get_or_init(|| {
            if std::arch::is_aarch64_feature_detected!("crc") {
                armv8_detected
            } else {
                crate::sb8::pg_comp_crc32c_sb8
            }
        });
        f(crc, data)
    }
}
