#[cfg(target_endian = "big")]
compile_error!("only the little-endian pg_crc32c layout is implemented");

#[cfg(target_arch = "aarch64")]
mod armv8;
mod legacy;
mod sb8;
#[cfg(target_arch = "x86_64")]
mod sse42;

#[cfg(target_arch = "aarch64")]
pub use armv8::pg_comp_crc32c_armv8;
pub use legacy::{legacy_crc32_lexeme, traditional_crc32};
pub use sb8::pg_comp_crc32c_sb8;
#[cfg(target_arch = "x86_64")]
pub use sse42::pg_comp_crc32c_sse42;

pub const CRC32C_INIT: u32 = 0xFFFF_FFFF;

#[inline(always)]
pub const fn fin_crc32c(crc: u32) -> u32 {
    crc ^ 0xFFFF_FFFF
}

// Hardware CRC resolves at compile time when the target enables it (FEAT_CRC32
// on aarch64, SSE4.2 on x86-64-v2+); featureless builds pay the resolve-once
// indirection below, mirroring C's *_choose.c function pointer. C 18.3's
// pg_comp_crc32c_avx512 (USE_AVX512_CRC32C_WITH_RUNTIME_CHECK) is not ported.
#[inline]
pub fn pg_comp_crc32c(crc: u32, data: &[u8]) -> u32 {
    #[cfg(all(target_arch = "aarch64", target_feature = "crc"))]
    // SAFETY: this arm only compiles when FEAT_CRC32 is a build-time feature.
    return unsafe { armv8::pg_comp_crc32c_armv8(crc, data) };
    #[cfg(all(target_arch = "aarch64", not(target_feature = "crc")))]
    return choose::comp(crc, data);
    #[cfg(all(target_arch = "x86_64", target_feature = "sse4.2"))]
    // SAFETY: this arm only compiles when SSE4.2 is a build-time feature.
    return unsafe { sse42::pg_comp_crc32c_sse42(crc, data) };
    #[cfg(all(target_arch = "x86_64", not(target_feature = "sse4.2")))]
    return choose::comp(crc, data);
    #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
    return sb8::pg_comp_crc32c_sb8(crc, data);
}

#[cfg(any(
    all(target_arch = "aarch64", not(target_feature = "crc")),
    all(target_arch = "x86_64", not(target_feature = "sse4.2"))
))]
mod choose {
    use std::sync::OnceLock;

    type Comp = fn(u32, &[u8]) -> u32;

    static COMP: OnceLock<Comp> = OnceLock::new();

    #[cfg(target_arch = "aarch64")]
    fn hw_detected(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: installed only after runtime detection of FEAT_CRC32.
        unsafe { crate::armv8::pg_comp_crc32c_armv8(crc, data) }
    }

    #[cfg(target_arch = "x86_64")]
    fn hw_detected(crc: u32, data: &[u8]) -> u32 {
        // SAFETY: installed only after runtime detection of SSE4.2.
        unsafe { crate::sse42::pg_comp_crc32c_sse42(crc, data) }
    }

    fn hw_available() -> bool {
        #[cfg(target_arch = "aarch64")]
        return std::arch::is_aarch64_feature_detected!("crc");
        #[cfg(target_arch = "x86_64")]
        return std::arch::is_x86_feature_detected!("sse4.2");
    }

    pub fn comp(crc: u32, data: &[u8]) -> u32 {
        let f = *COMP.get_or_init(|| {
            if hw_available() {
                hw_detected
            } else {
                crate::sb8::pg_comp_crc32c_sb8
            }
        });
        f(crc, data)
    }
}
