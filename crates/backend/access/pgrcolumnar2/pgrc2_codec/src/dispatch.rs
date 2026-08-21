//! Dispatch assembly (spec §19.5): the registry over every kernel list in
//! this crate + the reference Verbatim/Const vtables, and the ONE key
//! normalization readers use to go from stream-entry fields to a
//! [`KernelKey`].
//!
//! Laws: resolution happens at stream open (M3-F), once per (stream,
//! extent) — never in or near the hot loop; unknown and reserved encoding
//! IDs refuse typed BEFORE any table walk (`EncodingId::resolve` inside
//! `CodecRegistry::resolve`, born-RED-tested in the format crate and again
//! in `tests/corrupt.rs` here); kernels are monomorphized per width behind
//! fn pointers (the S4 pow2-switch law — `tests/dispatch_shape.rs` pins
//! that the per-width entries are DISTINCT function pointers).

use crate::{alpc, boolbm, bytefor, deltafor, dictcodes, ffor, fsst, packednum, verbhot};
use pgrc2_format::abi::{CodecRegistry, CodecVtable, KernelKey};
use pgrc2_format::verbatim::const_reference_vtables;
use pgrc2_format::FormatResult;
use pgsync::OnceLock;

/// The process-wide codec registry: this crate's hot kernels (VERBATIM's
/// gate-3 hot set included — M3 exit §2.3 license), then the reference
/// CONST vtables (disjoint key spaces; order is documentation, not
/// arbitration). The reference VERBATIM vtables stay out of the registry
/// and serve as the hot kernels' differential oracle
/// (`tests/kernel_diff.rs`). Assembled once at first use — open-time
/// resolution only.
pub fn registry() -> &'static CodecRegistry {
    static REG: OnceLock<CodecRegistry> = OnceLock::new();
    REG.get_or_init(|| {
        let mut entries: Vec<&'static CodecVtable> = Vec::new();
        entries.extend(bytefor::VT_BYTE_FOR.iter());
        entries.push(&deltafor::VT_DELTA_FOR);
        entries.push(&ffor::VT_FFOR);
        entries.push(&alpc::VT_ALP);
        entries.push(&alpc::VT_ALP_RD);
        entries.push(&alpc::VT_ALP_F32);
        entries.push(&boolbm::VT_BOOL_BITMAP);
        entries.push(&dictcodes::VT_DICT_VARLENA);
        entries.push(&dictcodes::VT_DICT_FIXED);
        entries.push(&fsst::VT_FSST_VARLENA);
        entries.extend(packednum::VT_PACKED_NUMERIC.iter());
        entries.extend(verbhot::VT_VERBATIM_HOT.iter());
        entries.push(&verbhot::VT_VERBATIM_VARLENA_HOT);
        entries.extend(const_reference_vtables());
        CodecRegistry::new(Box::leak(entries.into_boxed_slice()))
    })
}

/// Normalize stream-entry fields to the kernel key its codec registered.
/// The mapping moved to `pgrc2_format::enc::stream_kernel_key` (A-lane
/// amendment M3-A2 — the reader must consult the SAME normalization but
/// cannot depend on this crate); this delegation keeps the codec-crate face
/// stable for existing consumers.
pub fn stream_kernel_key(encoding: u16, class: u8, stream_width: u8) -> FormatResult<KernelKey> {
    pgrc2_format::enc::stream_kernel_key(encoding, class, stream_width)
}
