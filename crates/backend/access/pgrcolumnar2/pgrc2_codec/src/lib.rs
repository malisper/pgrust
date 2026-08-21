//! # pgrc2_codec — the pgrcolumnar2 codec family (chunk M3-C)
//!
//! The §7-matrix encode/decode kernel pairs over the FROZEN M3-A surface
//! (`docs/design/pgrc2-format.md`, cited as `spec §N`; ABI = spec §19) and
//! M3-B's vendored payload layouts. Chunk row + exit slice:
//! `docs/design/lanev3-m3-chunks.md` §2/§5 M3-C.
//!
//! ## Module map
//!
//! | module | encoding ids | notes |
//! |---|---|---|
//! | [`bytefor`] | 2 BYTE_FOR | int-family primary (spec §6.10 frozen); per-width monomorphized kernels |
//! | [`deltafor`] | 4 DELTA_FOR | cold/size arm only (S4 struck it from hot tiers) |
//! | [`ffor`] | 3 FFOR_INTERLEAVE | elected fused tier over the vendored `alp::bitpack` layout |
//! | [`alpc`] | 5 ALP / 6 ALP_RD | vendored self-describing granule frames (f64 + the SB-5 f32 arm); allocation-free hot reader; bit-exact incl. NaN payloads |
//! | [`fsst`] | 12 FSST | SB-4/OD-5 first-class string compression: per-(column,part) symbol table in the section header bytes; dict-loser text arm |
//! | [`boolbm`] | 7 BOOL_BITMAP | the §6.6 bitmap layout |
//! | [`dictcodes`] | 8 DICT_CODES | §6.11 frozen blocks; global codes; `decode_codes` face |
//! | [`packednum`] | 9 PACKED_NUMERIC | vendored A6b fixed-scale semantics over BYTE_FOR mantissa frames |
//! | [`arraydual`] | 10 ARRAY_DUAL | STRUCTURAL election: sizes+elements substreams + the assemble leg |
//! | [`jsonbshred`] | 11 JSONB_SHRED | STRUCTURAL election: image lane + typed path-lane adapters over the vendored shredder |
//! | [`verbhot`] | 0 VERBATIM | gate-3 hot kernels over the frozen §6.7/§6.8 layout (reference bodies in `pgrc2_format::verbatim` stay the oracle) |
//! | [`lz4`], [`wrapper`] | wrapper ids 1/2 | LZ4 block codec + §6.4 wrapped-section assembly; Zstd arm over the workspace dep (CMP-A, O-CMP-2(a); wasm32 = typed refusal) |
//! | [`election`] | — | analyze-then-elect, ≥10% encoding gate + ≥20% wrapper gate (O-CMP-4(a)), incompressible guard, typed demotions, the verify-at-encode stream driver |
//! | [`dispatch`] | — | registry assembly + stream-entry → `KernelKey` normalization |
//! | [`section`] | — | shared decode-side §6.4/§6.5 plumbing |
//!
//! VERBATIM (0) decodes through this crate's hot kernels ([`verbhot`]) —
//! the rewrite the previous note licensed on microbench evidence at M3-L,
//! which arrived (M3 exit §2.3: 0.03× through the reference witness) and
//! was ruled at the gate-3 fix. CONST (1) still decodes through the
//! reference vtables (0.98× parity — nothing to recover); the reference
//! VERBATIM bodies stay as the differential oracle
//! (`tests/kernel_diff.rs`).
//!
//! ## Laws carried (review checklist)
//!
//! - Fn-pointer dispatch per (encoding × class × width), monomorphized per
//!   width — never a width switch in a hot path (S4 pow2-switch law;
//!   pinned by `tests/dispatch_shape.rs`).
//! - Kernels are ctx-relative, allocation-free, no-panic, arena-out,
//!   bounds-validated even after CRC passes (typed error, never UB).
//! - `decode_sel ≡ decode_full ∘ select` (property-tested per encoding).
//! - Float paths bit-exact incl. NaN payloads (ALP exception mechanism;
//!   the SB-5 f32 arm carries the identical `f32::to_bits` law).
//! - String-class outputs are varlena-shaped, ≥8-aligned in the arena
//!   (StrView §7b — `tests/strview.rs` pins it born-RED-style).
//! - Elections input-decidable; refusal demotes typed ([`election`]).
//! - FSST id 12 is FIRST-CLASS (SB-4/OD-5, lanev4): [`fsst`] implements
//!   the Boncz/Neumann scheme; the per-(column,part) symbol table rides
//!   the stream's section header bytes inside CRC-over-wrapped, and
//!   FSST-under-zstd is encoding+wrapper (two layers, legal).
//! - Pure crate: no I/O, no clocks, no thread-locals, no env. The one
//!   process-global is the registry `OnceLock` (init-once, spec §19.5).
//! - `unsafe` is confined to [`section::varlena_payload`] (the encode-side
//!   datum-image deref the reference crate also needs), SAFETY-documented.

pub mod alpc;
pub mod arraydual;
pub mod boolbm;
pub mod bytefor;
pub mod deltafor;
pub mod dictcodes;
pub mod dispatch;
pub mod election;
pub mod ffor;
pub mod fsst;
pub mod jsonbshred;
pub mod lz4;
pub mod packednum;
pub mod section;
pub mod verbhot;
pub mod wrapper;

pub use dispatch::{registry, stream_kernel_key};
pub use election::{
    elect_array, elect_bool, elect_float, elect_int, elect_numeric, elect_stream_wrapper,
    elect_text_dict, elect_text_fsst, encode_stream, wins_by_ten_percent, wins_by_twenty_percent,
    Demotion, DictArm, Election, FsstArm, StreamBuild,
};
pub use pgrc2_format as format;

#[cfg(test)]
mod tests;
