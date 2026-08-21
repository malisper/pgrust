// PROVENANCE (order-key extraction, ruling 2026-08-08): extracted VERBATIM
// from pgrc2_meta @ 1bd74c9e27 (lanev3, PR #440) — the M3-E implementations
// of the functions M3-B was originally chartered to vendor. No semantic
// edits during the move: function bodies, constants, and seeds are
// byte-identical to E's; the golden pins travelled with them (see
// `tests` in each module). The coarse-key LAW (ExactKey/CoarseKey types,
// verdict enums, the finalize assert) deliberately did NOT move — it stays
// in `pgrc2_meta`, which wraps these raw functions in its typed wall.

//! # order_key — the shared order-embedding / bloom / hash support crate
//!
//! The ONE implementation of the pgrcolumnar2 order-embedding transforms
//! (float → sortable i64, uuid/macaddr fixed-image embeds, text/bytea
//! prefix embeds), the pure bloom-filter core, and the bloom/NDV/fingerprint
//! hash family. `pgrc2_meta` (seal + probe — one definition, one drift
//! surface) consumes it today; M4 abbreviated keys is the anticipated second
//! consumer. Any would-be second implementation of these functions is a
//! defect: depend on this crate instead.
//!
//! Functions here return RAW values (`i64` keys, `u64` hashes). Key-kind
//! semantics (Exact vs Coarse — spec §8.1's coarse-key law) are the
//! CONSUMER's to enforce: `pgrc2_meta::key` wraps each transform in its
//! `ExactKey`/`CoarseKey` types, and each transform's doc below states the
//! class its output must be treated as. Do not prove Eq AllPass from a
//! coarse (prefix/saturating) embed.
//!
//! ## Crate laws
//!
//! - Pure: no I/O, no clocks, no locks, no thread-locals, no env, no
//!   statics, no dependencies. Allocation-free except the caller's buffers.
//! - Golden-pinned: the hash vectors, transform goldens, and bloom policy
//!   constants are pinned in this crate's own tests AND re-exercised through
//!   `pgrc2_meta`'s pin suite. Bloom/NDV bytes are part bytes — a value
//!   drift here is a builder-version event, never a silent edit.

pub mod bloom;
pub mod hash;
pub mod transform;
