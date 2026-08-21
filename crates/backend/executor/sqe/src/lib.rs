//! sqe — the stencil query engine (production port of the PoC engine;
//! docs/design/sqe/production-plan.md §P1-1, docs/design/sqe/port-study/).
//!
//! Layer map (port-map.md §4 — module names kept from the reference tree
//! to minimize port churn; recorded deviation from the suggested renames):
//!
//! - `bank` / `scan` / `statsview` / `bankstats` / `flatface`: the read
//!   closure over the P1-0 pgrcolumnar2 crates (banks, cursors, stats
//!   faces).
//! - `pool`: the process-resident worker pool (claim law).
//! - `engine`: standing Faces per open relation, condition cache, build
//!   concurrency, SqeCtx/Engine handles.
//! - `ir` / `planner` / `exec`: physical IR (typed fingerprints, TypMeta
//!   currency), the single lowering pipeline (typed refusals), registry +
//!   condcache wiring.
//! - `answer` / `render`: the typed AnswerSet ABI and the ONE text seam.
//! - `stencils`: the parametric family bodies.
//! - `kernels*` / `fused` / `grouped` / `drivers` / `simd`: the harness
//!   library extractions the stencils call into.
//! - `rig` (feature "rig"): SQL front end, RON plans, the scalar oracle,
//!   and the bench driver — byte-identity instrumentation, never engine.

pub mod answer;
pub mod bank;
pub mod bankstats;
pub mod cancel;
pub mod coldledger;
pub mod condcache;
pub mod cost_params;
pub mod drivers;
pub mod engine;
pub mod exec;
pub mod face;
pub mod family;
pub mod flatface;
pub mod fold;
pub mod fp;
pub mod fused;
pub mod grouped;
pub mod horizon;
pub mod ir;
pub mod joins;
pub mod kernels;
pub mod kernels_dec;
pub mod kernels_f123;
pub mod kernels_f4f5;
pub mod kernels_f6;
pub mod kernels_g;
pub mod kernels_g2;
pub mod kernels_pred;
pub mod kernels_u;
pub mod like;
pub mod planner;
pub mod pool;
pub mod psmaface;
pub mod refuse;
pub mod render;
pub mod scan;
pub mod simd;
pub mod spill;
pub mod statsview;
pub mod stencils;
pub mod typmeta;
pub mod witness;

#[cfg(feature = "rig")]
pub mod rig;

#[cfg(feature = "tpchfloor")]
pub mod tpchfloor;
