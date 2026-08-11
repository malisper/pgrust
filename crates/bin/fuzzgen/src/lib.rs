//! fuzzgen: seeded SQL generator for the coverage-differential fuzzer
//! (charter: docs/design/coverage-differential-fuzzer.md).
//!
//! A session is (seed, toggle vector, statement budget). All randomness
//! flows from one seeded deterministic PRNG — no OS entropy anywhere — so
//! the same seed and toggle vector reproduce a byte-identical SQL stream.
//! The seed is the reproducibility witness and appears in every output
//! mode. Each statement carries production-level self-coverage metadata
//! (which grammar productions fired), the generator-side half of the
//! coverage-gap loop.

pub mod catalog;
pub mod expr;
pub mod render;
pub mod rng;
pub mod session;
pub mod toggles;
