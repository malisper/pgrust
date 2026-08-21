// PROVENANCE (O-1 vendoring, lane M3-B): vendored VERBATIM from
// origin/appbench-types @ 4000b2794c773909aa985d87e4d38ee842cffaab — donor tests carried;
// no semantic edits during the move (adaptations are separate commits).

//! ALP float compression (Afroozeh & Boncz, SIGMOD 2024; reference
//! implementation github.com/cwida/ALP), standalone.
//!
//! This is the appbench-types Milestone A4 encoder core: pgrcolumnar
//! integration happens later on the v9 substream directory, so this crate
//! deliberately depends on nothing and nothing depends on it yet.
//!
//! Shape: input is processed in 102400-value rowgroups of 1024-value
//! vectors (a short tail at either level is handled). Per rowgroup,
//! two-level sampling elects a scheme and up to five (e,f) candidates;
//! per vector, ALP classic encodes d = fast_round(n * 10^e * 10^-f) and
//! keeps only values that verify n == d * 10^f * 10^-e — verified as BIT
//! equality, so every NaN payload, -0.0 and out-of-domain value is stored
//! verbatim as an exception. ALP-RD covers rowgroups of "real" doubles via
//! a left-bits dictionary at a cut position >= 48. A rowgroup whose exact
//! encoded size would not beat raw bit images is stored [`Scheme::Raw`].
//!
//! Contracts the tests pin:
//! - decode(encode(x)) reproduces x bit-for-bit (f64::to_bits equality)
//!   for EVERY input, including NaN payloads, +/-0.0, infinities,
//!   denormals and the 2^51 fast-round domain boundary;
//! - encoding is a pure function of the input slice (deterministic
//!   sampling: fixed strides, ordered vote/dictionary maps);
//! - [`Encoded::size_bytes`] is exact per the documented per-vector
//!   formulas, so callers can run encoding elections on it.
//!
//! The writer-facing surface for the pgrcolumnar A4 integration lives in
//! [`granule`]: the same kernels reframed into 8192-value granules (8
//! vectors) as independent self-describing frames, plus an exact-size
//! [`granule::ElectionReport`] for the chunk writer's >=10%-win election
//! against its RawF arm. That module's frame layout is the serialized
//! form of record.
//!
//! [`granule32`] is the SB-5 f32 completeness arm: the same granule
//! surface at the 4-byte width (classic + raw arms, no RD; i32 encoded
//! domain, MAX_EXPONENT 10; transforms evaluate in f64 with ONE rounding
//! to f32 — `classic32.rs` module doc records why pure-f32 evaluation
//! cannot reach a near-zero exception rate), with the identical
//! bit-exactness contract — decode(encode(x)) reproduces x by
//! `f32::to_bits` equality for EVERY input.

#![forbid(unsafe_code)]
// Kernel loops use index form on fixed-size local arrays: the
// autovectorizable shape is the point (see bitpack.rs).
#![allow(clippy::needless_range_loop)]

pub mod bitpack;
mod classic;
mod classic32;
mod constants;
pub mod granule;
pub mod granule32;
mod rd;

pub use classic::AlpVector;
pub use classic32::AlpF32Vector;
pub use constants::{ROWGROUP_SIZE, VECTOR_SIZE};
pub use rd::{RdDictionary, RdVector};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scheme {
    Alp,
    AlpRd,
    Raw,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowGroup {
    Alp {
        vectors: Vec<AlpVector>,
    },
    AlpRd {
        dict: RdDictionary,
        vectors: Vec<RdVector>,
    },
    /// Verbatim f64 bit images; elected when neither scheme's exact size
    /// beats 8 bytes/value.
    Raw {
        values: Vec<u64>,
    },
}

impl RowGroup {
    pub fn scheme(&self) -> Scheme {
        match self {
            RowGroup::Alp { .. } => Scheme::Alp,
            RowGroup::AlpRd { .. } => Scheme::AlpRd,
            RowGroup::Raw { .. } => Scheme::Raw,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RowGroup::Alp { vectors } => vectors.iter().map(|v| v.len as usize).sum(),
            RowGroup::AlpRd { vectors, .. } => vectors.iter().map(|v| v.len as usize).sum(),
            RowGroup::Raw { values } => values.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_bytes(&self) -> usize {
        match self {
            RowGroup::Alp { vectors } => vectors.iter().map(|v| v.size_bytes()).sum(),
            RowGroup::AlpRd { dict, vectors } => {
                dict.size_bytes() + vectors.iter().map(|v| v.size_bytes()).sum::<usize>()
            }
            RowGroup::Raw { values } => values.len() * 8,
        }
    }

    fn exceptions(&self) -> usize {
        match self {
            RowGroup::Alp { vectors } => vectors.iter().map(|v| v.exc_positions.len()).sum(),
            RowGroup::AlpRd { vectors, .. } => {
                vectors.iter().map(|v| v.exc_positions.len()).sum()
            }
            RowGroup::Raw { .. } => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Encoded {
    pub rowgroups: Vec<RowGroup>,
    total_len: usize,
}

impl Encoded {
    pub fn len(&self) -> usize {
        self.total_len
    }

    pub fn is_empty(&self) -> bool {
        self.total_len == 0
    }

    /// Exact payload accounting (see the per-vector size_bytes formulas).
    pub fn size_bytes(&self) -> usize {
        self.rowgroups.iter().map(|rg| rg.size_bytes()).sum()
    }
}

/// Election summary for a would-be encoding; derived from [`encode`] so it
/// can never drift from what encoding actually produces.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Analysis {
    pub total_values: usize,
    pub raw_bytes: usize,
    pub encoded_bytes: usize,
    pub alp_rowgroups: usize,
    pub alp_rd_rowgroups: usize,
    pub raw_rowgroups: usize,
    pub exceptions: usize,
}

impl Analysis {
    pub fn of(encoded: &Encoded) -> Analysis {
        let mut a = Analysis {
            total_values: encoded.len(),
            raw_bytes: encoded.len() * 8,
            encoded_bytes: encoded.size_bytes(),
            alp_rowgroups: 0,
            alp_rd_rowgroups: 0,
            raw_rowgroups: 0,
            exceptions: 0,
        };
        for rg in &encoded.rowgroups {
            match rg.scheme() {
                Scheme::Alp => a.alp_rowgroups += 1,
                Scheme::AlpRd => a.alp_rd_rowgroups += 1,
                Scheme::Raw => a.raw_rowgroups += 1,
            }
            a.exceptions += rg.exceptions();
        }
        a
    }
}

pub fn analyze(values: &[f64]) -> Analysis {
    Analysis::of(&encode(values))
}

pub fn encode(values: &[f64]) -> Encoded {
    Encoded {
        rowgroups: values.chunks(constants::ROWGROUP_SIZE).map(encode_rowgroup).collect(),
        total_len: values.len(),
    }
}

pub fn decode(encoded: &Encoded) -> Vec<f64> {
    let mut out = Vec::with_capacity(encoded.len());
    decode_into(encoded, &mut out);
    out
}

pub fn decode_into(encoded: &Encoded, out: &mut Vec<f64>) {
    for rg in &encoded.rowgroups {
        match rg {
            RowGroup::Alp { vectors } => {
                for v in vectors {
                    classic::decode_vector(v, out);
                }
            }
            RowGroup::AlpRd { dict, vectors } => {
                for v in vectors {
                    rd::decode_rd_vector(v, dict, out);
                }
            }
            RowGroup::Raw { values } => {
                out.extend(values.iter().map(|&b| f64::from_bits(b)));
            }
        }
    }
}

/// First-stage sample: vectors at stride ROWGROUP_SAMPLES_JUMP, 32
/// equidistant values each. An incomplete vector is skipped unless it is
/// the first sample (tiny inputs must still get sampled).
pub(crate) fn first_stage_sample(values: &[f64]) -> Vec<Vec<f64>> {
    let mut out: Vec<Vec<f64>> = Vec::new();
    for (idx, chunk) in values.chunks(constants::VECTOR_SIZE).enumerate() {
        if idx % constants::ROWGROUP_SAMPLES_JUMP != 0 {
            continue;
        }
        if chunk.len() < constants::VECTOR_SIZE && !out.is_empty() {
            continue;
        }
        let inc = chunk.len().div_ceil(constants::SAMPLES_PER_VECTOR).max(1);
        out.push(chunk.iter().step_by(inc).copied().collect());
    }
    out
}

fn encode_rowgroup(values: &[f64]) -> RowGroup {
    debug_assert!(!values.is_empty() && values.len() <= constants::ROWGROUP_SIZE);
    let sampled = first_stage_sample(values);
    let selection = classic::find_top_k_combinations(&sampled);
    let candidate = if selection.use_rd {
        let flat: Vec<f64> = sampled.into_iter().flatten().collect();
        let dict = rd::find_best_dictionary(&flat);
        let vectors = values
            .chunks(constants::VECTOR_SIZE)
            .map(|c| rd::encode_rd_vector(c, &dict))
            .collect();
        RowGroup::AlpRd { dict, vectors }
    } else {
        let vectors = values
            .chunks(constants::VECTOR_SIZE)
            .map(|c| {
                let (e, f) = classic::choose_ef(&selection.combinations, c);
                classic::encode_vector(c, e, f)
            })
            .collect();
        RowGroup::Alp { vectors }
    };
    // Exact-size election: an encoding that cannot beat raw bit images is
    // never stored, so size_bytes() <= 8 * len holds per rowgroup.
    if candidate.size_bytes() >= values.len() * 8 {
        RowGroup::Raw {
            values: values.iter().map(|v| v.to_bits()).collect(),
        }
    } else {
        candidate
    }
}
