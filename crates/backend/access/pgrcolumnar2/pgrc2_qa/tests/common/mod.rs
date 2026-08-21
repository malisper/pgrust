//! Shared scale selection for the batteries. `PGRC2_QA_SCALE=full` is the
//! CI cluster tier (the chartered counts); anything else — including unset — is
//! the laptop smoke tier (Michael's rule: tiny-scale locally before any
//! CI cluster submission).

#[allow(dead_code)]
pub struct Scale {
    /// Adversarial persistence seeds explored per crash boundary.
    pub simvfs_seeds: u64,
    /// Mutations per corpus part in the read-fuzzer.
    pub fuzz_per_part: u64,
    /// kill -9 cycles in the real-process ladder.
    pub ladder_kills: u64,
}

#[allow(dead_code)]
pub fn scale() -> Scale {
    match std::env::var("PGRC2_QA_SCALE").as_deref() {
        Ok("full") => Scale {
            simvfs_seeds: 24,
            fuzz_per_part: 20_000,
            ladder_kills: 300,
        },
        _ => Scale {
            simvfs_seeds: 2,
            fuzz_per_part: 250,
            ladder_kills: 6,
        },
    }
}
