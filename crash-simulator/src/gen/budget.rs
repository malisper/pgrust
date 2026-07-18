//! Budget steering (`Remaining`, contract §2.1.2): per-statement-kind budgets
//! from profile weights, decremented as steps are emitted (noise AND
//! property-embedded), so the configured distribution holds across both.

use crate::gen::profile::StatementWeights;
use crate::property::Footprint;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Ddl = 0,
    Dml = 1,
    Query = 2,
    Tx = 3,
    Arm = 4,
    Fault = 5,
    Property = 6,
}

pub const N_KINDS: usize = 7;

pub const ALL_KINDS: [Kind; N_KINDS] =
    [Kind::Ddl, Kind::Dml, Kind::Query, Kind::Tx, Kind::Arm, Kind::Fault, Kind::Property];

#[derive(Debug, Clone)]
pub struct Budgets {
    remaining: [u64; N_KINDS],
}

impl Budgets {
    /// Largest-remainder allocation of `total` steps across kinds by weight.
    /// Integer arithmetic only.
    pub fn allocate(weights: &StatementWeights, total: u64, properties_available: bool) -> Budgets {
        let mut w = [
            weights.ddl,
            weights.dml,
            weights.query,
            weights.tx,
            weights.arm,
            weights.fault,
            weights.property,
        ];
        if !properties_available {
            w[Kind::Property as usize] = 0;
        }
        let wsum: u64 = w.iter().sum();
        let mut remaining = [0u64; N_KINDS];
        if wsum == 0 {
            return Budgets { remaining };
        }
        let mut assigned = 0u64;
        // (remainder, index) for largest-remainder distribution; ties broken by
        // fixed kind order — fully deterministic.
        let mut rems: Vec<(u64, usize)> = Vec::with_capacity(N_KINDS);
        for (i, &wi) in w.iter().enumerate() {
            let exact_num = wi * total;
            remaining[i] = exact_num / wsum;
            assigned += remaining[i];
            rems.push((exact_num % wsum, i));
        }
        rems.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
        let mut leftover = total - assigned;
        for &(_, i) in &rems {
            if leftover == 0 {
                break;
            }
            if w[i] > 0 {
                remaining[i] += 1;
                leftover -= 1;
            }
        }
        Budgets { remaining }
    }

    pub fn remaining(&self, k: Kind) -> u64 {
        self.remaining[k as usize]
    }

    pub fn consume(&mut self, k: Kind, n: u64) {
        let r = &mut self.remaining[k as usize];
        *r = r.saturating_sub(n);
    }

    /// Consume a property's expected footprint (saturating per kind) plus one
    /// unit of the property budget itself.
    pub fn consume_footprint(&mut self, fp: &Footprint) {
        self.consume(Kind::Property, 1);
        self.consume(Kind::Ddl, fp.ddl as u64);
        self.consume(Kind::Dml, fp.dml as u64);
        self.consume(Kind::Query, fp.query as u64);
        self.consume(Kind::Tx, fp.tx as u64);
        self.consume(Kind::Arm, fp.arm as u64);
        self.consume(Kind::Fault, fp.fault as u64);
    }

    pub fn total_remaining(&self) -> u64 {
        self.remaining.iter().sum()
    }
}
