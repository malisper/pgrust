//! uuid_abbrev_convert/uuid_abbrev_abort.

use crate::PgUuid;
use hyperloglog::HyperLogLog;

pub struct UuidAbbrevState {
    input_count: i64,
    estimating: bool,
    abbr_card: HyperLogLog,
}

impl UuidAbbrevState {
    pub fn new() -> UuidAbbrevState {
        UuidAbbrevState {
            input_count: 0,
            estimating: true,
            abbr_card: HyperLogLog::new(10),
        }
    }

    /// `uuid_abbrev_convert`; returns the native-endian abbreviated key word.
    pub fn convert(&mut self, uuid: &PgUuid) -> u64 {
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&uuid[..8]);
        self.input_count += 1;
        if self.estimating {
            let res = u64::from_ne_bytes(prefix);
            self.abbr_card
                .add(hashfn::hash_bytes_uint32(res as u32 ^ (res >> 32) as u32));
        }
        u64::from_be_bytes(prefix)
    }

    /// `uuid_abbrev_abort`: commit once past 100k distinct abbrevs, abort
    /// below 1 distinct per ~2k non-null inputs.
    pub fn abort(&mut self, memtupcount: i32) -> bool {
        if memtupcount < 10000 || self.input_count < 10000 || !self.estimating {
            return false;
        }
        let card = self.abbr_card.estimate();
        if card > 100000.0 {
            self.estimating = false;
            return false;
        }
        card < self.input_count as f64 / 2000.0 + 0.5
    }
}

impl Default for UuidAbbrevState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abbrev_orders_like_uuid_internal_cmp() {
        let mut st = UuidAbbrevState::new();
        let mut x: u64 = 42;
        let mut uuids: Vec<PgUuid> = Vec::new();
        for i in 0..200u64 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let mut u = [0u8; 16];
            u[..8].copy_from_slice(&(x >> (i % 5)).to_be_bytes());
            u[8..].copy_from_slice(&x.to_le_bytes());
            uuids.push(u);
        }
        let abbrevs: Vec<u64> = uuids.iter().map(|u| st.convert(u)).collect();
        for i in 0..uuids.len() {
            for j in 0..uuids.len() {
                let a = (abbrevs[i] > abbrevs[j]) as i32 - (abbrevs[i] < abbrevs[j]) as i32;
                if a != 0 {
                    assert_eq!(a, crate::uuid_internal_cmp(&uuids[i], &uuids[j]).signum());
                }
            }
        }
    }

    #[test]
    fn abort_gates() {
        let mut st = UuidAbbrevState::new();
        let u = [1u8; 16];
        st.convert(&u);
        assert!(!st.abort(9999));

        // 20k copies of one value: cardinality 1 < 20000/2000 + 0.5.
        let mut st = UuidAbbrevState::new();
        for _ in 0..20000 {
            st.convert(&u);
        }
        assert!(st.abort(20000));
    }
}
