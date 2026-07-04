//! varstr_abbrev_convert/varstr_abbrev_abort, collate-C arm only (the strxfrm
//! locale arm lands with collation support; resolution never arms it today).

use hyperloglog::HyperLogLog;

const PG_CACHE_LINE_SIZE: usize = 128;

pub struct VarStrAbbrevState {
    is_bpchar: bool,
    prop_card: f64,
    abbr_card: HyperLogLog,
    full_card: HyperLogLog,
}

#[inline]
fn bpchar_truelen(s: &[u8]) -> &[u8] {
    &s[..s.len() - s.iter().rev().take_while(|&&b| b == b' ').count()]
}

impl VarStrAbbrevState {
    pub fn new(is_bpchar: bool) -> VarStrAbbrevState {
        VarStrAbbrevState {
            is_bpchar,
            prop_card: 0.20,
            abbr_card: HyperLogLog::new(10),
            full_card: HyperLogLog::new(10),
        }
    }

    /// bpchar-aware trailing-space trim (bpchartruelen arm of the C convert).
    pub fn trimmed<'a>(&self, payload: &'a [u8]) -> &'a [u8] {
        if self.is_bpchar {
            bpchar_truelen(payload)
        } else {
            payload
        }
    }

    /// The HyperLogLog bookkeeping of `varstr_abbrev_convert`; `res` is the
    /// pre-byteswap prefix word (C hashes it before DatumBigEndianToNative).
    pub fn record(&mut self, data: &[u8], res: u64) {
        let len = data.len();
        let mut hash = hashfn::hash_bytes(&data[..len.min(PG_CACHE_LINE_SIZE)]);
        if len > PG_CACHE_LINE_SIZE {
            hash ^= hashfn::hash_bytes_uint32(len as u32);
        }
        self.full_card.add(hash);

        let hash = hashfn::hash_bytes_uint32(res as u32 ^ (res >> 32) as u32);
        self.abbr_card.add(hash);
    }

    /// `varstr_abbrev_convert`; `payload` is the detoasted varlena payload.
    /// Returns the native-endian abbreviated key word.
    pub fn convert(&mut self, payload: &[u8]) -> u64 {
        let data = self.trimmed(payload);

        let mut prefix = [0u8; 8];
        let n = data.len().min(8);
        prefix[..n].copy_from_slice(&data[..n]);
        // C's pre-byteswap Datum image: prefix bytes in memory order.
        self.record(data, u64::from_ne_bytes(prefix));

        // DatumBigEndianToNative: unsigned word compare == memcmp of prefix.
        u64::from_be_bytes(prefix)
    }

    /// `varstr_abbrev_abort`.
    pub fn abort(&mut self, memtupcount: i32) -> bool {
        if memtupcount < 100 {
            return false;
        }
        let abbrev_distinct = self.abbr_card.estimate().max(1.0);
        let key_distinct = self.full_card.estimate().max(1.0);

        if abbrev_distinct > key_distinct * self.prop_card {
            if memtupcount > 10000 {
                self.prop_card *= 0.65;
            }
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmp_u64(a: u64, b: u64) -> i32 {
        (a > b) as i32 - (a < b) as i32
    }

    #[test]
    fn abbrev_orders_like_varstrfastcmp_c() {
        let mut st = VarStrAbbrevState::new(false);
        let mut x: u64 = 7;
        let mut next = |limit: u64| {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (x >> 33) % limit
        };
        let mut vals: Vec<Vec<u8>> = Vec::new();
        for _ in 0..500 {
            let len = next(20) as usize;
            let mut v = Vec::with_capacity(len);
            for _ in 0..len {
                v.push(match next(6) {
                    0 => 0u8,
                    1 => b' ',
                    2 => 0xff,
                    n => b'a' + n as u8,
                });
            }
            vals.push(v);
        }
        let abbrevs: Vec<u64> = vals.iter().map(|v| st.convert(v)).collect();
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = cmp_u64(abbrevs[i], abbrevs[j]);
                if a != 0 {
                    assert_eq!(
                        a,
                        crate::varstrfastcmp_c(&vals[i], &vals[j]).signum(),
                        "{:?} vs {:?}",
                        vals[i],
                        vals[j]
                    );
                }
            }
        }
    }

    #[test]
    fn bpchar_abbrev_trims_trailing_spaces() {
        let mut st = VarStrAbbrevState::new(true);
        assert_eq!(st.convert(b"ab   "), st.convert(b"ab"));
        assert!(st.convert(b"ab c") != st.convert(b"ab"));
    }

    #[test]
    fn abort_heuristics() {
        let mut st = VarStrAbbrevState::new(false);
        assert!(!st.abort(99));

        // Pathological: one abbrev bucket, many distinct long keys.
        let mut st = VarStrAbbrevState::new(false);
        for i in 0..20000u32 {
            let mut v = vec![b'z'; 8];
            v.extend_from_slice(&i.to_be_bytes());
            st.convert(&v);
        }
        assert!(st.abort(20000));

        // Healthy: distinct prefixes track distinct keys.
        let mut st = VarStrAbbrevState::new(false);
        for i in 0..20000u32 {
            st.convert(&i.to_be_bytes());
        }
        assert!(!st.abort(20000));
    }
}
