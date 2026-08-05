//! network_abbrev_convert/network_abbrev_abort, SIZEOF_DATUM==8 arm.

use crate::{InetRef, PGSQL_AF_INET, PGSQL_AF_INET6};
use hyperloglog::HyperLogLog;

const ABBREV_BITS_INET4_NETMASK_SIZE: u32 = 6;
const ABBREV_BITS_INET4_SUBNET: u32 = 25;

pub struct NetworkAbbrevState {
    input_count: i64,
    estimating: bool,
    abbr_card: HyperLogLog,
}

impl NetworkAbbrevState {
    pub fn new() -> NetworkAbbrevState {
        NetworkAbbrevState {
            input_count: 0,
            estimating: true,
            abbr_card: HyperLogLog::new(10),
        }
    }

    /// `network_abbrev_convert`: unsigned-comparable key of
    /// family bit | network bits | (IPv4: netmask size | subnet bits).
    pub fn convert(&mut self, ip: InetRef<'_>) -> u64 {
        debug_assert!(ip.family == PGSQL_AF_INET || ip.family == PGSQL_AF_INET6);
        let ipv4 = ip.family == PGSQL_AF_INET;

        let (ipaddr_datum, mut res) = if ipv4 {
            let v = u32::from_be_bytes(ip.addr[..4].try_into().unwrap());
            (v as u64, 0u64)
        } else {
            let v = u64::from_be_bytes(ip.addr[..8].try_into().unwrap());
            (v, 1u64 << 63)
        };

        let bits = ip.bits as u32;
        let subnet_size = (ip.maxbits() as u32 - bits) % 64;
        let (subnet_bitmask, network) = if bits == 0 {
            (u64::MAX, 0)
        } else if bits < 64 {
            let mask = (1u64 << subnet_size) - 1;
            (mask, ipaddr_datum & !mask)
        } else {
            (0, ipaddr_datum)
        };

        if ipv4 {
            let netmask_size = (bits as u64) << ABBREV_BITS_INET4_SUBNET;
            let mut subnet = ipaddr_datum & subnet_bitmask;
            if subnet_size > ABBREV_BITS_INET4_SUBNET {
                subnet >>= subnet_size - ABBREV_BITS_INET4_SUBNET;
            }
            let network = network << (ABBREV_BITS_INET4_NETMASK_SIZE + ABBREV_BITS_INET4_SUBNET);
            res |= network | netmask_size | subnet;
        } else {
            res |= network >> 1;
        }

        self.input_count += 1;
        if self.estimating {
            self.abbr_card
                .add(hashfn::hash_bytes_uint32(res as u32 ^ (res >> 32) as u32));
        }
        res
    }

    /// `network_abbrev_abort`: commit once past 100k distinct abbrevs, abort
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

impl Default for NetworkAbbrevState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_cmp_internal;

    fn inet4(addr: [u8; 4], bits: u8) -> Vec<u8> {
        let mut v = vec![PGSQL_AF_INET, bits];
        v.extend_from_slice(&addr);
        v
    }

    fn inet6(addr: [u8; 16], bits: u8) -> Vec<u8> {
        let mut v = vec![PGSQL_AF_INET6, bits];
        v.extend_from_slice(&addr);
        v
    }

    #[test]
    fn abbrev_orders_like_network_cmp_internal() {
        let mut st = NetworkAbbrevState::new();
        let mut x: u64 = 3;
        let mut next = |limit: u64| {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (x >> 33) % limit
        };
        let mut vals: Vec<Vec<u8>> = Vec::new();
        for _ in 0..300 {
            if next(2) == 0 {
                let mut a = [0u8; 4];
                for b in &mut a {
                    *b = next(4) as u8 * 85;
                }
                vals.push(inet4(a, next(33) as u8));
            } else {
                let mut a = [0u8; 16];
                for b in &mut a {
                    *b = next(3) as u8 * 127;
                }
                vals.push(inet6(a, next(129) as u8));
            }
        }
        // C parity: cmp inputs are the netmasked network()/cidr forms only in
        // SQL paths; the sort comparator sees raw inet values as stored.
        let abbrevs: Vec<u64> = vals
            .iter()
            .map(|v| st.convert(crate::InetRef::from_payload(v)))
            .collect();
        for i in 0..vals.len() {
            for j in 0..vals.len() {
                let a = (abbrevs[i] > abbrevs[j]) as i32 - (abbrevs[i] < abbrevs[j]) as i32;
                if a != 0 {
                    let full = network_cmp_internal(
                        crate::InetRef::from_payload(&vals[i]),
                        crate::InetRef::from_payload(&vals[j]),
                    )
                    .signum();
                    assert_eq!(a, full, "{:?} vs {:?}", vals[i], vals[j]);
                }
            }
        }
    }

    #[test]
    fn family_bit_dominates() {
        let mut st = NetworkAbbrevState::new();
        let v4 = inet4([255, 255, 255, 255], 32);
        let v6 = inet6([0u8; 16], 0);
        assert!(
            st.convert(crate::InetRef::from_payload(&v4))
                < st.convert(crate::InetRef::from_payload(&v6))
        );
    }
}
