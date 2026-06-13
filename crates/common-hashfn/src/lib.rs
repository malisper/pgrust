const INITVAL: u32 = 0x9e37_79b9;
const POSTGRES_HASH_SALT: u32 = 3_923_095;

#[derive(Clone, Copy)]
struct HashState {
    a: u32,
    b: u32,
    c: u32,
}

impl HashState {
    fn new(len: usize) -> Self {
        assert!(
            len <= i32::MAX as usize,
            "PostgreSQL hash key length is an int"
        );

        let value = INITVAL
            .wrapping_add(len as u32)
            .wrapping_add(POSTGRES_HASH_SALT);
        Self {
            a: value,
            b: value,
            c: value,
        }
    }

    fn seed(&mut self, seed: u64) {
        if seed == 0 {
            return;
        }

        self.a = self.a.wrapping_add((seed >> 32) as u32);
        self.b = self.b.wrapping_add(seed as u32);
        self.mix();
    }

    fn mix(&mut self) {
        self.a = self.a.wrapping_sub(self.c);
        self.a ^= self.c.rotate_left(4);
        self.c = self.c.wrapping_add(self.b);
        self.b = self.b.wrapping_sub(self.a);
        self.b ^= self.a.rotate_left(6);
        self.a = self.a.wrapping_add(self.c);
        self.c = self.c.wrapping_sub(self.b);
        self.c ^= self.b.rotate_left(8);
        self.b = self.b.wrapping_add(self.a);
        self.a = self.a.wrapping_sub(self.c);
        self.a ^= self.c.rotate_left(16);
        self.c = self.c.wrapping_add(self.b);
        self.b = self.b.wrapping_sub(self.a);
        self.b ^= self.a.rotate_left(19);
        self.a = self.a.wrapping_add(self.c);
        self.c = self.c.wrapping_sub(self.b);
        self.c ^= self.b.rotate_left(4);
        self.b = self.b.wrapping_add(self.a);
    }

    fn final_mix(&mut self) {
        self.c ^= self.b;
        self.c = self.c.wrapping_sub(self.b.rotate_left(14));
        self.a ^= self.c;
        self.a = self.a.wrapping_sub(self.c.rotate_left(11));
        self.b ^= self.a;
        self.b = self.b.wrapping_sub(self.a.rotate_left(25));
        self.c ^= self.b;
        self.c = self.c.wrapping_sub(self.b.rotate_left(16));
        self.a ^= self.c;
        self.a = self.a.wrapping_sub(self.c.rotate_left(4));
        self.b ^= self.a;
        self.b = self.b.wrapping_sub(self.a.rotate_left(14));
        self.c ^= self.b;
        self.c = self.c.wrapping_sub(self.b.rotate_left(24));
    }

    fn finish32(mut self) -> u32 {
        self.final_mix();
        self.c
    }

    fn finish64(mut self) -> u64 {
        self.final_mix();
        ((self.b as u64) << 32) | self.c as u64
    }
}

/// Hashes a byte slice using PostgreSQL's Bob Jenkins hash.
pub fn hash_bytes(bytes: &[u8]) -> u32 {
    hash_bytes_state(bytes).finish32()
}

/// Hashes a byte slice using PostgreSQL's Bob Jenkins hash with an optional seed.
pub fn hash_bytes_extended(bytes: &[u8], seed: u64) -> u64 {
    let mut state = HashState::new(bytes.len());
    state.seed(seed);
    hash_bytes_into_state(state, bytes).finish64()
}

/// Hashes a 32-bit value using PostgreSQL's optimized integer hash path.
pub fn hash_bytes_uint32(value: u32) -> u32 {
    let mut state = HashState::new(size_of::<u32>());
    state.a = state.a.wrapping_add(value);
    state.finish32()
}

/// Hashes a 32-bit value using PostgreSQL's optimized integer hash path with an optional seed.
pub fn hash_bytes_uint32_extended(value: u32, seed: u64) -> u64 {
    let mut state = HashState::new(size_of::<u32>());
    state.seed(seed);
    state.a = state.a.wrapping_add(value);
    state.finish64()
}

/// Hashes a NUL-terminated string key as PostgreSQL's `string_hash` does.
///
/// At most `keysize - 1` bytes are hashed, matching dynahash's fixed-size key
/// truncation rule. If `keysize` is zero, the unsigned C subtraction wraps and
/// the full C string length is considered.
pub fn string_hash(key: &[u8], keysize: usize) -> u32 {
    let strlen = key.iter().position(|&byte| byte == 0).unwrap_or(key.len());
    let limit = keysize.wrapping_sub(1);
    let len = strlen.min(limit);
    hash_bytes(&key[..len])
}

/// Hashes exactly `keysize` bytes from a fixed-size tag key.
pub fn tag_hash(key: &[u8], keysize: usize) -> u32 {
    assert!(key.len() >= keysize, "tag_hash key shorter than keysize");
    hash_bytes(&key[..keysize])
}

/// Hashes a uint32 key as PostgreSQL's `uint32_hash` does.
pub fn uint32_hash(key: u32) -> u32 {
    hash_bytes_uint32(key)
}

pub fn rotate_high_and_low_32bits(value: u64) -> u64 {
    ((value << 1) & 0xffff_fffe_ffff_fffe) | ((value >> 31) & 0x0000_0001_0000_0001)
}

pub fn hash_combine(mut a: u32, b: u32) -> u32 {
    a ^= b
        .wrapping_add(INITVAL)
        .wrapping_add(a << 6)
        .wrapping_add(a >> 2);
    a
}

pub fn hash_combine64(mut a: u64, b: u64) -> u64 {
    a ^= b
        .wrapping_add(0x49a0_f4dd_15e5_a8e3)
        .wrapping_add(a << 54)
        .wrapping_add(a >> 7);
    a
}

pub fn murmurhash32(data: u32) -> u32 {
    let mut h = data;

    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h
}

pub fn murmurhash64(data: u64) -> u64 {
    let mut h = data;

    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^= h >> 33;
    h
}

fn hash_bytes_state(bytes: &[u8]) -> HashState {
    hash_bytes_into_state(HashState::new(bytes.len()), bytes)
}

fn hash_bytes_into_state(mut state: HashState, mut bytes: &[u8]) -> HashState {
    while bytes.len() >= 12 {
        state.a = state.a.wrapping_add(read_u32(&bytes[0..4]));
        state.b = state.b.wrapping_add(read_u32(&bytes[4..8]));
        state.c = state.c.wrapping_add(read_u32(&bytes[8..12]));
        state.mix();
        bytes = &bytes[12..];
    }

    state.a = state
        .a
        .wrapping_add(read_tail_word(&bytes[..bytes.len().min(4)]));

    if bytes.len() > 4 {
        state.b = state
            .b
            .wrapping_add(read_tail_word(&bytes[4..bytes.len().min(8)]));
    }

    if bytes.len() > 8 {
        state.c = state.c.wrapping_add(read_tail_word_c(&bytes[8..]));
    }

    state
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_ne_bytes(bytes.try_into().expect("u32 chunk has four bytes"))
}

/// Reads a partial trailing word for the `a` or `b` accumulators.
///
/// Matches the little-endian tail switch in hashfn.c (cases 1-7): the first
/// available byte goes at shift 0, the next at shift 8, then shift 16, then
/// shift 24 for a full word. There is no reserved low byte here.
fn read_tail_word(bytes: &[u8]) -> u32 {
    let mut word = [0; size_of::<u32>()];
    word[..bytes.len()].copy_from_slice(bytes);
    u32::from_ne_bytes(word)
}

/// Reads a partial trailing word for the `c` accumulator.
///
/// In hashfn.c's little-endian tail switch the lowest byte of `c` is reserved
/// for the length (comment at hashfn.c:228/467), so case 9 places k[8] at
/// shift 8, case 10 places k[9] at shift 16, and case 11 places k[10] at shift
/// 24. Only 1-3 trailing bytes ever reach this word, so the high byte stays in
/// `c`'s low byte (the reserved length slot).
fn read_tail_word_c(bytes: &[u8]) -> u32 {
    let mut word = 0u32;
    for (index, &byte) in bytes.iter().enumerate() {
        word += (byte as u32) << (8 * (index + 1));
    }
    word
}

/// Installs every seam declared in `common-hashfn-seams` to this crate's real
/// implementations.
pub fn init_seams() {
    common_hashfn_seams::hash_bytes_uint32::set(hash_bytes_uint32);
    common_hashfn_seams::hash_bytes_uint32_extended::set(hash_bytes_uint32_extended);
    common_hashfn_seams::tag_hash::set(tag_hash);
    common_hashfn_seams::string_hash::set(string_hash);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_vectors_match_postgres_hashfn() {
        // Oracle values produced by compiling the little-endian branch of
        // postgres-18.3/src/common/hashfn.c (lines 145-361 / 371-600) as a
        // standalone reference and running it on a little-endian host.
        assert_eq!(hash_bytes(b""), 0xa7ea_466d);
        assert_eq!(hash_bytes(b"a"), 0x4013_70b1);
        assert_eq!(hash_bytes(b"abc"), 0xd12f_eb97);
        // "PostgreSQL" has length 10, so it exercises the case-10 `c` tail word
        // whose low byte is reserved for the length. The previous oracle
        // (0x6800_d906) enshrined the pre-fix bug where the `c` tail bytes were
        // packed 8 bits too low.
        assert_eq!(hash_bytes(b"PostgreSQL"), 0x9ae1_fe84);
        assert_eq!(hash_bytes(b"abcdefghijklmnopqrstuvwxyz"), 0x0a00_e7bb);
        assert_eq!(
            hash_bytes_extended(b"PostgreSQL", 0x0123_4567_89ab_cdef),
            0x271d_3a06_c807_e270
        );
    }

    #[test]
    fn tail_lengths_nine_ten_eleven_reserve_c_low_byte() {
        // Tail lengths 9/10/11 are exactly the cases where hashfn.c places the
        // trailing bytes into the `c` accumulator at shifts 8/16/24 (the low
        // byte of `c` is reserved for the length). Oracle values are from the
        // standalone little-endian reference build of hashfn.c.
        assert_eq!(hash_bytes(b"123456789"), 0x3c73_47a8); // len 9
        assert_eq!(hash_bytes(b"1234567890"), 0xe9c1_ee42); // len 10
        assert_eq!(hash_bytes(b"12345678901"), 0x6259_ed3a); // len 11

        // The extended hash with seed 0 must agree with the 32-bit hash in its
        // low word for these same tail lengths.
        assert_eq!(hash_bytes_extended(b"123456789", 0) as u32, 0x3c73_47a8);
        assert_eq!(hash_bytes_extended(b"1234567890", 0) as u32, 0xe9c1_ee42);
        assert_eq!(hash_bytes_extended(b"12345678901", 0) as u32, 0x6259_ed3a);
    }

    #[test]
    fn integer_hashes_match_byte_hashes_on_native_endian() {
        for value in [0, 1, 42, 0x0102_0304, u32::MAX] {
            assert_eq!(hash_bytes_uint32(value), hash_bytes(&value.to_ne_bytes()));
            assert_eq!(
                hash_bytes_uint32_extended(value, 0) as u32,
                hash_bytes_uint32(value)
            );
        }
    }

    #[test]
    fn extended_hash_with_zero_seed_keeps_32_bit_result_in_low_bits() {
        for bytes in [
            b"".as_slice(),
            b"abc",
            b"123456789",   // tail len 9  (c low byte reserved)
            b"1234567890",  // tail len 10 (c low byte reserved)
            b"12345678901", // tail len 11 (c low byte reserved)
            b"0123456789abcdef",
        ] {
            assert_eq!(hash_bytes_extended(bytes, 0) as u32, hash_bytes(bytes));
        }
    }

    #[test]
    fn string_hash_stops_at_nul_and_respects_keysize_minus_one() {
        assert_eq!(string_hash(b"abc\0def", 16), hash_bytes(b"abc"));
        assert_eq!(string_hash(b"abcdef", 4), hash_bytes(b"abc"));
        assert_eq!(string_hash(b"abcdef", 0), hash_bytes(b"abcdef"));
    }

    #[test]
    fn tag_and_uint32_wrappers_match_core_hashes() {
        assert_eq!(tag_hash(b"abcdef", 4), hash_bytes(b"abcd"));
        assert_eq!(uint32_hash(12345), hash_bytes_uint32(12345));
    }

    #[test]
    fn inline_header_helpers_match_expected_values() {
        assert_eq!(
            rotate_high_and_low_32bits(0x8000_0001_0000_0001),
            0x0000_0003_0000_0002
        );
        assert_eq!(hash_combine(0x1234_5678, 0x9abc_def0), 0xd8a3_5a3f);
        assert_eq!(
            hash_combine64(0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210),
            0xc51c_b367_d2e6_ff61
        );
        assert_eq!(murmurhash32(0x1234_5678), 0xe37c_d1bc);
        assert_eq!(murmurhash64(0x0123_4567_89ab_cdef), 0x87cb_fbfe_8902_2cea);
    }
}
