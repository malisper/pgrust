//! timingsafe_bcmp.c: constant-time compare; non-zero iff different (unequal lengths differ).
pub fn timingsafe_bcmp(b1: &[u8], b2: &[u8]) -> i32 {
    let mut ret: u8 = (b1.len() != b2.len()) as u8;
    for (p1, p2) in b1.iter().zip(b2.iter()) {
        ret |= p1 ^ p2;
    }
    (ret != 0) as i32
}

#[cfg(test)]
mod tests {
    use super::timingsafe_bcmp;

    #[test]
    fn matches_c_on_equal_length_inputs() {
        assert_eq!(timingsafe_bcmp(&[], &[]), 0);
        assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 3]), 0);
        assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2, 4]), 1);
        assert_eq!(timingsafe_bcmp(&[0, 2, 3], &[1, 2, 3]), 1);
        assert_eq!(timingsafe_bcmp(&[0xff, 0], &[0, 0xff]), 1);
        assert_eq!(timingsafe_bcmp(b"pencil", b"pencil"), 0);
        assert_eq!(timingsafe_bcmp(b"pencil", b"pencim"), 1);
    }

    #[test]
    fn unequal_lengths_differ() {
        assert_eq!(timingsafe_bcmp(&[1, 2, 3], &[1, 2]), 1);
        assert_eq!(timingsafe_bcmp(&[], &[0]), 1);
        assert_eq!(timingsafe_bcmp(b"abc", b"abcd"), 1);
    }
}
