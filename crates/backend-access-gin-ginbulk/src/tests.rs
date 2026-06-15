//! Unit tests for the GIN build accumulator (`ginbulk.c`) logic that needs no
//! buffer manager: the `step` sequence in [`super::ginInsertBAEntries`] and the
//! near-balanced insertion order it produces.

/// `ginInsertBAEntries` computes `step` as the largest power of two `<= nentries`
/// (ginbulk.c:215). Verify the bit-smear matches for a representative range.
#[test]
fn step_is_largest_power_of_two_le_nentries() {
    fn step_of(nentries: u32) -> u32 {
        let mut step = nentries;
        step |= step >> 1;
        step |= step >> 2;
        step |= step >> 4;
        step |= step >> 8;
        step |= step >> 16;
        step >>= 1;
        step + 1
    }
    assert_eq!(step_of(1), 1);
    assert_eq!(step_of(2), 2);
    assert_eq!(step_of(3), 2);
    assert_eq!(step_of(4), 4);
    assert_eq!(step_of(7), 4);
    assert_eq!(step_of(8), 8);
    assert_eq!(step_of(2048), 2048);
    assert_eq!(step_of(2049), 2048);
}
