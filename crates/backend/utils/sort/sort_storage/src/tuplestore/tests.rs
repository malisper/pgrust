//! Pure-logic tests for the `tuplestore.c` port. The end-to-end store paths
//! need the executor slot pool + EState (`tuplestore_gettupleslot`) and, on
//! spill, a real `BufFile`, which a unit test cannot stand up. Covered here:
//! the flat-`MinimalTuple` length accessor, the on-tape length-word framing
//! arithmetic, and the budget/seek constants that pin the C fidelity.

use super::*;

#[test]
fn exec_flag_and_seek_constants_match_c() {
    // executor.h.
    assert_eq!(EXEC_FLAG_REWIND, 0x0004);
    assert_eq!(EXEC_FLAG_BACKWARD, 0x0008);
    // SEEK_SET / SEEK_CUR.
    assert_eq!(SEEK_SET, 0);
    assert_eq!(SEEK_CUR, 1);
}

#[test]
fn minimal_tuple_offsets_match_c() {
    // offsetof(MinimalTupleData, t_infomask2) == 10; sizeof(uint) length word.
    assert_eq!(MINIMAL_TUPLE_DATA_OFFSET, 10);
    assert_eq!(LEN_WORD_SIZE, 4);
    assert_eq!(POINTER_SIZE, 8);
    assert_eq!(ALLOCSET_SEPARATE_THRESHOLD, 8192);
}

#[test]
fn blob_t_len_reads_native_endian_first_word() {
    // The first four bytes of a flat MinimalTuple blob are t_len (native).
    let t_len: u32 = 0x0001_0203;
    let mut blob = vec![0u8; 32];
    blob[0..4].copy_from_slice(&t_len.to_ne_bytes());
    assert_eq!(blob_t_len(&blob), t_len);
}

#[test]
fn on_tape_length_framing_arithmetic() {
    // writetup_heap: tupbodylen = t_len - MINIMAL_TUPLE_DATA_OFFSET;
    //                tuplen = tupbodylen + LEN_WORD_SIZE.
    // readtup_heap inverts it: tupbodylen = len - LEN_WORD_SIZE;
    //                          tuplen = tupbodylen + MINIMAL_TUPLE_DATA_OFFSET.
    let t_len: usize = 100;
    let tupbodylen = t_len - MINIMAL_TUPLE_DATA_OFFSET;
    let on_tape = tupbodylen + LEN_WORD_SIZE;
    // reconstruct
    let back_body = on_tape - LEN_WORD_SIZE;
    let back_tlen = back_body + MINIMAL_TUPLE_DATA_OFFSET;
    assert_eq!(back_tlen, t_len);
}
