use super::*;

#[test]
fn pipe_protocol_constants_match_syslogger_h() {
    // offsetof(PipeProtoHeader, data): 2 + 2 + 4 + 1, no padding before
    // the char[] flexible member.
    assert_eq!(PIPE_HEADER_SIZE, 9);
    assert_eq!(PIPE_MAX_PAYLOAD, PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE);
    assert_eq!(READ_BUF_SIZE, 2 * PIPE_CHUNK_SIZE);
}

#[test]
fn truncate_reserves_nul_byte() {
    let mut s = String::from("abcdef");
    truncate_to_cstr_capacity(&mut s, 4);
    assert_eq!(s, "abc");

    let mut s = String::from("ab");
    truncate_to_cstr_capacity(&mut s, 4);
    assert_eq!(s, "ab");

    let mut s = String::from("abc");
    truncate_to_cstr_capacity(&mut s, 0);
    assert_eq!(s, "");
}

#[test]
fn truncate_respects_char_boundaries() {
    let mut s = String::from("aééé"); // 'é' is 2 bytes
    truncate_to_cstr_capacity(&mut s, 5); // max 4 bytes: "aé" (3) fits, next 'é' splits
    assert_eq!(s, "aé");
}

#[test]
fn partial_header_is_left_justified() {
    let mut buf = [0u8; READ_BUF_SIZE];
    // Six junk bytes, then the start of a protocol header (a NUL): the junk
    // is dumped (to a null FILE -> reported via write_stderr) and the header
    // fragment is kept, left-justified.
    buf[..6].copy_from_slice(b"junk!!");
    // partial valid header start at offset 6
    buf[6] = 0;
    buf[7] = 0;
    let len: u16 = 100;
    buf[8..10].copy_from_slice(&len.to_ne_bytes());
    let pid: i32 = 42;
    buf[10..14].copy_from_slice(&pid.to_ne_bytes());
    buf[14] = PIPE_PROTO_IS_LAST | PIPE_PROTO_DEST_STDERR;

    let mut n = 15; // header present but payload missing
    process_pipe_input(&mut buf, &mut n);
    assert_eq!(n, 9, "the 9-byte header fragment must be retained");
    assert_eq!(buf[0], 0);
    assert_eq!(buf[1], 0);
    assert_eq!(u16::from_ne_bytes([buf[2], buf[3]]), 100);
}
