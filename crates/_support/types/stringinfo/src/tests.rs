use super::*;
use mcx::MemoryContext;

fn sentinel(s: &StringInfo<'_>) -> u8 {
    unsafe { *s.data.as_ptr().add(s.len()) }
}

#[test]
fn init_default() {
    let ctx = MemoryContext::new("t");
    let s = StringInfo::new_in(ctx.mcx()).unwrap();
    assert_eq!(s.len(), 0);
    assert_eq!(s.capacity(), 1024);
    assert_eq!(s.cursor, 0);
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn append_within_capacity_does_not_grow() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'x'; 1023]).unwrap();
    assert_eq!(s.capacity(), 1024);
    assert_eq!(s.len(), 1023);
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn append_at_boundary_doubles() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'x'; 1024]).unwrap();
    assert_eq!(s.capacity(), 2048);
    assert_eq!(s.len(), 1024);
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn big_append_multi_doubles() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'y'; 5000]).unwrap();
    assert_eq!(s.capacity(), 8192);
    assert_eq!(s.len(), 5000);
}

#[test]
fn append_byte_growth_matches_c() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'x'; 1022]).unwrap();
    s.append_byte(b'a').unwrap();
    assert_eq!(s.capacity(), 1024);
    s.append_byte(b'b').unwrap();
    assert_eq!(s.capacity(), 2048);
    assert_eq!(s.len(), 1024);
    assert_eq!(&s.as_bytes()[1022..], b"ab");
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn max_size_error() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(b"abc").unwrap();
    let err = s.enlarge(MAX_ALLOC_SIZE - 3).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert_eq!(
        err.message(),
        "string buffer exceeds maximum allowed length (1073741823 bytes)"
    );
    assert_eq!(
        err.detail(),
        Some("Cannot enlarge string buffer containing 3 bytes by 1073741820 more bytes.")
    );
    assert_eq!(s.len(), 3);
    assert_eq!(s.capacity(), 1024);
}

#[test]
fn enlarge_pre_reserves_without_len_change() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(b"hello").unwrap();
    s.enlarge(3000).unwrap();
    assert_eq!(s.len(), 5);
    assert_eq!(s.capacity(), 4096);
    assert_eq!(s.as_bytes(), b"hello");
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn append_correctness_interleaved() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    let mut reference = alloc::vec::Vec::new();
    for i in 0..2000usize {
        let chunk = [(i % 251) as u8; 7];
        let take = i % 8;
        s.append_bytes(&chunk[..take]).unwrap();
        reference.extend_from_slice(&chunk[..take]);
        s.append_byte((i % 256) as u8).unwrap();
        reference.push((i % 256) as u8);
    }
    assert_eq!(s.as_bytes(), reference.as_slice());
    assert_eq!(sentinel(&s), 0);
    assert_eq!(s.cursor, 0);
}

#[test]
fn append_nt_then_byte() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes_nt(b"raw").unwrap();
    s.append_byte(b'!').unwrap();
    assert_eq!(s.as_bytes(), b"raw!");
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn append_spaces_and_str() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_str("a").unwrap();
    s.append_spaces(4).unwrap();
    s.append_spaces(0).unwrap();
    s.append_str("b").unwrap();
    assert_eq!(s.as_bytes(), b"a    b");
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn reset_keeps_allocation() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'z'; 3000]).unwrap();
    s.cursor = 7;
    let cap = s.capacity();
    s.reset();
    assert_eq!(s.len(), 0);
    assert_eq!(s.cursor, 0);
    assert_eq!(s.capacity(), cap);
    assert_eq!(sentinel(&s), 0);
}

#[test]
fn from_vec_appendable() {
    let ctx = MemoryContext::new("t");
    let mut v = PgVec::new_in(ctx.mcx());
    mcx::vec_append_bytes(&mut v, b"seed").unwrap();
    let mut s = StringInfo::from_vec(v).unwrap();
    assert_eq!(s.as_bytes(), b"seed");
    assert_eq!(s.cursor, 0);
    assert_eq!(sentinel(&s), 0);
    s.append_bytes(b"+more").unwrap();
    assert_eq!(s.as_bytes(), b"seed+more");
}

#[test]
fn into_vec_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(b"payload").unwrap();
    let v = s.into_vec();
    assert_eq!(v.as_slice(), b"payload");
}

#[test]
fn append_bytes_z_counts_nul_in_len() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes_z(b"hi").unwrap();
    assert_eq!(s.len(), 3);
    assert_eq!(s.as_bytes(), b"hi\0");
    s.append_bytes_z(b"").unwrap();
    assert_eq!(s.as_bytes(), b"hi\0\0");
}

#[test]
fn append_bytes_z_grows() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::new_in(ctx.mcx()).unwrap();
    s.append_bytes(&[b'x'; 1023]).unwrap();
    s.append_bytes_z(b"").unwrap();
    assert_eq!(s.capacity(), 2048);
    assert_eq!(s.len(), 1024);
}

#[test]
fn write_fixed_after_enlarge() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::with_capacity_in(ctx.mcx(), 8).unwrap();
    s.enlarge(8).unwrap();
    s.write_fixed([1, 2, 3, 4]);
    s.write_fixed(0x0506u16.to_be_bytes());
    assert_eq!(s.as_bytes(), &[1, 2, 3, 4, 5, 6]);
}

#[test]
#[should_panic]
fn write_fixed_without_room_panics() {
    let ctx = MemoryContext::new("t");
    let mut s = StringInfo::with_capacity_in(ctx.mcx(), 2).unwrap();
    s.write_fixed([1, 2, 3, 4]);
}
