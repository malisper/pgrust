use super::*;

#[test]
fn toast_pointer_image_roundtrip() {
    let tp = VarattExternal {
        va_rawsize: 100004,
        va_extinfo: 60000 | (TOAST_LZ4_COMPRESSION_ID << VARLENA_EXTSIZE_BITS),
        va_valueid: 16400,
        va_toastrelid: 16399,
    };
    let img = tp.to_ondisk_image();
    assert_eq!(img.len(), TOAST_POINTER_SIZE);
    assert_eq!(TOAST_POINTER_SIZE, 18);
    assert_eq!((img[0], img[1]), (0x01, 18));
    assert!(varatt_is_external_ondisk(&img));
    assert_eq!(VarattExternal::from_image(&img).unwrap(), tp);
    assert_eq!(tp.extsize(), 60000);
    assert_eq!(tp.compress_method(), TOAST_LZ4_COMPRESSION_ID);
    assert!(tp.is_compressed());

    assert!(VarattExternal::from_image(&img[..10]).is_err());
    assert!(!varatt_is_external_ondisk(&[0x01, 1])); // VARTAG_INDIRECT
    assert!(!varatt_is_external_ondisk(&[0x02, 18]));
}

#[test]
fn extinfo_packing_matches_varatt_h() {
    let mut tp = VarattExternal::default();
    tp.va_rawsize = 5000 + 4;
    tp.set_size_and_compress_method(4000, TOAST_PGLZ_COMPRESSION_ID);
    assert_eq!(tp.va_extinfo, 4000); // pglz id is 0
    assert!(tp.is_compressed());

    tp.set_size_and_compress_method(4000, TOAST_LZ4_COMPRESSION_ID);
    assert_eq!(tp.va_extinfo, 4000 | (1 << 30));
    assert_eq!(tp.extsize(), 4000);
    assert_eq!(tp.compress_method(), 1);

    let un = VarattExternal {
        va_rawsize: 5004,
        va_extinfo: 5000,
        va_valueid: 1,
        va_toastrelid: 2,
    };
    assert!(!un.is_compressed());
}

#[test]
fn compressed_inline_tcinfo() {
    let mut datum = [0u8; 12];
    let word = ((12u32) << 2) | 0x02; // SET_VARSIZE_COMPRESSED
    datum[0..4].copy_from_slice(&word.to_ne_bytes());
    toast_compress_set_size_and_compress_method(&mut datum, 4096, TOAST_LZ4_COMPRESSION_ID)
        .unwrap();
    assert_eq!(toast_compress_extsize(&datum).unwrap(), 4096);
    assert_eq!(toast_compress_method(&datum).unwrap(), TOAST_LZ4_COMPRESSION_ID);

    let err = toast_compress_extsize(&datum[..6]).unwrap_err();
    assert_eq!(err.message(), "truncated compressed datum header");
}

#[test]
fn compression_constants_match_headers() {
    assert_eq!(VARLENA_EXTSIZE_BITS, 30);
    assert_eq!(VARLENA_EXTSIZE_MASK, 0x3FFF_FFFF);
    assert_eq!(TOAST_PGLZ_COMPRESSION_ID, 0);
    assert_eq!(TOAST_LZ4_COMPRESSION_ID, 1);
    assert_eq!(TOAST_INVALID_COMPRESSION_ID, 2);
    assert!(compression_method_is_valid(b'p'));
    assert!(compression_method_is_valid(b'l'));
    assert!(!compression_method_is_valid(0));
}

#[test]
fn toast_snapshot_gate() {
    let cx = mcx::MemoryContext::new("t");
    let m = cx.mcx();
    let snap = get_toast_snapshot(m, true).unwrap();
    assert_eq!(snap.snapshot_type as i32, SnapshotType::SNAPSHOT_TOAST as i32);
    let err = get_toast_snapshot(m, false).unwrap_err();
    assert_eq!(err.message(), "cannot fetch toast data without an active snapshot");
}
