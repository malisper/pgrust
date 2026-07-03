use super::*;
use core::mem::MaybeUninit;
use mcx::MemoryContext;

fn compress_image(input: &[u8], method: u32) -> Vec<u8> {
    let mut dest = vec![MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(input.len())];
    let n = pglz::pglz_compress_into(input, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS).unwrap();
    let total = VARHDRSZ_COMPRESSED + n;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&(((total as u32) << 2) | 0x02).to_ne_bytes());
    image.extend_from_slice(&((input.len() as u32) | (method << 30)).to_ne_bytes());
    image.extend(dest[..n].iter().map(|b| unsafe { b.assume_init() }));
    image
}

fn plain_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + VARHDRSZ;
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(payload);
    image
}

fn short_image(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + 1;
    assert!(total <= 0x7F);
    let mut image = vec![((total as u8) << 1) | 0x01];
    image.extend_from_slice(payload);
    image
}

fn ondisk_image(rawsize: i32, extsize: u32, method: u32) -> Vec<u8> {
    let mut image = vec![0x01, VARTAG_ONDISK];
    image.extend_from_slice(&rawsize.to_ne_bytes());
    image.extend_from_slice(&(extsize | (method << 30)).to_ne_bytes());
    image.extend_from_slice(&0xBEEFu32.to_ne_bytes());
    image.extend_from_slice(&0xCAFEu32.to_ne_bytes());
    image
}

fn sample(n: usize) -> Vec<u8> {
    let phrase = b"toast toast toast and more toast, buttered ";
    (0..n).map(|i| phrase[i % phrase.len()]).collect()
}

#[test]
fn detoast_attr_decompresses_inline_pglz() {
    let ctx = MemoryContext::new("t");
    let input = sample(1500);
    let image = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    let out = detoast_attr(ctx.mcx(), &image).unwrap();
    assert_eq!(varsize_4b(&out), input.len() + VARHDRSZ);
    assert_eq!(&out[VARHDRSZ..], &input[..]);
}

#[test]
fn detoast_attr_converts_short_header() {
    let ctx = MemoryContext::new("t");
    let image = short_image(b"tiny");
    let out = detoast_attr(ctx.mcx(), &image).unwrap();
    assert_eq!(varsize_4b(&out), 4 + VARHDRSZ);
    assert_eq!(&out[VARHDRSZ..], b"tiny");
}

#[test]
fn detoast_attr_copies_plain_verbatim() {
    let ctx = MemoryContext::new("t");
    let image = plain_image(b"plain value");
    let out = detoast_attr(ctx.mcx(), &image).unwrap();
    assert_eq!(&out[..], &image[..]);
}

#[test]
fn detoast_attr_slice_on_inline_compressed() {
    let ctx = MemoryContext::new("t");
    let input = sample(4000);
    let image = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    for (off, len) in [(0i32, 10i32), (100, 250), (3990, 100), (0, -1), (777, -1), (5000, 33)] {
        let out = detoast_attr_slice(ctx.mcx(), &image, off, len).unwrap();
        let expect: &[u8] = if off as usize >= input.len() {
            &[]
        } else {
            let end = if len < 0 {
                input.len()
            } else {
                input.len().min((off + len) as usize)
            };
            &input[off as usize..end]
        };
        assert_eq!(&out[VARHDRSZ..], expect, "off={off} len={len}");
        assert_eq!(varsize_4b(&out), expect.len() + VARHDRSZ);
    }
}

#[test]
fn detoast_attr_slice_on_plain_and_short() {
    let ctx = MemoryContext::new("t");
    let plain = plain_image(b"hello world");
    let out = detoast_attr_slice(ctx.mcx(), &plain, 6, 5).unwrap();
    assert_eq!(&out[VARHDRSZ..], b"world");
    let short = short_image(b"hello world");
    let out = detoast_attr_slice(ctx.mcx(), &short, 0, 5).unwrap();
    assert_eq!(&out[VARHDRSZ..], b"hello");
}

#[test]
fn detoast_attr_slice_rejects_negative_offset() {
    let ctx = MemoryContext::new("t");
    let err = detoast_attr_slice(ctx.mcx(), &plain_image(b"x"), -1, 5).unwrap_err();
    assert!(err.message().contains("invalid sliceoffset: -1"));
}

#[test]
fn decompress_slice_overlong_request_takes_full_path() {
    let ctx = MemoryContext::new("t");
    let input = sample(600);
    let image = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    let out = toast_decompress_datum_slice(ctx.mcx(), &image, 100_000).unwrap();
    assert_eq!(&out[VARHDRSZ..], &input[..]);
    let out = toast_decompress_datum_slice(ctx.mcx(), &image, 128).unwrap();
    assert_eq!(&out[VARHDRSZ..], &input[..128]);
    assert_eq!(varsize_4b(&out), 128 + VARHDRSZ);
}

#[test]
fn lz4_and_invalid_method_ids_error() {
    let ctx = MemoryContext::new("t");
    let input = sample(200);
    let lz4 = compress_image(&input, TOAST_LZ4_COMPRESSION_ID);
    let err = toast_decompress_datum(ctx.mcx(), &lz4).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    let bad = compress_image(&input, 2);
    let err = toast_decompress_datum(ctx.mcx(), &bad).unwrap_err();
    assert!(err.message().contains("invalid compression method id 2"));
}

#[test]
fn corrupt_pglz_stream_errors_with_data_corrupted() {
    let ctx = MemoryContext::new("t");
    let input = sample(200);
    let mut image = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    let n = image.len();
    image.truncate(n - 5);
    let hdr = (((image.len() as u32) << 2) | 0x02).to_ne_bytes();
    image[..VARHDRSZ].copy_from_slice(&hdr);
    let err = toast_decompress_datum(ctx.mcx(), &image).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
}

#[test]
fn raw_and_physical_sizes() {
    let input = sample(900);
    let compressed = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    assert_eq!(toast_raw_datum_size(&compressed), input.len() + VARHDRSZ);
    assert_eq!(toast_datum_size(&compressed), varsize_4b(&compressed));

    let short = short_image(b"shorty");
    assert_eq!(toast_raw_datum_size(&short), 6 + VARHDRSZ);
    assert_eq!(toast_datum_size(&short), 7);

    let plain = plain_image(b"plain");
    assert_eq!(toast_raw_datum_size(&plain), 5 + VARHDRSZ);
    assert_eq!(toast_datum_size(&plain), 5 + VARHDRSZ);

    let ondisk = ondisk_image(10_004, 6_000, TOAST_PGLZ_COMPRESSION_ID);
    assert_eq!(toast_raw_datum_size(&ondisk), 10_004);
    assert_eq!(toast_datum_size(&ondisk), 6_000);
    assert_eq!(varsize_any(&ondisk), 18);
    let tp = VarattExternal::from_image(&ondisk);
    assert!(tp.is_compressed());
    assert_eq!(tp.compress_method(), TOAST_PGLZ_COMPRESSION_ID);
    let uncompressed = ondisk_image(6_004, 6_000, 0);
    assert!(!VarattExternal::from_image(&uncompressed).is_compressed());
}

// Seams are set-once per process: this single test installs both this
// crate's inbound seam and a fake toast fetch, then drives the external arm.
#[test]
fn seam_install_and_external_fetch_arm() {
    let ctx = MemoryContext::new("t");
    init_seams();
    let input = sample(300);
    let inline = compress_image(&input, TOAST_PGLZ_COMPRESSION_ID);
    let via_seam = detoast_seams::detoast_attr::call(ctx.mcx(), &inline).unwrap();
    assert_eq!(&via_seam[VARHDRSZ..], &input[..]);

    // Fake fetch: hand back a fixed compressed image for any ondisk pointer.
    fn fake_fetch<'mcx>(mcx: Mcx<'mcx>, _attr: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
        let input: Vec<u8> = {
            let phrase = b"toast toast toast and more toast, buttered ";
            (0..300).map(|i| phrase[i % phrase.len()]).collect()
        };
        let mut dest =
            vec![MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(input.len())];
        let n = pglz::pglz_compress_into(&input, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS).unwrap();
        let total = VARHDRSZ_COMPRESSED + n;
        let mut v = mcx::vec_with_capacity_in(mcx, total)?;
        mcx::vec_append_bytes(&mut v, &(((total as u32) << 2) | 0x02).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, &(input.len() as u32).to_ne_bytes())?;
        for b in &dest[..n] {
            mcx::vec_append_bytes(&mut v, &[unsafe { b.assume_init() }])?;
        }
        Ok(v)
    }
    toast_internals_seams::toast_fetch_datum::set(fake_fetch);
    let ondisk = ondisk_image(304, 250, TOAST_PGLZ_COMPRESSION_ID);
    let out = detoast_attr(ctx.mcx(), &ondisk).unwrap();
    assert_eq!(&out[VARHDRSZ..], &input[..]);
}

#[repr(C)]
struct FakeExpanded {
    hdr: datum::ExpandedObjectHeader,
    payload: [u8; 11],
}

unsafe fn fake_flat_size(_eohptr: *mut datum::ExpandedObjectHeader) -> usize {
    VARHDRSZ + 11
}

unsafe fn fake_flatten(eohptr: *mut datum::ExpandedObjectHeader, result: *mut u8, n: usize) {
    assert_eq!(n, VARHDRSZ + 11);
    let obj = eohptr as *mut FakeExpanded;
    core::ptr::copy_nonoverlapping(((n as u32) << 2).to_ne_bytes().as_ptr(), result, VARHDRSZ);
    core::ptr::copy_nonoverlapping((*obj).payload.as_ptr(), result.add(VARHDRSZ), 11);
}

static FAKE_METHODS: datum::ExpandedObjectMethods = datum::ExpandedObjectMethods {
    get_flat_size: fake_flat_size,
    flatten_into: fake_flatten,
};

#[test]
fn expanded_arms_flatten() {
    let ctx = MemoryContext::new("t");
    let obj = Box::into_raw(Box::new(FakeExpanded {
        hdr: datum::ExpandedObjectHeader::empty(),
        payload: *b"hello world",
    }));
    let (rw_image, ro_image): (Vec<u8>, Vec<u8>) = unsafe {
        let hdr = core::ptr::addr_of_mut!((*obj).hdr);
        datum::expandeddatum::eoh_init_header(hdr, &FAKE_METHODS, core::ptr::null());
        let rw = datum::expandeddatum::eohp_get_rw_datum(hdr).as_usize() as *const u8;
        let ro = datum::expandeddatum::eohp_get_ro_datum(hdr).as_usize() as *const u8;
        let sz = datum::expandeddatum::EXPANDED_POINTER_SIZE;
        (
            core::slice::from_raw_parts(rw, sz).to_vec(),
            core::slice::from_raw_parts(ro, sz).to_vec(),
        )
    };

    for image in [&rw_image, &ro_image] {
        let out = detoast_external_attr(ctx.mcx(), image).unwrap();
        assert_eq!(varsize_4b(&out), VARHDRSZ + 11);
        assert_eq!(&out[VARHDRSZ..], b"hello world");

        let out = detoast_attr(ctx.mcx(), image).unwrap();
        assert_eq!(&out[VARHDRSZ..], b"hello world");

        let out = detoast_attr_slice(ctx.mcx(), image, 6, 5).unwrap();
        assert_eq!(&out[VARHDRSZ..], b"world");

        assert_eq!(toast_raw_datum_size(image), VARHDRSZ + 11);
        assert_eq!(toast_datum_size(image), VARHDRSZ + 11);
        assert_eq!(varsize_any(image), 10);
    }
    drop(unsafe { Box::from_raw(obj) });
}
