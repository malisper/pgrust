use super::*;

fn compress(input: &[u8], strategy: &PglzStrategy) -> Option<Vec<u8>> {
    let mut dest = vec![MaybeUninit::<u8>::uninit(); pglz_max_output(input.len())];
    let n = pglz_compress_into(input, &mut dest, strategy)?;
    Some(dest[..n].iter().map(|b| unsafe { b.assume_init() }).collect())
}

fn decompress(src: &[u8], rawsize: usize, complete: bool) -> Option<Vec<u8>> {
    let mut dest = vec![MaybeUninit::<u8>::uninit(); rawsize];
    let n = pglz_decompress(src, &mut dest, complete)?;
    Some(dest[..n].iter().map(|b| unsafe { b.assume_init() }).collect())
}

#[test]
fn default_strategy_rejects_small_input() {
    assert_eq!(compress(b"short", &PGLZ_STRATEGY_DEFAULT), None);
}

#[test]
fn always_strategy_roundtrips_repetitive_input() {
    let input = b"abcabcabcabcabcabcabcabcabcabcabcabc";
    let c = compress(input, &PGLZ_STRATEGY_ALWAYS).unwrap();
    assert!(c.len() < input.len());
    assert_eq!(decompress(&c, input.len(), true).unwrap(), input);
}

#[test]
fn default_strategy_roundtrips_large_repetitive_input() {
    let input = vec![b'x'; 2048];
    let c = compress(&input, &PGLZ_STRATEGY_DEFAULT).unwrap();
    assert_eq!(decompress(&c, input.len(), true).unwrap(), input);
}

#[test]
fn decompresses_literal_stream() {
    assert_eq!(decompress(&[0, b'a', b'b', b'c'], 3, true).unwrap(), b"abc");
}

#[test]
fn decompresses_match_stream() {
    assert_eq!(
        decompress(&[0b0000_0010, b'a', 0x01, 0x01], 4, true).unwrap(),
        b"aaaa"
    );
}

#[test]
fn incomplete_decompression_is_corrupt_only_when_checked() {
    assert_eq!(decompress(&[0, b'a'], 2, true), None);
    assert_eq!(decompress(&[0, b'a'], 2, false).unwrap(), b"a");
}

#[test]
fn rejects_bad_backreference_and_truncated_tag() {
    assert_eq!(decompress(&[1, 0x00, 0x00], 3, true), None);
    assert_eq!(decompress(&[0b10, b'a', 0x01], 20, false), None);
    assert_eq!(decompress(&[0b10, b'a', 0x0f, 0x01], 20, false), None);
    // A tag at exactly source end isn't reached (loop bound) — C stops too.
    assert_eq!(decompress(&[0b10, b'a'], 20, false).unwrap(), b"a");
}

#[test]
fn slice_decompress_stops_at_dest() {
    let input: Vec<u8> = (0..512u32).map(|i| (i % 61) as u8).collect();
    let c = compress(&input, &PGLZ_STRATEGY_ALWAYS).unwrap();
    for take in [1usize, 7, 64, 300, 511] {
        assert_eq!(decompress(&c, take, false).unwrap(), &input[..take]);
    }
}

#[test]
fn decompress_slice_wrapper_matches() {
    let input = vec![b'q'; 700];
    let c = compress(&input, &PGLZ_STRATEGY_DEFAULT).unwrap();
    let mut out = vec![0u8; 700];
    assert_eq!(pglz_decompress_slice(&c, &mut out, true), Some(700));
    assert_eq!(out, input);
}

#[test]
fn negative_match_size_drop_roundtrips() {
    let s = PglzStrategy {
        match_size_drop: -50,
        first_success_by: 512,
        max_input_size: 1 << 20,
        ..PGLZ_STRATEGY_DEFAULT
    };
    let input = vec![b'z'; 4096];
    let c = compress(&input, &s).unwrap();
    assert!(c.len() < input.len());
    assert_eq!(decompress(&c, input.len(), true).unwrap(), input);
}

#[test]
fn hist_idx_matches_platform_char_signedness() {
    let signed = (0x80u8 as c_char as i32) < 0;
    let expect = if signed { 384 } else { 0x80 << 6 & 0x1FFF ^ 0x80 << 4 ^ 0x80 << 2 ^ 0x80 };
    let _ = expect;
    let input = [0x80u8; 4];
    let four = hist_idx(&input, 0, 0x1FFF);
    let short = hist_idx(&input[..1], 0, 0x1FFF);
    if signed {
        assert_eq!(short, 0x1F80);
        assert_eq!(four, (((-128i32) << 6) ^ ((-128) << 4) ^ ((-128) << 2) ^ -128) as usize & 0x1FFF);
    } else {
        assert_eq!(short, 0x80);
        assert_eq!(four, ((0x80 << 6) ^ (0x80 << 4) ^ (0x80 << 2) ^ 0x80) & 0x1FFF);
    }
}

// LCG digits stream: the arrays.sql shape that hung fabled's compressor when
// a recycled bucket-head spliced onto itself; must terminate + roundtrip.
#[test]
#[cfg_attr(miri, ignore)]
fn large_randomish_text_terminates_and_roundtrips() {
    let mut s: u64 = 0x12345678;
    let digits = b"0123456789.";
    let input: Vec<u8> = (0..200_000)
        .map(|_| {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            digits[((s >> 33) as usize) % digits.len()]
        })
        .collect();
    if let Some(c) = compress(&input, &PGLZ_STRATEGY_ALWAYS) {
        assert_eq!(decompress(&c, input.len(), true).unwrap(), input);
    }
}

#[test]
fn max_output_matches_macro() {
    assert_eq!(pglz_max_output(0), 4);
    assert_eq!(pglz_max_output(100), 104);
}
