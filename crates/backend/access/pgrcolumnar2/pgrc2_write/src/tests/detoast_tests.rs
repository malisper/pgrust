//! Detoast-on-ingest (M3-D slice leg 5): byte-exactness on the detoasted
//! image — toasted and inline inputs CONVERGE to identical part bytes (the
//! pinning test), plus the typed refusal arms and the StrView §7b
//! varlena-shaped-output pin.

use super::*;
use crate::ingest::{normalize_varlena, ExternalDetoast, NoExternalDetoast, VarlenaForm};
use crate::publish::TxnVerdict;
use crate::WriteError;
use pgrc2_format::abi::{ByteArena, DecodeOut, KernelCtx, KernelKey};
use pgrc2_format::part::{StreamRole, StreamSectionHdr};
use pgrc2_format::wire::varlena_4b_u_payload_len;

fn payload() -> Vec<u8> {
    // Compressible (repetitive) so the pglz arm works, ≤126 B for the short
    // arm.
    b"abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabc".to_vec()
}

#[test]
fn normalize_arms_agree() {
    let p = payload();
    let mut ext = NoExternalDetoast;
    let mut s1 = Vec::new();
    let mut s2 = Vec::new();
    let mut s3 = Vec::new();
    let a = img_4b_u(&p);
    let b = img_short(&p);
    let c = img_pglz(&p);
    let (pa, fa) = normalize_varlena(&a, &mut ext, &mut s1).expect("4b");
    let (pb, fb) = normalize_varlena(&b, &mut ext, &mut s2).expect("short");
    let (pc, fc) = normalize_varlena(&c, &mut ext, &mut s3).expect("pglz");
    assert_eq!(fa, VarlenaForm::Plain4B);
    assert_eq!(fb, VarlenaForm::Short1B);
    assert_eq!(fc, VarlenaForm::CompressedPglz);
    assert_eq!(pa, p.as_slice());
    assert_eq!(pb, p.as_slice());
    assert_eq!(pc, p.as_slice());
}

#[test]
fn lz4_toast_refused_typed() {
    let p = payload();
    // Hand-build a 4B-C image claiming lz4 (method bits = 1).
    let mut img = Vec::new();
    let total = 8 + p.len();
    img.extend_from_slice(&((((total as u32) << 2) | 0x02).to_le_bytes()));
    let tcinfo = (p.len() as u32) | (1 << 30);
    img.extend_from_slice(&tcinfo.to_le_bytes());
    img.extend_from_slice(&p);
    let mut scratch = Vec::new();
    let err = normalize_varlena(&img, &mut NoExternalDetoast, &mut scratch).unwrap_err();
    assert_eq!(
        err,
        WriteError::Refused {
            what: "compression method lz4 not supported"
        }
    );
}

#[test]
fn external_pointer_refused_without_capability_served_with_one() {
    // 1B_E image: b0 == 0x01 (tag byte follows; content irrelevant to the
    // dispatch decision).
    let img = [0x01u8, 18, 0, 0, 0, 0];
    let mut scratch = Vec::new();
    let err = normalize_varlena(&img, &mut NoExternalDetoast, &mut scratch).unwrap_err();
    assert!(matches!(err, WriteError::Refused { .. }));

    struct FakeExt(Vec<u8>);
    impl ExternalDetoast for FakeExt {
        fn detoast_external(&mut self, _image: &[u8], out: &mut Vec<u8>) -> crate::WriteResult<()> {
            out.extend_from_slice(&self.0);
            Ok(())
        }
    }
    let mut ext = FakeExt(payload());
    let mut scratch2 = Vec::new();
    let (p, form) = normalize_varlena(&img, &mut ext, &mut scratch2).expect("external");
    assert_eq!(form, VarlenaForm::External);
    assert_eq!(p, payload().as_slice());
}

/// The convergence pin: the same value ingested as 4B-U, 1B-short, and
/// pglz-compressed toast produces IDENTICAL part files.
#[test]
fn toast_forms_converge_to_identical_part_bytes() {
    let p = payload();
    let run = |img: Vec<u8>| -> Vec<u8> {
        let mut vfs = mem_with_dir();
        let mut kit = Kit::new();
        let mut w = open_writer(vec![text_col(1)], stamp(7, 1));
        for _ in 0..100 {
            let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &kit.resolver,
                shred: &mut kit.shred,
                shred_opts: &kit.opts,
            };
            w.append_row(&[RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
                .expect("append");
        }
        let probe = Probe::new(TxnVerdict::InProgress).set(7, TxnVerdict::Committed);
        finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
        vfs.read_full(&format!("{DIR}/part-0.pgrc2")).expect("part")
    };
    let from_plain = run(img_4b_u(&p));
    let from_short = run(img_short(&p));
    let from_pglz = run(img_pglz(&p));
    assert!(from_plain == from_short, "short form diverged");
    assert!(from_plain == from_pglz, "pglz form diverged");
}

/// StrView §7b pin (the M3-D binding: ingest detoast): decoded string
/// outputs from the sealed bytes are varlena-shaped, 8-aligned datums.
#[test]
fn decoded_varlena_outputs_are_varlena_shaped() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![text_col(1)], stamp(7, 1));
    let imgs: Vec<Vec<u8>> = (0..50).map(|i| img_4b_u(format!("v{i}").as_bytes())).collect();
    for img in &imgs {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(7, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let pv = PartView::open(&mut vfs, "part-0.pgrc2");
    let (entry, extents) = pv.stream(1, 0, StreamRole::Values).expect("values");
    let ext0 = &extents[0];
    let section = &pv.bytes[ext0.file_off as usize..(ext0.file_off + ext0.len) as usize];
    let hdr = StreamSectionHdr::decode(section).expect("hdr");
    let ft = hdr.frame_table(section).expect("ft");
    let key = KernelKey {
        encoding: entry.encoding,
        class: entry.class,
        width: entry.width,
    };
    use crate::seal::VerifyResolver;
    let vt = crate::seal::ReferenceResolver.resolve(key).expect("vtable");
    let mut datums = vec![0u64; 50];
    let mut arena_buf = vec![0u8; 64 * 1024];
    let mut out = DecodeOut {
        datums: &mut datums,
        arena: ByteArena::new(&mut arena_buf),
    };
    let ctx = KernelCtx {
        key,
        flags: entry.flags,
        fixed_len: 0,
        bytes: section,
        frame_table: ft.as_deref(),
        granule: 0,
        granule_in_extent: 0,
        rows: 50,
        values: 50,
        validity_bytes: None,
        overflow: None,
        dict: None,
    };
    let n = (vt.decode_full)(&ctx, &mut out).expect("decode");
    assert_eq!(n, 50);
    for (i, &d) in out.datums.iter().enumerate() {
        assert_eq!(d % 8, 0, "varlena datum not 8-aligned (spec §19.4)");
        // SAFETY: datum points into the live arena.
        let header =
            u32::from_le_bytes(unsafe { std::slice::from_raw_parts(d as *const u8, 4) }.try_into().unwrap());
        let len = varlena_4b_u_payload_len(header, "pin").expect("varlena-shaped (§7b)");
        // SAFETY: image is header + len bytes in the arena.
        let got = unsafe { std::slice::from_raw_parts((d + 4) as *const u8, len as usize) };
        assert_eq!(got, format!("v{i}").as_bytes());
    }
}
