use std::cell::RefCell;

use ::mcx::{Mcx, MemoryContext};
use ::sink::{
    bbsink_archive_contents, bbsink_begin_archive, bbsink_begin_backup, bbsink_begin_manifest,
    bbsink_cleanup, bbsink_end_archive, bbsink_end_backup, bbsink_end_manifest,
    bbsink_manifest_contents, Bbsink, BbsinkOps, BbsinkState,
};
use ::types_core::{Size, TimeLineID, XLogRecPtr};
use ::types_error::PgResult;
use compression::{parse_compress_specification, PgCompressAlgorithm};

const BUFLEN: usize = 32768;

#[derive(Default, Clone)]
struct Captured {
    archives: Vec<(String, Vec<u8>)>,
    manifest: Vec<u8>,
    chunk_lens: Vec<usize>,
}

/// Terminal sink capturing forwarded archives + manifest.
struct CaptureOps<'a, 'mcx> {
    out: &'a RefCell<Captured>,
    mcx: Mcx<'mcx>,
}

impl<'a, 'mcx> BbsinkOps<'mcx> for CaptureOps<'a, 'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        let len = sink.buffer_length();
        sink.set_buffer(self.mcx, len)
    }
    fn begin_archive(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        name: &str,
    ) -> PgResult<()> {
        self.out.borrow_mut().archives.push((name.to_string(), Vec::new()));
        Ok(())
    }
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let mut out = self.out.borrow_mut();
        out.chunk_lens.push(len);
        let data = sink.buffer_slice(len).to_vec();
        out.archives.last_mut().expect("archive begun").1.extend_from_slice(&data);
        Ok(())
    }
    fn end_archive(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
    fn begin_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()> {
        let data = sink.buffer_slice(len).to_vec();
        self.out.borrow_mut().manifest.extend_from_slice(&data);
        Ok(())
    }
    fn end_manifest(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
    fn end_backup(
        &mut self,
        _sink: &mut Bbsink<'mcx>,
        _state: &mut BbsinkState,
        _endptr: XLogRecPtr,
        _endtli: TimeLineID,
    ) -> PgResult<()> {
        Ok(())
    }
    fn cleanup(&mut self, _sink: &mut Bbsink<'mcx>, _state: &mut BbsinkState) -> PgResult<()> {
        Ok(())
    }
}

/// Pseudo-random but deterministic payload with compressible structure.
fn payload(total: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(total);
    let mut x: u64 = 0x243F6A8885A308D3;
    while v.len() < total {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        // Mix of repetitive and noisy runs.
        if (x >> 60) & 1 == 0 {
            v.extend_from_slice(b"pgrust base backup block ");
        } else {
            v.push((x >> 32) as u8);
        }
    }
    v.truncate(total);
    v
}

#[derive(Clone, Copy)]
enum Codec {
    Gzip,
    Lz4,
    #[cfg(not(target_family = "wasm"))]
    Zstd,
}

/// Drive a full backup through a compression sink of the given codec: two
/// archives + a manifest, feeding `data` in buffer-length chunks. Returns
/// what the terminal sink saw.
fn run_chain(
    data: &[u8],
    manifest: &[u8],
    codec: Codec,
    sp: &compression::PgCompressSpecification,
) -> Captured {
    let out = RefCell::new(Captured::default());
    let ctx = MemoryContext::new("compress sink test");
    {
        let mcx = ctx.mcx();
        let terminal = Box::new(Bbsink::new(
            mcx,
            Box::new(CaptureOps { out: &out, mcx }),
            None,
        ));
        let mut sink = match codec {
            Codec::Gzip => crate::bbsink_gzip_new(mcx, terminal, sp),
            Codec::Lz4 => crate::bbsink_lz4_new(mcx, terminal, sp),
            #[cfg(not(target_family = "wasm"))]
            Codec::Zstd => crate::bbsink_zstd_new(mcx, terminal, sp),
        }
        .expect("sink construction");
        let mut state = BbsinkState::default();

        bbsink_begin_backup(&mut sink, &mut state, BUFLEN).expect("begin_backup");
        for archive in ["base.tar", "16384.tar"] {
            bbsink_begin_archive(&mut sink, &mut state, archive).expect("begin_archive");
            let mut off = 0;
            while off < data.len() {
                let n = BUFLEN.min(data.len() - off);
                sink.buffer_slice_mut(n).copy_from_slice(&data[off..off + n]);
                bbsink_archive_contents(&mut sink, &mut state, n).expect("archive_contents");
                off += n;
            }
            bbsink_end_archive(&mut sink, &mut state).expect("end_archive");
        }
        bbsink_begin_manifest(&mut sink, &mut state).expect("begin_manifest");
        let mut off = 0;
        while off < manifest.len() {
            let n = BUFLEN.min(manifest.len() - off);
            sink.buffer_slice_mut(n).copy_from_slice(&manifest[off..off + n]);
            bbsink_manifest_contents(&mut sink, &mut state, n).expect("manifest_contents");
            off += n;
        }
        bbsink_end_manifest(&mut sink, &mut state).expect("end_manifest");
        bbsink_end_backup(&mut sink, &mut state, 0, 1).expect("end_backup");
        bbsink_cleanup(&mut sink, &mut state).expect("cleanup");
    }
    out.into_inner()
}

fn spec(alg: PgCompressAlgorithm, detail: Option<&str>) -> compression::PgCompressSpecification {
    let s = parse_compress_specification(alg, detail);
    assert!(s.parse_error.is_none());
    assert!(compression::validate_compress_specification(&s).is_none());
    s
}

// --------------------------------------------------------------------------
// gzip
// --------------------------------------------------------------------------

fn gunzip(member: &[u8]) -> Vec<u8> {
    // RFC 1952: 10-byte header (no extra fields expected), deflate body,
    // 8-byte trailer.
    assert!(member.len() > 18, "gzip member too short");
    assert_eq!(&member[..3], &[0x1f, 0x8b, 0x08], "gzip magic + deflate CM");
    assert_eq!(member[3], 0, "no FLG bits expected");
    let body = &member[10..member.len() - 8];
    let out = miniz_oxide::inflate::decompress_to_vec(body).expect("raw inflate");
    let crc = u32::from_le_bytes(member[member.len() - 8..member.len() - 4].try_into().unwrap());
    let isize_ = u32::from_le_bytes(member[member.len() - 4..].try_into().unwrap());
    assert_eq!(crc, crc32c::zlib_crc32_extend(0, &out), "trailer CRC32");
    assert_eq!(isize_, out.len() as u32, "trailer ISIZE");
    out
}

#[test]
fn gzip_round_trip() {
    let data = payload(200_000);
    let manifest = b"{\"PostgreSQL-Backup-Manifest-Version\": 1}".to_vec();
    let sp = spec(PgCompressAlgorithm::Gzip, None);
    let got = run_chain(&data, &manifest, Codec::Gzip, &sp);

    assert_eq!(got.archives.len(), 2);
    for (name, bytes) in &got.archives {
        assert!(name.ends_with(".tar.gz"), "archive renamed: {name}");
        assert_eq!(gunzip(bytes), data);
        assert!(bytes.len() < data.len(), "compressible payload shrank");
    }
    assert_eq!(got.archives[0].0, "base.tar.gz");
    assert_eq!(got.archives[1].0, "16384.tar.gz");
    // Manifest passes through uncompressed.
    assert_eq!(got.manifest, manifest);
    // Every forwarded chunk fit the successor's buffer.
    assert!(got.chunk_lens.iter().all(|&n| n > 0 && n <= BUFLEN));
}

#[test]
fn gzip_levels_round_trip() {
    let data = payload(60_000);
    for detail in ["1", "9", "level=5"] {
        let sp = spec(PgCompressAlgorithm::Gzip, Some(detail));
        let got = run_chain(&data, b"", Codec::Gzip, &sp);
        assert_eq!(gunzip(&got.archives[0].1), data, "level {detail}");
    }
}

#[test]
fn gzip_empty_archive() {
    // An archive with no contents still produces a valid empty gzip member.
    let sp = spec(PgCompressAlgorithm::Gzip, None);
    let got = run_chain(&[], b"", Codec::Gzip, &sp);
    assert_eq!(gunzip(&got.archives[0].1), Vec::<u8>::new());
}

// --------------------------------------------------------------------------
// lz4
// --------------------------------------------------------------------------

fn unlz4(frame: &[u8]) -> Vec<u8> {
    use std::io::Read;
    let mut dec = lz4_flex::frame::FrameDecoder::new(frame);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).expect("lz4 frame decode");
    out
}

#[test]
fn lz4_round_trip() {
    let data = payload(200_000);
    let manifest = b"manifest bytes".to_vec();
    let sp = spec(PgCompressAlgorithm::Lz4, None);
    let got = run_chain(&data, &manifest, Codec::Lz4, &sp);

    assert_eq!(got.archives.len(), 2);
    for (name, bytes) in &got.archives {
        assert!(name.ends_with(".tar.lz4"), "archive renamed: {name}");
        assert_eq!(unlz4(bytes), data);
    }
    assert_eq!(got.manifest, manifest);
}

#[test]
fn lz4_empty_archive() {
    let sp = spec(PgCompressAlgorithm::Lz4, None);
    let got = run_chain(&[], b"", Codec::Lz4, &sp);
    assert_eq!(unlz4(&got.archives[0].1), Vec::<u8>::new());
}

// --------------------------------------------------------------------------
// zstd
// --------------------------------------------------------------------------

#[cfg(not(target_family = "wasm"))]
#[test]
fn zstd_round_trip() {
    let data = payload(200_000);
    let manifest = b"manifest bytes".to_vec();
    let sp = spec(PgCompressAlgorithm::Zstd, None);
    let got = run_chain(&data, &manifest, Codec::Zstd, &sp);

    assert_eq!(got.archives.len(), 2);
    for (name, bytes) in &got.archives {
        assert!(name.ends_with(".tar.zst"), "archive renamed: {name}");
        assert_eq!(zstd::decode_all(&bytes[..]).expect("zstd decode"), data);
        assert!(bytes.len() < data.len());
    }
    assert_eq!(got.manifest, manifest);
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn zstd_levels_and_long_round_trip() {
    let data = payload(60_000);
    for detail in ["1", "19", "level=-2", "long"] {
        let sp = spec(PgCompressAlgorithm::Zstd, Some(detail));
        let got = run_chain(&data, b"", Codec::Zstd, &sp);
        assert_eq!(
            zstd::decode_all(&got.archives[0].1[..]).expect("zstd decode"),
            data,
            "detail {detail}"
        );
    }
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn zstd_empty_archive() {
    let sp = spec(PgCompressAlgorithm::Zstd, None);
    let got = run_chain(&[], b"", Codec::Zstd, &sp);
    assert_eq!(
        zstd::decode_all(&got.archives[0].1[..]).expect("zstd decode"),
        Vec::<u8>::new()
    );
}
