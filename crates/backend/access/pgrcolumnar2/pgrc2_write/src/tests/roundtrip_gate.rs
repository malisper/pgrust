//! `verify_roundtrip` on the seal path, born-RED (the M3-D non-negotiable):
//! a seeded encode corruption is CAUGHT before any file is written (tooth
//! 1), and the honest path witnesses one verify per granule per value
//! stream (tooth 2).

use super::*;
use crate::elect::{Candidate, CandidateSource, EncoderFactory, FullElectInput};
use crate::publish::TxnVerdict;
use crate::WriteError;
use pgrc2_format::abi::{EncodeInput, GranuleEncoder, KernelKey};
use pgrc2_format::class::StorageClass;
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::verbatim::ConstEncoder;

/// Delegates to the real CONST encoder, then corrupts one payload byte —
/// the seeded defect verify_roundtrip must catch.
struct CorruptingEncoder {
    inner: ConstEncoder,
}

impl GranuleEncoder for CorruptingEncoder {
    fn key(&self) -> KernelKey {
        self.inner.key()
    }
    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        ovf: &mut OverflowSink<'_>,
    ) -> pgrc2_format::FormatResult<()> {
        self.inner.encode_granule(input, w, ovf)?;
        // Flip the last written payload byte (the CONST record's value).
        let buf = w.payload();
        if let Some(last) = buf.last_mut() {
            *last ^= 0xFF;
        }
        Ok(())
    }
    fn finish_stream(
        &mut self,
        w: &mut StreamSectionWriter<'_>,
    ) -> pgrc2_format::FormatResult<()> {
        self.inner.finish_stream(w)
    }
}

struct CorruptingFactory {
    class: StorageClass,
}

impl EncoderFactory for CorruptingFactory {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: pgrc2_format::enc::EncodingId::Const.as_u16(),
            class: self.class.id(),
            width: self.class.width(),
        }
    }
    fn make(&self) -> Box<dyn GranuleEncoder> {
        Box::new(CorruptingEncoder {
            inner: ConstEncoder::new(self.class),
        })
    }
}

struct HostileSource;

impl CandidateSource for HostileSource {
    fn propose(&self, input: &FullElectInput<'_>) -> Vec<Candidate> {
        let stats = input.stats;
        vec![Candidate {
            factory: Box::new(CorruptingFactory { class: stats.class }),
            encoded_len: 1, // always wins the gate
        }]
    }
}

#[test]
fn seeded_encode_corruption_is_caught_before_any_file_write() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1)], stamp(44, 1));
    // Constant column so CONST is a plausible winner.
    for i in 0..100u64 {
        let hostile = HostileSource;
        let sources: [&dyn CandidateSource; 1] = [&hostile];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let r = w.append_row(&[RawDatum::Word(7)], &mut kit.ext, &mut env);
        r.expect("append never seals here");
        let _ = i;
    }
    let hostile = HostileSource;
    let sources: [&dyn CandidateSource; 1] = [&hostile];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    let err = w.finish(&mut env).unwrap_err();
    match err {
        WriteError::RoundTrip {
            attno,
            path_ord,
            granule,
            ..
        } => {
            assert_eq!((attno, path_ord, granule), (1, 0, 0));
        }
        other => panic!("expected RoundTrip, got {other:?}"),
    }
    // The corrupted part never reached a file.
    assert!(vfs.list_dir(DIR).expect("list").is_empty());
}

/// Tooth 2: the honest path verifies EVERY granule of EVERY value stream —
/// the count is exact, so a silently-skipped verify cannot pass.
#[test]
fn honest_seal_verifies_every_granule() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer(vec![int8_col(1), text_col(2)], stamp(45, 1));
    for i in 0..20_000u64 {
        let sources: [&dyn CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let img = img_4b_u(format!("x{}", i % 100).as_bytes());
        w.append_row(&[RawDatum::Word(i), RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(45, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    // 20k rows = 3 granules; 2 value streams.
    assert_eq!(w.seal_reports()[0].granules_verified, 2 * 3);
}
