//! The two-witness null law, born-RED (M3-D slice leg 11; spec §6.6): a
//! seeded stats/popcount skew FAILS the seal (tooth 1), and the honest path
//! witnesses the exact number of cross-checks performed (tooth 2 — the gate
//! provably RAN).

use super::*;
use crate::meta_standin::StandinMetaBuilder;
use crate::publish::TxnVerdict;
use crate::seal::{seal_part, PartSpec, ReferenceResolver};
use crate::WriteError;
use pgrc2_format::abi::{ColumnMetaBuilder, EncodeInput};
use pgrc2_format::meta::StatsRecord;
use pgrc2_format::part::SectionKind;

/// A meta builder that lies about granule 0's nonnull by +1.
struct LyingBuilder {
    inner: StandinMetaBuilder,
}

impl ColumnMetaBuilder for LyingBuilder {
    fn observe_granule(&mut self, input: &EncodeInput<'_>, granule: u32) {
        self.inner.observe_granule(input, granule);
    }
    fn seal_granule(&mut self, granule: u32) -> StatsRecord {
        let mut r = self.inner.seal_granule(granule);
        if granule == 0 {
            r.nonnull += 1; // the seeded skew
        }
        r
    }
    fn seal_band(&mut self, band: u32) -> StatsRecord {
        self.inner.seal_band(band)
    }
    fn seal_part(&mut self) -> StatsRecord {
        self.inner.seal_part()
    }
    fn aux_sections(&mut self) -> Vec<(SectionKind, Vec<u8>)> {
        Vec::new()
    }
}

fn spec() -> PartSpec {
    PartSpec {
        spc: SPC,
        db: DB,
        relfilenumber: RELFILENUMBER,
        schema_fingerprint: 0xABCD,
    }
}

fn one_col(rows: u64) -> Vec<crate::ingest::ColBuffer> {
    let mut c = crate::ingest::ColBuffer::new(int8_col(1));
    for i in 0..rows {
        if i % 7 == 0 {
            c.append_null();
        } else {
            c.append_word(i).expect("word");
        }
    }
    vec![c]
}

#[test]
fn seeded_nonnull_skew_fails_the_seal() {
    let mut vfs = mem_with_dir();
    let cols = one_col(1000);
    let mut builders: Vec<Box<dyn ColumnMetaBuilder>> = vec![Box::new(LyingBuilder {
        inner: StandinMetaBuilder::new(),
    })];
    let err = seal_part(
        &mut vfs,
        DIR,
        &spec(),
        &cols,
        &[],
        &mut builders,
        &[],
        &ReferenceResolver,
        &crate::structural::StructuralPolicy::default(),
        1,
        0,
    )
    .unwrap_err();
    match err {
        WriteError::TwoWitnessSkew {
            attno,
            granule,
            stats_nonnull,
            bitmap_nonnull,
        } => {
            assert_eq!(attno, 1);
            assert_eq!(granule, 0);
            assert_eq!(stats_nonnull, bitmap_nonnull + 1);
        }
        other => panic!("expected TwoWitnessSkew, got {other:?}"),
    }
    // The refused part never reached a file.
    assert!(vfs.list_dir(DIR).expect("list").is_empty());
}

/// Tooth 2: the honest seal performed EXACTLY (granules + bands + 1) checks
/// per stream — the gate cannot be silently skipped.
#[test]
fn honest_seal_witnesses_exact_crosscheck_count() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let schema = vec![int8_col(1), text_col(2)];
    let mut w = open_writer(schema, stamp(33, 1));
    for i in 0..70_000u64 {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let img = img_4b_u(format!("t{i}").as_bytes());
        let d1 = if i % 3 == 0 {
            RawDatum::Null
        } else {
            RawDatum::Word(i)
        };
        w.append_row(&[d1, RawDatum::Bytes(&img)], &mut kit.ext, &mut env)
            .expect("append");
    }
    let probe = Probe::new(TxnVerdict::InProgress).set(33, TxnVerdict::Committed);
    finish_and_publish(&mut w, &mut vfs, &mut kit, &probe);
    let r = &w.seal_reports()[0];
    // 70k rows: 9 granules, 2 bands; 2 streams.
    let per_stream = 9 + 2 + 1;
    assert_eq!(r.nonnull_crosschecks, 2 * per_stream);
    assert_eq!(r.granules_verified, 2 * 9);
}
