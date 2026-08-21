//! Declared-cluster-key sorted ingest (IN-1 / FT-6 / OD-8, M3-L2).
//!
//! The laws under test:
//! - a DECLARED key sorts natively at seal: for a unique total key, a
//!   shuffled feed and a presorted feed of the same multiset seal
//!   BYTE-IDENTICAL parts, and the part carries the FT-6 clustered witness
//!   (spec-§9 SortKey section, nkeys>0, + the SealReport leg);
//! - the sort is STABLE with PG null placement: nulls go where declared
//!   (NULLS LAST here) and equal-key/null groups keep ARRIVAL order — the
//!   sealed bytes are a pure function of (input sequence, declared key),
//!   predicted exactly by a stable partition oracle;
//! - an UNDECLARED table is legal and unlicensed (OD-8): nkeys = 0, no
//!   witness — mechanisms simply don't license;
//! - a key whose stored form is not its sort order refuses TYPED (a wrong
//!   order would silently poison every witness-gated consumer).

use super::*;
use crate::structural::{ClusterKeyDecl, NullsOrder, SortDir, StructuralPolicy};
use pgrc2_format::sortkey::SortKeyRecord;

const FXID: u64 = 77;

fn asc_key(attno: u32) -> ClusterKeyDecl {
    ClusterKeyDecl {
        attno,
        dir: SortDir::Asc,
        nulls: NullsOrder::Last,
    }
}

/// Seal one part of int8+text mixed rows fed in `order`, with (optionally)
/// a declared cluster key on the int8 column; return (tmp bytes, report).
fn seal_with_order(
    order: &[u64],
    key: Option<ClusterKeyDecl>,
) -> (Vec<u8>, crate::seal::SealReport) {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer_policy(
        vec![int8_col(1), text_col(2)],
        stamp(FXID, 1),
        PartCutPolicy {
            max_rows: u64::MAX,
            max_bytes: u64::MAX,
            cut_granule_rows: 128,
        },
    );
    if let Some(k) = key {
        w.set_structural(StructuralPolicy::new().with_cluster_key(vec![k]));
    }
    for &i in order {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        with_mixed_row(i, |row| w.append_row(row, &mut kit.ext, &mut env).expect("append"));
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    assert_eq!(w.sealed_parts().len(), 1, "single-part fixture");
    let tmp = &w.sealed_parts()[0].tmp_name;
    let bytes = vfs.read_full(&format!("{DIR}/{tmp}")).expect("tmp bytes");
    (bytes, w.seal_reports()[0].clone())
}

/// Deterministic shuffle (LCG, the in-suite idiom).
fn shuffle_of(mut v: Vec<u64>, seed: u64) -> Vec<u64> {
    let mut state = seed;
    for i in (1..v.len()).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (state >> 33) as usize % (i + 1);
        v.swap(i, j);
    }
    v
}

/// Row ids whose `mixed_row` int8 key is NEVER null and strictly ascends:
/// `mixed_row(i)` nulls the key iff `i % 13 == 0`, so `i = 13k + 1` avoids
/// every null while keeping keys unique and monotone in `k`.
fn unique_key_rows(n: u64) -> Vec<u64> {
    (0..n).map(|k| 13 * k + 1).collect()
}

fn view_of(bytes: &[u8]) -> PartView {
    PartView {
        bytes: bytes.to_vec(),
    }
}

#[test]
fn declared_key_sorts_unsorted_input_and_stamps_the_witness() {
    let n = 500u64;
    let presorted = unique_key_rows(n);
    let shuffled = shuffle_of(presorted.clone(), 0xC1D5_7E11_0234_9F5B);
    let (shuffled_bytes, shuffled_rep) = seal_with_order(&shuffled, Some(asc_key(1)));
    let (sorted_bytes, sorted_rep) = seal_with_order(&presorted, Some(asc_key(1)));
    // The sorted-ingest law (unique total key): sealed bytes are a pure
    // function of (multiset, declared key) — arrival order is erased.
    assert_eq!(
        shuffled_bytes, sorted_bytes,
        "shuffled and presorted feeds must seal byte-identical parts"
    );
    // Witness, report leg.
    assert_eq!(shuffled_rep.cluster_keys, vec![1]);
    assert_eq!(sorted_rep.cluster_keys, vec![1]);
    // Witness, on-disk leg: spec-§9 SortKey with the declared entry.
    let pv = view_of(&shuffled_bytes);
    let body = pv
        .section_bytes(SectionKind::SortKey, 0, 0)
        .expect("SortKey section present");
    let rec = SortKeyRecord::decode(&body).expect("well-formed");
    assert_eq!(rec.keys.len(), 1);
    assert_eq!(rec.keys[0].attno, 1);
    assert_eq!(rec.keys[0].dir, SortDir::Asc as u8);
    assert_eq!(rec.keys[0].nulls, NullsOrder::Last as u8);
    // O-10: the logical multiset identity is order-blind — the sort must
    // not change the column digests vs an UNDECLARED seal of the same
    // multiset.
    let (_, undeclared_rep) = seal_with_order(&shuffled, None);
    assert_eq!(shuffled_rep.col_hashes, undeclared_rep.col_hashes);
}

/// Stable sort + PG null placement, predicted EXACTLY: sealing a shuffled
/// feed (null keys included) equals sealing the stable-partition oracle of
/// that same feed — non-null keys ascending (unique, so total), then the
/// null-key rows in THEIR ARRIVAL ORDER (NULLS LAST; stability keeps the
/// suborder). This is the honest form of "arrival order is erased": it is
/// erased exactly up to the declared key, never beyond it.
#[test]
fn nulls_last_and_stability_match_the_partition_oracle() {
    let n = 400u64;
    let arrivals = shuffle_of((0..n).collect(), 0x5EED_0BAD_F00D_1234);
    let mut oracle: Vec<u64> = arrivals.iter().copied().filter(|i| i % 13 != 0).collect();
    oracle.sort_by_key(|&i| i as i64 - 5000); // the mixed_row int8 key
    oracle.extend(arrivals.iter().copied().filter(|i| i % 13 == 0));
    let (got, rep) = seal_with_order(&arrivals, Some(asc_key(1)));
    let (want, _) = seal_with_order(&oracle, Some(asc_key(1)));
    assert_eq!(got, want, "stable NULLS LAST partition oracle diverged");
    assert_eq!(rep.cluster_keys, vec![1]);
}

#[test]
fn undeclared_table_stays_unordered_nkeys_zero() {
    let arrivals = shuffle_of((0..300).collect(), 0xA5A5_5A5A_1111_2222);
    let (bytes, rep) = seal_with_order(&arrivals, None);
    assert!(rep.cluster_keys.is_empty());
    let pv = view_of(&bytes);
    let body = pv
        .section_bytes(SectionKind::SortKey, 0, 0)
        .expect("SortKey section always present");
    let rec = SortKeyRecord::decode(&body).expect("well-formed");
    assert!(rec.keys.is_empty(), "OD-8: undeclared = unordered part");
}

#[test]
fn non_byte_ordered_collation_key_refuses_typed() {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    // A text column under a non-C deterministic collation: strcmp is NOT
    // its sort order — the declared key must refuse, not mis-sort.
    let mut col = text_col(1);
    col.collation_class = CollationClass::OtherDeterministic;
    let mut w = open_writer_policy(vec![col], stamp(FXID, 1), PartCutPolicy::default());
    w.set_structural(StructuralPolicy::new().with_cluster_key(vec![asc_key(1)]));
    let imgs = [img_4b_u(b"zz"), img_4b_u(b"aa")];
    for image in &imgs {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.append_row(&[RawDatum::Bytes(image)], &mut kit.ext, &mut env)
            .expect("append buffers only");
    }
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs: &mut vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    let err = w.finish(&mut env).expect_err("must refuse at seal");
    assert!(
        matches!(err, crate::WriteError::Refused { what } if what.contains("cluster key")),
        "typed refusal expected, got {err:?}"
    );
}

#[test]
fn descending_key_seals_descending() {
    let n = 200u64;
    let desc = ClusterKeyDecl {
        attno: 1,
        dir: SortDir::Desc,
        nulls: NullsOrder::Last,
    };
    let mut presorted_desc = unique_key_rows(n);
    presorted_desc.reverse();
    let shuffled = shuffle_of(unique_key_rows(n), 0x0DDB_A11_5EED_77);
    assert_eq!(
        seal_with_order(&shuffled, Some(desc)).0,
        seal_with_order(&presorted_desc, Some(desc)).0,
        "desc-declared shuffled feed must equal the desc-presorted feed"
    );
}
