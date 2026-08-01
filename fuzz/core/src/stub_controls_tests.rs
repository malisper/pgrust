//! MUST-FAIL CONTROLS for the stub:* constructed-state builders
//! (constructor-audit discipline): a builder bug fabricates agreement —
//! both sides consume the same wrong structure and the differential is
//! blind. Each control below (a) establishes the baseline agreement and
//! then (b) plants a deliberate ONE-SIDE-ONLY construction difference and
//! asserts the differential plane actually catches it. If a control's
//! `assert_ne!` ever fails, the plane is dead — fix the plane, never the
//! control.
//!
//! The builder injection sweep (planted defects in scratch copies of the
//! builders themselves; results in fuzz/STUBS.md) leans on these controls
//! plus the demo targets' seed-replay structural validators.

use crate::stub_encoding;
use crate::stub_snapshot::{
    build_rust_snapshot, c_snapshot_plane, decode_snap, ser_snapshot_plane, snap_wire, SnapSpec,
};
use crate::stub_tupdesc::{decode_desc, spec_wire, Cursor};

// -------------------------------------------------------------------------
// stub:tupdesc
// -------------------------------------------------------------------------

/// Baseline: identical spec on both sides agrees. Control: flip attnotnull
/// of att 0 in the WIRE ONLY (C constructs a different descriptor than the
/// Rust side) — the descriptor field plane must diverge.
#[test]
fn tupdesc_control_one_side_notnull_flip_is_caught() {
    let _serial = crate::c_oracle_serial();
    if !crate::tupaccess_diff::control_install() {
        return; // sibling module owns the seams in this test process
    }
    // natts=2, no constr; att0 = menu2(int4) notnull, att1 = menu0(char)
    let bytes = [2u8, 0, 2, 0x02, 0, 0, 0, 0, 1, 0];
    let mut cur = Cursor { b: &bytes, i: 0 };
    let spec = decode_desc(&mut cur);
    let wire = spec_wire(&spec);

    let (r, c) = crate::tupaccess_diff::desc_control_planes(&wire, &spec);
    assert_eq!(r, c, "tupdesc control baseline must agree");

    // wire layout: [natts][dflags][tdtypmod x4][att0: menu,aflags,...]
    let mut bad = wire.clone();
    assert_eq!(bad[7], 0x02, "control expects att0 aflags at wire[7]");
    bad[7] ^= 0x02;
    let (r2, c2) = crate::tupaccess_diff::desc_control_planes(&bad, &spec);
    assert_ne!(r2, c2, "differential is BLIND to a one-side notnull flip");
}

/// Control: change att0's MENU on the C side only (different attlen/byval)
/// — a shape-level construction difference must diverge too.
#[test]
fn tupdesc_control_one_side_menu_swap_is_caught() {
    let _serial = crate::c_oracle_serial();
    if !crate::tupaccess_diff::control_install() {
        return;
    }
    let bytes = [1u8, 0, 2, 0, 0, 0];
    let mut cur = Cursor { b: &bytes, i: 0 };
    let spec = decode_desc(&mut cur);
    let wire = spec_wire(&spec);
    let (r, c) = crate::tupaccess_diff::desc_control_planes(&wire, &spec);
    assert_eq!(r, c, "tupdesc control baseline must agree");
    let mut bad = wire.clone();
    assert_eq!(bad[6], 2, "control expects att0 menu at wire[6]");
    bad[6] = 3; // int4 -> int8 menu entry on the C side only
    let (r2, c2) = crate::tupaccess_diff::desc_control_planes(&bad, &spec);
    assert_ne!(r2, c2, "differential is BLIND to a one-side menu swap");
}

// -------------------------------------------------------------------------
// stub:nodes
// -------------------------------------------------------------------------

/// The nodes builder bridges through the TEXT plane: Rust builds the tree,
/// C reads its rendering. Baseline: C's re-out of the rendered tree is
/// byte-identical. Control: hand C a text describing a DIFFERENT tree — the
/// re-out plane must see it. (The blind class for this builder — a builder
/// bug producing a different-but-legal tree consumed identically by both
/// sides — is documented in fuzz/STUBS.md; it shrinks the surface, never
/// falsifies a verdict.)
#[test]
fn nodes_control_one_side_tree_difference_is_caught() {
    let _serial = crate::c_oracle_serial();
    let cx = mcx::MemoryContext::new("stub_nodes_control");
    let m = cx.mcx();
    // sel=1 => Integer, ival = 7 (LE)
    let node = crate::stub_nodes::build_value_node(m, &[1, 7, 0, 0, 0]).expect("builder");
    let out = outfuncs::nodeToString(m, node).expect("out");
    assert!(out.as_str().contains('7'), "expected the Integer literal in {:?}", out.as_str());

    let re = crate::nodesfam_diff::c_reout_control(out.as_str().as_bytes()).expect("C read");
    assert_eq!(re.as_slice(), out.as_str().as_bytes(), "nodes control baseline must agree");

    let tampered = out.as_str().replace('7', "8");
    let re2 = crate::nodesfam_diff::c_reout_control(tampered.as_bytes()).expect("C read");
    assert_ne!(
        re2.as_slice(),
        out.as_str().as_bytes(),
        "re-out plane is BLIND to a one-side tree difference"
    );
}

// -------------------------------------------------------------------------
// stub:snapshot
// -------------------------------------------------------------------------

fn sample_spec() -> SnapSpec {
    // xcnt=2, subxcnt=1, deliberately un-ordered xids (the builder must not
    // normalize them)
    let bytes: Vec<u8> = [
        &[0u8][..],                    // type = MVCC
        &100u32.to_le_bytes()[..],     // xmin
        &90u32.to_le_bytes()[..],      // xmax < xmin on purpose
        &[2u8][..],
        &7u32.to_le_bytes()[..],
        &0xffff_fff0u32.to_le_bytes()[..],
        &[1u8][..],
        &55u32.to_le_bytes()[..],
        &[0x03u8][..],                 // suboverflowed + takenDuringRecovery
        &9u32.to_le_bytes()[..],       // curcid
        &0xdeadbeefu32.to_le_bytes()[..],
        &0x0102030405060708u64.to_le_bytes()[..],
    ]
    .concat();
    let mut cur = Cursor { b: &bytes, i: 0 };
    decode_snap(&mut cur)
}

/// Baseline agreement + mini-fuzz over seeded pseudo-random specs.
#[test]
fn snapshot_construction_agrees() {
    let cx = mcx::MemoryContext::new("stub_snapshot_test");
    let m = cx.mcx();
    crate::stub_snapshot::assert_snapshot_construction_agrees(m, &sample_spec());

    // xorshift64 byte stream: 500 random specs through both constructors
    let mut s: u64 = 0x9e3779b97f4a7c15;
    for _ in 0..500 {
        let mut bytes = Vec::with_capacity(600);
        for _ in 0..75 {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            bytes.extend_from_slice(&s.to_le_bytes());
        }
        let mut cur = Cursor { b: &bytes, i: 0 };
        let spec = decode_snap(&mut cur);
        crate::stub_snapshot::assert_snapshot_construction_agrees(m, &spec);
    }
}

/// Control: perturb the WIRE ONLY (C constructs from different bytes than
/// the Rust side's spec) — the field plane must diverge.
#[test]
fn snapshot_control_c_side_tamper_is_caught() {
    let cx = mcx::MemoryContext::new("stub_snapshot_control");
    let m = cx.mcx();
    let spec = sample_spec();
    let wire = snap_wire(&spec);

    let snap = build_rust_snapshot(m, &spec);
    let mut rplane = Vec::new();
    ser_snapshot_plane(&mut rplane, &snap);
    assert_eq!(rplane, c_snapshot_plane(&wire), "snapshot control baseline must agree");

    // wire[5..9] = xmax; flip its low byte on the C side only
    let mut bad = wire.clone();
    bad[5] ^= 1;
    assert_ne!(
        rplane,
        c_snapshot_plane(&bad),
        "field plane is BLIND to a one-side xmax flip"
    );
}

/// Control: perturb the RUST side only (xip[0]) — the same plane must
/// diverge in the other direction.
#[test]
fn snapshot_control_rust_side_tamper_is_caught() {
    let cx = mcx::MemoryContext::new("stub_snapshot_control2");
    let m = cx.mcx();
    let spec = sample_spec();
    let wire = snap_wire(&spec);

    let mut spec2 = spec.clone();
    spec2.xip[0] ^= 1;
    let snap2 = build_rust_snapshot(m, &spec2);
    let mut rplane2 = Vec::new();
    ser_snapshot_plane(&mut rplane2, &snap2);
    assert_ne!(
        rplane2,
        c_snapshot_plane(&wire),
        "field plane is BLIND to a one-side xip flip"
    );
}

// -------------------------------------------------------------------------
// stub:encoding
// -------------------------------------------------------------------------

/// The pin itself: every row of the encoding tables agrees.
#[test]
fn encoding_tables_are_pinned() {
    stub_encoding::assert_encoding_tables_pinned();
}

/// Control: compare each Rust row against the WRONG C row (index+1) — the
/// comparator must see every one (official names are all distinct).
#[test]
fn encoding_control_shifted_index_is_caught() {
    for e in 0..stub_encoding::N_ENCODINGS {
        let r = stub_encoding::rust_row(e);
        let mut c = stub_encoding::c_row((e + 1) % stub_encoding::N_ENCODINGS);
        c.enc = e; // isolate the TABLE columns; the index field is not the plane
        assert_ne!(r, c, "encoding comparator is BLIND at shifted index {e}");
    }
}

/// The enc_from_byte clamp is part of the compared-input contract: pin its
/// exact arithmetic so a clamp change is a caught contract change, not a
/// silent drift both sides agree on.
#[test]
fn encoding_clamp_is_pinned() {
    assert_eq!(stub_encoding::enc_from_byte(0), 0);
    assert_eq!(stub_encoding::enc_from_byte(41), 41);
    assert_eq!(stub_encoding::enc_from_byte(42), 0);
    assert_eq!(stub_encoding::enc_from_byte(255), 255 % 42);
}
