//! Join family admission (election rules = phase-2 recognizer work).

use crate::bank::{Bank, Face};
use crate::ir::PredSpec;
use crate::joins::ir::*;
use crate::typmeta::COLLATION_C;

/// [sqe-join-depth] Dimension-stage cap: the generalized product walk
/// serves N staged builds (chained keys allowed, `DimSrc::Dim(j)` with
/// j < i); six stages = an 8-way tree, the identity-tested ceiling.
pub const MAX_DIM_STAGES: usize = 6;

/// [sqe-mech3] Composite group-key cap (multi-key grouped fold).
pub const MAX_GROUP_KEYS: usize = 8;

/// [crossdim-or] Staged-disjunction arm cap (mask bits are u64-cheap;
/// the cap is the identity-tested ceiling).
pub const MAX_OR_ARMS: usize = 8;

/// [semianti-flt] Membership-filter stage cap.
pub const MAX_FILTER_STAGES: usize = 4;

/// Varlena side-pred admission: full-decode payload faces under the
/// byte-order collation class only (the per-side single-relation law).
fn check_var_terms(b: &Bank, p: &PredSpec) -> Result<(), JoinRefuse> {
    for t in &p.var_terms {
        if !matches!(b.face(t.col), Face::Varlena) {
            return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
        }
        let ty = b.typ(t.col);
        if ty.collation != COLLATION_C {
            return Err(JoinRefuse::Collation { attno: t.col, collation: ty.collation });
        }
    }
    Ok(())
}

/// Float equality has no exact i64-embed law (-0/NaN) — not word-embed.
fn word_embed(b: &Bank, c: u32) -> bool {
    matches!(b.face(c), Face::SignedWord(_) | Face::UnsignedWord(1..=4) | Face::Bool)
}

/// [packednum] Fold-input word embed: the agg vocabulary additionally
/// admits the PackedNumeric mantissa lane (exact scaled ints — the same
/// i128 sum / order law). Join KEYS, quals and side-preds keep the
/// narrower `word_embed` (no numeric predicate/key vocabulary).
fn agg_word_embed(b: &Bank, c: u32) -> bool {
    word_embed(b, c) || matches!(b.face(c), Face::PackedNumeric { .. })
}

/// The bank a `JoinOut`/side reference resolves against.
fn side_bank<'a>(
    build: &'a Bank,
    probe: &'a Bank,
    dim_banks: &[&'a Bank],
    side: JoinSide,
) -> Result<&'a Bank, JoinRefuse> {
    Ok(match side {
        JoinSide::Build => build,
        JoinSide::Probe => probe,
        JoinSide::Dim(i) => dim_banks
            .get(i as usize)
            .copied()
            .ok_or(JoinRefuse::Unsupported { what: "dim-stage-index" })?,
    })
}

/// Validate + construct a `JoinNode`. Fail-closed: shapes outside the
/// phase-1 vocabulary are typed refusals, never panics downstream.
/// [sqe-mech3] `dim_banks` aligns with `dims` (the dimension stages'
/// banks); both empty = the 2-way family unchanged.
#[allow(clippy::too_many_arguments)]
pub fn join_node(
    build: &Bank,
    probe: &Bank,
    dim_banks: &[&Bank],
    q: u32,
    join_type: JoinType,
    keys: Vec<JoinKey>,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    dims: Vec<DimStage>,
    out: Vec<JoinOut>,
    build_rows_hint: u64,
    packed_word_outs: bool,
) -> Result<JoinNode, JoinRefuse> {
    if keys.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "no-equi-key" });
    }
    mk_join_node(
        build, probe, dim_banks, q, join_type, keys, quals, build_pred, probe_pred, dims, out,
        build_rows_hint, packed_word_outs,
    )
}

/// P4-5: validate + construct the KEYLESS `JoinNode` (nest-loop stencil):
/// no equi-key lanes; `quals` (possibly empty — a witnessed cross
/// product) decide matched-ness per pair, Eq allowed. No dim stages
/// (keyless v1 is strictly 2-way). Each side must plan at least one
/// decode column (the granule walk needs a lane), and the runner
/// witness-gates the build x probe pair product.
#[allow(clippy::too_many_arguments)]
pub fn nest_loop_node(
    build: &Bank,
    probe: &Bank,
    q: u32,
    join_type: JoinType,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    out: Vec<JoinOut>,
    build_rows_hint: u64,
) -> Result<JoinNode, JoinRefuse> {
    check_nl_side_cols(&quals, &build_pred, &probe_pred, &out)?;
    // Row goal: no scaled render — PackedNumeric outs refuse (same law
    // as the keyed row goal).
    mk_join_node(
        build,
        probe,
        &[],
        q,
        join_type,
        Vec::new(),
        quals,
        build_pred,
        probe_pred,
        Vec::new(),
        out,
        build_rows_hint,
        false,
    )
}

/// Each keyless side must plan >= 1 column: the runner's granule walk
/// and the probe loop both need a decode lane per side. Dim-side refs
/// cannot occur (keyless nodes carry no dim stages).
fn check_nl_side_cols(
    quals: &[JoinQual],
    build_pred: &Option<PredSpec>,
    probe_pred: &Option<PredSpec>,
    out: &[JoinOut],
) -> Result<(), JoinRefuse> {
    if out.iter().any(|o| matches!(o.side, JoinSide::Dim(_))) {
        return Err(JoinRefuse::Unsupported { what: "dim-stage-index" });
    }
    let side_has = |side: JoinSide, pred: &Option<PredSpec>| {
        !quals.is_empty()
            || pred.as_ref().is_some_and(|p| {
                !p.terms.is_empty() || !p.col_terms.is_empty() || !p.var_terms.is_empty()
            })
            || out.iter().any(|o| o.side == side)
    };
    if !side_has(JoinSide::Build, build_pred) || !side_has(JoinSide::Probe, probe_pred) {
        return Err(JoinRefuse::Unsupported { what: "nl-side-no-cols" });
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn mk_join_node(
    build: &Bank,
    probe: &Bank,
    dim_banks: &[&Bank],
    q: u32,
    join_type: JoinType,
    keys: Vec<JoinKey>,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    dims: Vec<DimStage>,
    out: Vec<JoinOut>,
    build_rows_hint: u64,
    packed_word_outs: bool,
) -> Result<JoinNode, JoinRefuse> {
    if !keys.is_empty() && quals.iter().any(|q| q.op == JoinCmp::Eq) {
        // Hashable equalities live in the key lanes; a residual Eq qual
        // belongs to the keyless (nest-loop) vocabulary only.
        return Err(JoinRefuse::Unsupported { what: "keyed-eq-qual" });
    }
    if out.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "no-out-cols" });
    }
    if dims.len() != dim_banks.len() || dims.len() > MAX_DIM_STAGES {
        return Err(JoinRefuse::Unsupported { what: "dim-stage-depth" });
    }
    let ntext = keys.iter().filter(|k| build.typ(k.build_col).is_varlena()).count();
    let cap = crate::joins::hash_join::MAX_KEY_LANES;
    if ntext > cap || keys.len() - ntext > cap {
        return Err(JoinRefuse::Unsupported { what: "too-many-key-lanes" });
    }
    let word_ok = word_embed;
    for k in &keys {
        let bt = build.typ(k.build_col);
        let pt = probe.typ(k.probe_col);
        match (bt.is_varlena(), pt.is_varlena()) {
            (true, true) => {
                for (c, t) in [(k.build_col, bt), (k.probe_col, pt)] {
                    if t.collation != COLLATION_C {
                        return Err(JoinRefuse::Collation { attno: c, collation: t.collation });
                    }
                }
            }
            (false, false) => {
                if !word_ok(build, k.build_col) || !word_ok(probe, k.probe_col) {
                    return Err(JoinRefuse::Unsupported { what: "key-face-no-word-embed" });
                }
            }
            _ => return Err(JoinRefuse::Unsupported { what: "key-type-mix" }),
        }
    }
    for qq in &quals {
        if build.typ(qq.build_col).is_varlena()
            || probe.typ(qq.probe_col).is_varlena()
            || !word_ok(build, qq.build_col)
            || !word_ok(probe, qq.probe_col)
        {
            return Err(JoinRefuse::Unsupported { what: "qual-face-no-word-embed" });
        }
    }
    for (b, p) in [(build, &build_pred), (probe, &probe_pred)] {
        if let Some(p) = p {
            for t in &p.terms {
                if !word_ok(b, t.col) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
            check_var_terms(b, p)?;
            // [colcmp] both operands word lanes of this side.
            for t in &p.col_terms {
                if !word_ok(b, t.a) || !word_ok(b, t.b) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
        }
    }
    // [sqe-mech3] dimension-stage admission: word equi-keys onto the
    // BUILD side; word pred conjuncts; C-collation byte-eq text
    // conjuncts. (Dim payload columns are validated with `out` below.)
    for (di, d) in dims.iter().enumerate() {
        let db = dim_banks[di];
        if d.keys.is_empty() {
            return Err(JoinRefuse::Unsupported { what: "dim-no-equi-key" });
        }
        if d.keys.len() > cap {
            return Err(JoinRefuse::Unsupported { what: "too-many-key-lanes" });
        }
        for k in &d.keys {
            // [sqe-join-depth] the key's non-dim side resolves against
            // its source bank: Build, or an EARLIER dim stage (chained).
            let sb = match k.src {
                DimSrc::Build => build,
                DimSrc::Dim(j) => {
                    if j as usize >= di {
                        return Err(JoinRefuse::Unsupported { what: "dim-chain-order" });
                    }
                    dim_banks[j as usize]
                }
            };
            if db.typ(k.dim_col).is_varlena()
                || sb.typ(k.build_col).is_varlena()
                || !word_ok(db, k.dim_col)
                || !word_ok(sb, k.build_col)
            {
                return Err(JoinRefuse::Unsupported { what: "dim-key-face" });
            }
        }
        if let Some(p) = &d.pred {
            check_var_terms(db, p)?;
            for t in &p.terms {
                if !word_ok(db, t.col) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
            for t in &p.col_terms {
                if !word_ok(db, t.a) || !word_ok(db, t.b) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
        }
        for t in &d.text_eqs {
            let ty = db.typ(t.col);
            if !ty.is_varlena() {
                return Err(JoinRefuse::Unsupported { what: "dim-text-eq-face" });
            }
            if ty.collation != COLLATION_C {
                return Err(JoinRefuse::Collation { attno: t.col, collation: ty.collation });
            }
        }
    }
    if matches!(join_type, JoinType::Semi | JoinType::Anti)
        && out.iter().any(|o| o.side != JoinSide::Probe)
    {
        return Err(JoinRefuse::Unsupported { what: "semi-anti-build-out" });
    }
    // [sqe-semi-anti] right classes emit build entries (match-flag law):
    // probe columns never appear on an emitted row.
    if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti)
        && out.iter().any(|o| o.side != JoinSide::Build)
    {
        return Err(JoinRefuse::Unsupported { what: "right-semi-anti-probe-out" });
    }
    if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti | JoinType::Right)
        && keys.is_empty()
    {
        // Keyless (nest-loop) right classes are outside the vocabulary.
        return Err(JoinRefuse::Unsupported { what: "right-join-keyless" });
    }
    // [rightouter] no swept null row exists for staged dims.
    if join_type == JoinType::Right && !dims.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "right-join-dims" });
    }
    let mut out_tys = Vec::with_capacity(out.len());
    let mut out_bytes = Vec::with_capacity(out.len());
    for o in &out {
        let b = side_bank(build, probe, dim_banks, o.side)?;
        let t = b.typ(o.col);
        // [packednum] a PackedNumeric out is a WORD lane (mantissas),
        // admitted only where the consumer re-attaches the scale (the
        // agg fold sinks — `packed_word_outs`); the row goal has no
        // scaled render and refuses.
        let packed = matches!(b.face(o.col), Face::PackedNumeric { .. });
        if packed && !packed_word_outs {
            return Err(JoinRefuse::Unsupported { what: "out-face-packed-numeric" });
        }
        if !packed && !t.is_varlena() && !word_ok(b, o.col) {
            return Err(JoinRefuse::Unsupported { what: "out-face-no-word-embed" });
        }
        out_bytes.push(!packed && t.is_varlena());
        out_tys.push(t);
    }
    Ok(JoinNode {
        q,
        join_type,
        keys,
        quals,
        build_pred,
        probe_pred,
        build_in: None,
        probe_in: None,
        build_fold: None,
        num_fold: None,
        staged_or: None,
        filters: Vec::new(),
        dims,
        out,
        out_tys,
        out_bytes,
        build_rows_hint,
        build_budget_bytes: crate::joins::hash_join::join_build_budget_bytes(),
        l2_bytes: 512 * 1024,
    })
}

/// [corrsubq] Attach the grouped-build collapse to a constructed keyed
/// node (row or agg goal — the agg goal passes its inner `join`). The
/// node must be two-way Semi/Left over a non-empty key set; `fold.col`
/// must already own a build payload lane (a qual's build column or a
/// Build out) of a word-embed face; Sum folds are admitted for word
/// widths <= 4 only (an i64 cell then cannot overflow across a u32-
/// indexed table); `missing` is the count ops' law only. `eq_qual`
/// (`probe = build.col`) is the one keyed-Eq residual the fold makes
/// exact — it reads the collapsed cell, never the key lanes.
pub fn attach_build_fold(
    node: &mut JoinNode,
    build: &Bank,
    probe: &Bank,
    fold: BuildFold,
    eq_qual: Option<JoinQual>,
) -> Result<(), JoinRefuse> {
    if node.keys.is_empty()
        || !node.dims.is_empty()
        || node.num_fold.is_some()
        || node.staged_or.is_some()
        || !node.filters.is_empty()
    {
        return Err(JoinRefuse::Unsupported { what: "build-fold-shape" });
    }
    if !matches!(node.join_type, JoinType::Semi | JoinType::Left) {
        return Err(JoinRefuse::Unsupported { what: "build-fold-join-type" });
    }
    if let Some(q) = eq_qual {
        if q.op != JoinCmp::Eq
            || q.build_col != fold.col
            || build.typ(q.build_col).is_varlena()
            || probe.typ(q.probe_col).is_varlena()
            || !word_embed(build, q.build_col)
            || !word_embed(probe, q.probe_col)
        {
            return Err(JoinRefuse::Unsupported { what: "build-fold-eq-qual" });
        }
        node.quals.push(q);
    }
    let has_lane = node.quals.iter().any(|q| q.build_col == fold.col)
        || node.out.iter().any(|o| o.side == JoinSide::Build && o.col == fold.col);
    if !has_lane || build.typ(fold.col).is_varlena() || !word_embed(build, fold.col) {
        return Err(JoinRefuse::Unsupported { what: "build-fold-col" });
    }
    match fold.op {
        JoinAggOp::CountStar | JoinAggOp::CountCol | JoinAggOp::Min | JoinAggOp::Max => {}
        JoinAggOp::CountDistinct => {
            return Err(JoinRefuse::Unsupported { what: "build-fold-op" });
        }
        JoinAggOp::Sum => {
            let narrow = matches!(
                build.face(fold.col),
                Face::SignedWord(1..=4) | Face::UnsignedWord(1..=4)
            );
            if !narrow {
                return Err(JoinRefuse::Unsupported { what: "build-fold-sum-width" });
            }
        }
    }
    if fold.missing.is_some() && !matches!(fold.op, JoinAggOp::CountStar | JoinAggOp::CountCol) {
        return Err(JoinRefuse::Unsupported { what: "build-fold-missing" });
    }
    node.build_fold = Some(fold);
    Ok(())
}

/// [corrnumcell] Attach the numeric collapse: two-way Semi, exclusive
/// with the word fold; lanes re-proven against FACES, scales capped.
pub fn attach_num_fold(
    node: &mut JoinNode,
    build: &Bank,
    probe: &Bank,
    nf: NumFold,
) -> Result<(), JoinRefuse> {
    if node.keys.is_empty()
        || !node.dims.is_empty()
        || node.build_fold.is_some()
        || node.num_fold.is_some()
    {
        return Err(JoinRefuse::Unsupported { what: "num-fold-shape" });
    }
    if node.join_type != JoinType::Semi {
        return Err(JoinRefuse::Unsupported { what: "num-fold-join-type" });
    }
    let cap = 0..=30;
    if !cap.contains(&nf.scale) || !cap.contains(&nf.qual.k_scale) || !cap.contains(&nf.qual.probe_scale)
    {
        return Err(JoinRefuse::Face { attno: nf.col, what: "corr-numcell-scale-cap" });
    }
    let lane_ok = |bank: &Bank, col: u32, scale: i32| match bank.face(col) {
        Face::PackedNumeric { scale: s } => s == scale,
        Face::SignedWord(1..=8) | Face::UnsignedWord(1..=8) => scale == 0,
        _ => false,
    };
    if !lane_ok(build, nf.col, nf.scale) {
        return Err(JoinRefuse::Face { attno: nf.col, what: "corr-numcell-scale" });
    }
    if !lane_ok(probe, nf.qual.probe_col, nf.qual.probe_scale) {
        return Err(JoinRefuse::Face { attno: nf.qual.probe_col, what: "corr-numcell-probe-scale" });
    }
    node.num_fold = Some(nf);
    Ok(())
}
/// One CaseTest's face law at (bank, col): word tests on word-embed
/// lanes, byte tests on C-collated `Face::Varlena` lanes.
fn check_case_test(b: &Bank, col: u32, test: &CaseTest) -> Result<(), JoinRefuse> {
    match test {
        CaseTest::Word(_) => {
            if !word_embed(b, col) {
                return Err(JoinRefuse::Unsupported { what: "staged-or-face" });
            }
        }
        CaseTest::Packed(_, s) => {
            // The authored mantissa grid must BE the witnessed lane's.
            if !matches!(b.face(col), Face::PackedNumeric { scale } if scale == *s) {
                return Err(JoinRefuse::Unsupported { what: "staged-or-face" });
            }
        }
        CaseTest::InWords(_) => {
            if !word_embed(b, col) {
                return Err(JoinRefuse::Unsupported { what: "staged-or-face" });
            }
        }
        CaseTest::Bytes(_) => {
            let ty = b.typ(col);
            if !ty.is_varlena() || !matches!(b.face(col), Face::Varlena) {
                return Err(JoinRefuse::Unsupported { what: "staged-or-face" });
            }
            if ty.collation != COLLATION_C {
                return Err(JoinRefuse::Collation { attno: col, collation: ty.collation });
            }
        }
        CaseTest::And(ts) => {
            if ts.is_empty() {
                return Err(JoinRefuse::Unsupported { what: "staged-or-shape" });
            }
            for t in ts {
                check_case_test(b, col, t)?;
            }
        }
    }
    Ok(())
}

/// [crossdim-or] Attach a staged disjunction to a constructed keyed
/// INNER node (row or agg goal — the agg goal passes its inner `join`).
/// WHERE-filter semantics only hold conjunctively on the INNER class;
/// every term's face admits under the CASE-test law at its site's bank.
pub fn attach_staged_or(
    node: &mut JoinNode,
    build: &Bank,
    probe: &Bank,
    dim_banks: &[&Bank],
    so: StagedOr,
) -> Result<(), JoinRefuse> {
    if node.join_type != JoinType::Inner {
        return Err(JoinRefuse::Unsupported { what: "staged-or-join-type" });
    }
    if node.keys.is_empty() || node.build_fold.is_some() || node.staged_or.is_some() {
        return Err(JoinRefuse::Unsupported { what: "staged-or-shape" });
    }
    let narms = so.narms as usize;
    if narms < 2 || narms > MAX_OR_ARMS || so.terms.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "staged-or-shape" });
    }
    let mut armed = vec![false; narms];
    for t in &so.terms {
        if t.arm as usize >= narms {
            return Err(JoinRefuse::Unsupported { what: "staged-or-shape" });
        }
        armed[t.arm as usize] = true;
        let b = side_bank(build, probe, dim_banks, t.site)?;
        check_case_test(b, t.col, &t.test)?;
    }
    if !armed.iter().all(|&a| a) {
        // An untermed arm is vacuously TRUE — the disjunction is no
        // filter at all; the recognizer must not have staged it.
        return Err(JoinRefuse::Unsupported { what: "staged-or-shape" });
    }
    node.staged_or = Some(so);
    Ok(())
}

/// [semianti-flt] Attach semi/anti membership row filters to a
/// constructed keyed node. `filter_banks` aligns with `stages`; hosts
/// are Build/Probe scan columns, all faces word-embed; stage scan
/// conjuncts follow the dim-scan pred law.
pub fn attach_filter_stages(
    node: &mut JoinNode,
    build: &Bank,
    probe: &Bank,
    filter_banks: &[&Bank],
    stages: Vec<FilterStage>,
) -> Result<(), JoinRefuse> {
    if stages.len() != filter_banks.len() || stages.len() > MAX_FILTER_STAGES {
        return Err(JoinRefuse::Unsupported { what: "filter-stage-depth" });
    }
    if node.build_fold.is_some() || !node.filters.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "filter-stage-shape" });
    }
    // WHERE-grain row filters commute with INNER semantics only (a
    // build-host filter under LEFT would move null-extension).
    if node.join_type != JoinType::Inner {
        return Err(JoinRefuse::Unsupported { what: "filter-join-type" });
    }
    for (fi, f) in stages.iter().enumerate() {
        if !matches!(f.src, StageSrc::Bank) {
            return Err(JoinRefuse::Unsupported { what: "filter-stage-shape" });
        }
        let fb = filter_banks[fi];
        let hb = match f.host {
            JoinSide::Build => build,
            JoinSide::Probe => probe,
            JoinSide::Dim(_) => {
                return Err(JoinRefuse::Unsupported { what: "filter-host-side" });
            }
        };
        if f.keys.is_empty() || f.keys.len() > crate::joins::hash_join::MAX_KEY_LANES {
            return Err(JoinRefuse::Unsupported { what: "filter-no-equi-key" });
        }
        for k in &f.keys {
            if fb.typ(k.stage_col).is_varlena()
                || hb.typ(k.host_col).is_varlena()
                || !word_embed(fb, k.stage_col)
                || !word_embed(hb, k.host_col)
            {
                return Err(JoinRefuse::Unsupported { what: "filter-key-face" });
            }
        }
        for q in &f.quals {
            // Equalities are key lanes; a residual Eq is outside —
            // except against a collapsed cell, which it reads exactly.
            let eq_cell =
                matches!(&f.fold, Some(StageFold::Word(bf)) if bf.col == q.stage_col);
            if q.op == JoinCmp::Eq && !eq_cell {
                return Err(JoinRefuse::Unsupported { what: "filter-eq-qual" });
            }
            if fb.typ(q.stage_col).is_varlena()
                || hb.typ(q.host_col).is_varlena()
                || !word_embed(fb, q.stage_col)
                || !word_embed(hb, q.host_col)
            {
                return Err(JoinRefuse::Unsupported { what: "filter-qual-face" });
            }
        }
        // [mapstage] the grouped collapse: Semi verdict only; word quals
        // all read the collapsed cell (a residual against another stage
        // column is outside); Num carries its own qual, so `quals` is
        // empty. Lane laws mirror attach_build_fold / attach_num_fold.
        match &f.fold {
            None => {}
            Some(StageFold::Word(bf)) => {
                if f.anti {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-anti" });
                }
                if f.quals.is_empty() || f.quals.iter().any(|q| q.stage_col != bf.col) {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-residual" });
                }
                if fb.typ(bf.col).is_varlena() || !word_embed(fb, bf.col) {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-col" });
                }
                match bf.op {
                    JoinAggOp::CountStar
                    | JoinAggOp::CountCol
                    | JoinAggOp::Min
                    | JoinAggOp::Max => {}
                    JoinAggOp::CountDistinct => {
                        return Err(JoinRefuse::Unsupported { what: "filter-fold-op" });
                    }
                    JoinAggOp::Sum => {
                        let narrow = matches!(
                            fb.face(bf.col),
                            Face::SignedWord(1..=4) | Face::UnsignedWord(1..=4)
                        );
                        if !narrow {
                            return Err(JoinRefuse::Unsupported { what: "filter-fold-sum-width" });
                        }
                    }
                }
                if bf.missing.is_some()
                    && !matches!(bf.op, JoinAggOp::CountStar | JoinAggOp::CountCol)
                {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-missing" });
                }
            }
            Some(StageFold::Num(nf)) => {
                if f.anti {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-anti" });
                }
                if !f.quals.is_empty() {
                    return Err(JoinRefuse::Unsupported { what: "filter-fold-residual" });
                }
                let cap = 0..=30;
                if !cap.contains(&nf.scale)
                    || !cap.contains(&nf.qual.k_scale)
                    || !cap.contains(&nf.qual.probe_scale)
                {
                    return Err(JoinRefuse::Face { attno: nf.col, what: "corr-numcell-scale-cap" });
                }
                let lane_ok = |bank: &Bank, col: u32, scale: i32| match bank.face(col) {
                    Face::PackedNumeric { scale: s } => s == scale,
                    Face::SignedWord(1..=8) | Face::UnsignedWord(1..=8) => scale == 0,
                    _ => false,
                };
                if !lane_ok(fb, nf.col, nf.scale) {
                    return Err(JoinRefuse::Face { attno: nf.col, what: "corr-numcell-scale" });
                }
                if !lane_ok(hb, nf.qual.probe_col, nf.qual.probe_scale) {
                    return Err(JoinRefuse::Face {
                        attno: nf.qual.probe_col,
                        what: "corr-numcell-probe-scale",
                    });
                }
            }
        }
        if let Some(p) = &f.pred {
            check_var_terms(fb, p)?;
            for t in &p.terms {
                if !word_embed(fb, t.col) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
            for t in &p.col_terms {
                if !word_embed(fb, t.a) || !word_embed(fb, t.b) {
                    return Err(JoinRefuse::Unsupported { what: "side-pred-face" });
                }
            }
        }
        for t in &f.text_eqs {
            let ty = fb.typ(t.col);
            if !ty.is_varlena() {
                return Err(JoinRefuse::Unsupported { what: "dim-text-eq-face" });
            }
            if ty.collation != COLLATION_C {
                return Err(JoinRefuse::Collation { attno: t.col, collation: ty.collation });
            }
        }
    }
    node.filters = stages;
    Ok(())
}

/// [mapjoingoal] Attach the goal-fed map stage: the stage side is
/// answer data (canonical words by the goal's own admission), so only
/// the HOST lanes and the cell laws check here — Num fold, Semi
/// verdict, INNER tops, no residuals. Returns the stage index; the
/// engagement swaps its rows in (`set_map_goal_rows`).
pub fn attach_map_goal_stage(
    node: &mut JoinNode,
    build: &Bank,
    probe: &Bank,
    stage: FilterStage,
) -> Result<usize, JoinRefuse> {
    if node.filters.len() >= MAX_FILTER_STAGES {
        return Err(JoinRefuse::Unsupported { what: "filter-stage-depth" });
    }
    if node.join_type != JoinType::Inner {
        return Err(JoinRefuse::Unsupported { what: "filter-join-type" });
    }
    if node.build_fold.is_some()
        || stage.anti
        || !stage.quals.is_empty()
        || stage.pred.is_some()
        || !stage.text_eqs.is_empty()
        || !matches!(stage.src, StageSrc::Rows(_))
    {
        return Err(JoinRefuse::Unsupported { what: "filter-stage-shape" });
    }
    let Some(StageFold::Num(nf)) = &stage.fold else {
        return Err(JoinRefuse::Unsupported { what: "filter-stage-shape" });
    };
    if matches!(nf.op, NumCellOp::Avg) {
        return Err(JoinRefuse::Unsupported { what: "map-goal-avg" });
    }
    if stage.keys.is_empty() || stage.keys.len() > crate::joins::hash_join::MAX_KEY_LANES {
        return Err(JoinRefuse::Unsupported { what: "filter-no-equi-key" });
    }
    let hb = match stage.host {
        JoinSide::Build => build,
        JoinSide::Probe => probe,
        JoinSide::Dim(_) => {
            return Err(JoinRefuse::Unsupported { what: "filter-host-side" });
        }
    };
    for k in &stage.keys {
        if hb.typ(k.host_col).is_varlena() || !word_embed(hb, k.host_col) {
            return Err(JoinRefuse::Unsupported { what: "filter-key-face" });
        }
    }
    let cap = 0..=30;
    if !cap.contains(&nf.scale)
        || !cap.contains(&nf.qual.k_scale)
        || !cap.contains(&nf.qual.probe_scale)
    {
        return Err(JoinRefuse::Face { attno: nf.col, what: "corr-numcell-scale-cap" });
    }
    let probe_lane_ok = match hb.face(nf.qual.probe_col) {
        Face::PackedNumeric { scale: s } => s == nf.qual.probe_scale,
        Face::SignedWord(1..=8) | Face::UnsignedWord(1..=8) => nf.qual.probe_scale == 0,
        _ => false,
    };
    if !probe_lane_ok {
        return Err(JoinRefuse::Face {
            attno: nf.qual.probe_col,
            what: "corr-numcell-probe-scale",
        });
    }
    node.filters.push(stage);
    Ok(node.filters.len() - 1)
}

/// [mapjoingoal] Swap the engagement's pre-collapsed rows into the
/// goal-fed stage (per-engagement node clones only — the map is data).
pub fn set_map_goal_rows(node: &mut JoinNode, fi: usize, rows: std::sync::Arc<StageRows>) {
    if let Some(f) = node.filters.get_mut(fi) {
        f.src = StageSrc::Rows(rows);
    }
}

/// Validate + construct a `JoinAggNode` (fused agg-over-join goal). The
/// inner `JoinNode.out` becomes the distinct agg-input set (+ group
/// keys), so probe/build decode planning is shared with the row goal.
/// [sqe-mech3] `groups` = the composite key list (any side incl. Dim;
/// word-embed or C-collation text faces); any JoinAggOp mix folds per
/// group. `arith` legs must be Sum over word-embed int inputs — the
/// no-overflow witness is the runner's admission (`check_join_agg`).
#[allow(clippy::too_many_arguments)]
pub fn join_agg_node(
    build: &Bank,
    probe: &Bank,
    dim_banks: &[&Bank],
    q: u32,
    join_type: JoinType,
    keys: Vec<JoinKey>,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    dims: Vec<DimStage>,
    aggs: Vec<JoinAggReq>,
    groups: Vec<JoinOut>,
    build_rows_hint: u64,
) -> Result<JoinAggNode, JoinRefuse> {
    if keys.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "no-equi-key" });
    }
    mk_join_agg_node(
        build, probe, dim_banks, q, join_type, keys, quals, build_pred, probe_pred, dims, aggs,
        groups, build_rows_hint,
    )
}

/// P4-5: the fused agg-over-nest-loop goal (keyless `JoinAggNode`; no
/// dim stages — keyless v1 is strictly 2-way).
#[allow(clippy::too_many_arguments)]
pub fn nest_loop_agg_node(
    build: &Bank,
    probe: &Bank,
    q: u32,
    join_type: JoinType,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    aggs: Vec<JoinAggReq>,
    groups: Vec<JoinOut>,
    build_rows_hint: u64,
) -> Result<JoinAggNode, JoinRefuse> {
    mk_join_agg_node(
        build,
        probe,
        &[],
        q,
        join_type,
        Vec::new(),
        quals,
        build_pred,
        probe_pred,
        Vec::new(),
        aggs,
        groups,
        build_rows_hint,
    )
}

#[allow(clippy::too_many_arguments)]
fn mk_join_agg_node(
    build: &Bank,
    probe: &Bank,
    dim_banks: &[&Bank],
    q: u32,
    join_type: JoinType,
    keys: Vec<JoinKey>,
    quals: Vec<JoinQual>,
    build_pred: Option<PredSpec>,
    probe_pred: Option<PredSpec>,
    dims: Vec<DimStage>,
    aggs: Vec<JoinAggReq>,
    groups: Vec<JoinOut>,
    build_rows_hint: u64,
) -> Result<JoinAggNode, JoinRefuse> {
    if aggs.is_empty() {
        return Err(JoinRefuse::Unsupported { what: "agg-none" });
    }
    if groups.len() > MAX_GROUP_KEYS {
        return Err(JoinRefuse::Unsupported { what: "agg-group-too-many-keys" });
    }
    for g in &groups {
        let b = side_bank(build, probe, dim_banks, g.side)?;
        let ty = b.typ(g.col);
        if matches!(b.face(g.col), Face::PackedNumeric { .. }) {
            // [scale-alg] packed keys group by the exact mantissa word.
        } else if ty.is_varlena() {
            // Byte grouping identity: C collation only (multi-key text
            // keys ride the composite sink's byte identity).
            if ty.collation != COLLATION_C {
                return Err(JoinRefuse::Collation { attno: g.col, collation: ty.collation });
            }
        } else if !word_embed(b, g.col) {
            return Err(JoinRefuse::Unsupported { what: "agg-group-face" });
        }
        if matches!(join_type, JoinType::Semi | JoinType::Anti) && g.side != JoinSide::Probe {
            return Err(JoinRefuse::Unsupported { what: "agg-group-side" });
        }
        // [sqe-semi-anti] right classes group over BUILD columns only
        // (the emitted rows carry no probe/dim values).
        if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti)
            && g.side != JoinSide::Build
        {
            return Err(JoinRefuse::Unsupported { what: "agg-group-side" });
        }
    }
    let mut outs: Vec<JoinOut> = Vec::new();
    let mut agg_oi: Vec<usize> = Vec::new();
    let mut agg_oi2: Vec<usize> = Vec::new();
    let mut agg_oic: Vec<usize> = Vec::new();
    let mut specs: Vec<JoinAggSpec> = Vec::new();
    let mut add_out = |io: JoinOut| -> usize {
        outs.iter().position(|o| *o == io).unwrap_or_else(|| {
            outs.push(io);
            outs.len() - 1
        })
    };
    for r in aggs {
        // [caseleg] word tests on word-embed lanes, byte tests on
        // C-collated varlena lanes (byte equality / LIKE ARE the law).
        if let Some(c) = &r.case {
            let cb = side_bank(build, probe, dim_banks, c.col.side)?;
            fn leaves<'t>(t: &'t CaseTest, out: &mut Vec<&'t CaseTest>) {
                match t {
                    CaseTest::And(ts) => ts.iter().for_each(|t| leaves(t, out)),
                    _ => out.push(t),
                }
            }
            let mut lv = Vec::new();
            leaves(&c.test, &mut lv);
            if lv.is_empty() {
                return Err(JoinRefuse::Unsupported { what: "agg-case-test-shape" });
            }
            for t in lv {
                let ok = match t {
                    CaseTest::Word(_) | CaseTest::InWords(_) => word_embed(cb, c.col.col),
                    CaseTest::Packed(_, s) => {
                        matches!(cb.face(c.col.col), Face::PackedNumeric { scale } if scale == *s)
                    }
                    CaseTest::Bytes(_) => {
                        let ty = cb.typ(c.col.col);
                        if ty.is_varlena() && ty.collation != COLLATION_C {
                            return Err(JoinRefuse::Collation {
                                attno: c.col.col,
                                collation: ty.collation,
                            });
                        }
                        ty.is_varlena() && matches!(cb.face(c.col.col), Face::Varlena)
                    }
                    CaseTest::And(_) => false,
                };
                if !ok {
                    return Err(JoinRefuse::Unsupported { what: "agg-case-test-face" });
                }
            }
            if matches!(join_type, JoinType::Semi | JoinType::Anti) && c.col.side != JoinSide::Probe
            {
                return Err(JoinRefuse::Unsupported { what: "agg-case-side" });
            }
            if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti)
                && c.col.side != JoinSide::Build
            {
                return Err(JoinRefuse::Unsupported { what: "agg-case-side" });
            }
            agg_oic.push(add_out(c.col));
        } else {
            agg_oic.push(usize::MAX);
        }
        match (r.op, r.input) {
            (JoinAggOp::CountStar, Some(_)) => {
                return Err(JoinRefuse::Unsupported { what: "agg-count-star-input" });
            }
            (JoinAggOp::CountStar, None) => {
                if r.arith.is_some() || r.input2.is_some() || r.case.is_some() {
                    return Err(JoinRefuse::Unsupported { what: "agg-count-star-input" });
                }
                agg_oi.push(usize::MAX);
                agg_oi2.push(usize::MAX);
                specs.push(JoinAggSpec {
                    op: r.op,
                    input: None,
                    input2: None,
                    arith: None,
                    case: None,
                    out: JoinAggSpec::out_ty(r.op, None),
                });
            }
            // [caseleg] the predicated row count (no input lane).
            (JoinAggOp::CountCol, None) if r.case.is_some() => {
                if r.arith.is_some() || r.input2.is_some() {
                    return Err(JoinRefuse::Unsupported { what: "agg-arith-shape" });
                }
                agg_oi.push(usize::MAX);
                agg_oi2.push(usize::MAX);
                specs.push(JoinAggSpec {
                    op: r.op,
                    input: None,
                    input2: None,
                    arith: None,
                    case: r.case,
                    out: JoinAggSpec::out_ty(r.op, None),
                });
            }
            (_, None) => {
                return Err(JoinRefuse::Unsupported { what: "agg-missing-input" });
            }
            (op, Some(io)) => {
                let b = side_bank(build, probe, dim_banks, io.side)?;
                let t = b.typ(io.col);
                // Distinct sets: bare word lanes, grouped shapes only
                // (varlena identity is a pending ruling; the ungrouped
                // sinks carry no set plane).
                if op == JoinAggOp::CountDistinct {
                    if r.arith.is_some() || r.input2.is_some() || r.case.is_some() {
                        return Err(JoinRefuse::Unsupported { what: "agg-distinct-shape" });
                    }
                    if groups.is_empty() {
                        return Err(JoinRefuse::Unsupported { what: "agg-distinct-ungrouped" });
                    }
                    if t.is_varlena() || !word_embed(b, io.col) {
                        return Err(JoinRefuse::Unsupported { what: "agg-distinct-input" });
                    }
                }
                // [packednum] sum/min/max fold inputs admit the
                // PackedNumeric word lane (agg_word_embed); every other
                // varlena input keeps the typed refusal.
                if matches!(op, JoinAggOp::Sum | JoinAggOp::Min | JoinAggOp::Max)
                    && !agg_word_embed(b, io.col)
                {
                    return Err(JoinRefuse::Unsupported { what: "agg-input-face" });
                }
                // [sqe-mech3] fused arithmetic: Sum only; operand faces
                // word-embed; MulCC/MulKSub carry input2, AddK must not.
                if let Some(ar) = r.arith {
                    if op != JoinAggOp::Sum {
                        return Err(JoinRefuse::Unsupported { what: "agg-arith-op" });
                    }
                    if ar.needs_input2() != r.input2.is_some() {
                        return Err(JoinRefuse::Unsupported { what: "agg-arith-shape" });
                    }
                    // [scale-alg] packed lanes, or the scale-0 int
                    // lane of a mixed product ([tpch-expr]).
                    let (packed, psa, psb) = match ar {
                        JoinArith::PackedMulK { sa, sb, .. } => (true, sa, sb),
                        JoinArith::PackedMulKSubCC { sa, sb, .. } => (true, sa, sb),
                        _ => (false, 0, 0),
                    };
                    let packed_lane_ok = |bk: &crate::bank::Bank, col: u32, scale: i32| {
                        matches!(bk.face(col), Face::PackedNumeric { .. })
                            || (scale == 0
                                && !bk.typ(col).is_varlena()
                                && word_embed(bk, col))
                    };
                    if packed != matches!(b.face(io.col), Face::PackedNumeric { .. })
                        && !(packed && packed_lane_ok(b, io.col, psa))
                    {
                        return Err(JoinRefuse::Unsupported { what: "agg-input-face" });
                    }
                    if let Some(io2) = r.input2 {
                        let b2 = side_bank(build, probe, dim_banks, io2.side)?;
                        let ok2 = if packed {
                            packed_lane_ok(b2, io2.col, psb)
                        } else {
                            !b2.typ(io2.col).is_varlena() && word_embed(b2, io2.col)
                        };
                        if !ok2 {
                            return Err(JoinRefuse::Unsupported { what: "agg-input-face" });
                        }
                    }
                    if let JoinArith::PackedMulKSubCC { c, sc, d, sd, .. } = ar {
                        for (io3, s3) in [(c, sc), (d, sd)] {
                            let b3 = side_bank(build, probe, dim_banks, io3.side)?;
                            if !packed_lane_ok(b3, io3.col, s3) {
                                return Err(JoinRefuse::Unsupported { what: "agg-input-face" });
                            }
                            add_out(io3);
                        }
                    }
                } else if r.input2.is_some() {
                    return Err(JoinRefuse::Unsupported { what: "agg-arith-shape" });
                }
                let oi = add_out(io);
                let oi2 = r.input2.map(&mut add_out).unwrap_or(usize::MAX);
                agg_oi.push(oi);
                agg_oi2.push(oi2);
                specs.push(JoinAggSpec {
                    op,
                    input: Some(io),
                    input2: r.input2,
                    arith: r.arith,
                    case: r.case,
                    out: JoinAggSpec::out_ty(op, Some(t)),
                });
            }
        }
    }
    let group_oi: Vec<usize> = groups.iter().map(|g| add_out(*g)).collect();
    if outs.is_empty() {
        // pure count(*): decode plan still needs one out; a key or qual
        // lane is already planned, so it is free. [sqe-semi-anti] right
        // classes take a BUILD lane (their emitted rows are build rows).
        if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
            let col = keys
                .first()
                .map(|k| k.build_col)
                .ok_or(JoinRefuse::Unsupported { what: "right-join-keyless" })?;
            outs.push(JoinOut { side: JoinSide::Build, col });
        } else {
            let col = keys
                .first()
                .map(|k| k.probe_col)
                .or_else(|| quals.first().map(|q| q.probe_col))
                .or_else(|| probe_pred.as_ref().and_then(|p| p.terms.first()).map(|t| t.col))
                .ok_or(JoinRefuse::Unsupported { what: "nl-side-no-cols" })?;
            outs.push(JoinOut { side: JoinSide::Probe, col });
        }
    }
    if keys.is_empty() {
        check_nl_side_cols(&quals, &build_pred, &probe_pred, &outs)?;
    }
    let join = mk_join_node(
        build,
        probe,
        dim_banks,
        q,
        join_type,
        keys,
        quals,
        build_pred,
        probe_pred,
        dims,
        outs,
        build_rows_hint,
        true,
    )?;
    Ok(JoinAggNode {
        join,
        aggs: specs,
        agg_oi,
        agg_oi2,
        agg_oic,
        groups,
        group_oi,
        group_xf: Vec::new(),
        having: None,
    })
}
