use ::adt_tsvector_core::io::tsvector_in_core;
use ::adt_tsvector_core::layout::TsVec;
use ::adt_tsvector_core::query::TsQueryRef;
use ::adt_tsquery_core::io::tsquery_in_core;
use ::mcx::{MemoryContext, Mcx};

use crate::rank::{calc_rank, DEFAULT_WEIGHTS, DEF_NORM_METHOD};
use crate::rank_cd::calc_rank_cd;

fn v<'a>(mcx: Mcx<'a>, s: &str) -> TsVec<'a> {
    let img = tsvector_in_core(mcx, s.as_bytes(), None).unwrap().unwrap();
    TsVec { payload: &img.leak()[4..] }
}

fn q<'a>(mcx: Mcx<'a>, s: &str) -> TsQueryRef<'a> {
    let img = tsquery_in_core(mcx, s.as_bytes(), None).unwrap().unwrap();
    TsQueryRef { payload: &img.leak()[4..] }
}

fn close(got: f32, want: f32) -> bool {
    (got - want).abs() <= want.abs().max(1e-6) * 1e-5
}

#[test]
fn rank_matrix() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (doc, query, want) in [
        (" a:1 s:2C d g", "a | s", 0.0911891f32),
        (" a:1 sa:2C d g", "a | s", 0.0303964),
        (" a:1 sa:2C d g", "a | s:*", 0.0911891),
        (" a:1 sa:2C d g", "a | sa:*", 0.0911891),
        (" a:1 s:2B d g", "a | s", 0.151982),
        (" a:1 s:2 d g", "a | s", 0.0607927),
        (" a:1 s:2C d g", "a & s", 0.140153),
        (" a:1 s:2B d g", "a & s", 0.198206),
        (" a:1 s:2 d g", "a & s", 0.0991032),
    ] {
        let got =
            calc_rank(mcx, &DEFAULT_WEIGHTS, v(mcx, doc), q(mcx, query), DEF_NORM_METHOD)
                .unwrap();
        assert!(close(got, want), "{doc} @@ {query}: got {got}, want {want}");
    }
}

#[test]
fn rank_cd_matrix() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for (doc, query, want) in [
        (" a:1 s:2C d g", "a | s", 0.3f32),
        (" a:1 sa:2C d g", "a | s", 0.1),
        (" a:1 sa:2C d g", "a | s:*", 0.3),
        (" a:1 sa:2C d g", "a | sa:*", 0.3),
        (" a:1 sa:3C sab:2c d g", "a | sa:*", 0.5),
        (" a:1 s:2B d g", "a | s", 0.5),
        (" a:1 s:2 d g", "a | s", 0.2),
        (" a:1 s:2C d g", "a & s", 0.133333),
        (" a:1 s:2B d g", "a & s", 0.16),
        (" a:1 s:2 d g", "a & s", 0.1),
        (" a:1 s:2A d g", "a <-> s", 0.181818),
        (" a:1 s:2C d g", "a <-> s", 0.133333),
        (" a:1 s:2 d g", "a <-> s", 0.1),
        (" a:1 s:2 d:2A g", "a <-> s", 0.1),
    ] {
        let got = calc_rank_cd(mcx, &DEFAULT_WEIGHTS, v(mcx, doc), q(mcx, query), 0).unwrap();
        assert!(close(got, want), "{doc} @@ {query}: got {got}, want {want}");
    }
}
