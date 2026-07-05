//! tsginidx.c: GIN support functions for tsvector_ops, consumed by the GIN
//! closed-set opclass dispatch (gin/src/opclass.rs) plus the SQL-callable
//! comparators as fmgr builtins.

use ::adt_tsvector_core::execute::{
    ts_execute_ternary, ExecPhraseData, Ternary, TS_EXEC_PHRASE_NO_POS,
};
use ::adt_tsvector_core::layout::{ts_compare_string, TsVec};
use ::adt_tsvector_core::op::tsquery_requires_match;
use ::adt_tsvector_core::query::{Item, Operand, TsQueryRef};
use ::datum::Datum;
use ::gin_vocab::{
    GinTernaryValue, GIN_FALSE, GIN_MAYBE, GIN_SEARCH_MODE_ALL, GIN_SEARCH_MODE_DEFAULT, GIN_TRUE,
};
use ::mcx::{Mcx, PgVec};
use ::types_error::PgResult;

pub mod builtins;

#[cfg(test)]
mod tests;

pub fn gin_cmp_tslexeme(a: &[u8], b: &[u8]) -> i32 {
    ts_compare_string(a, b, false)
}

pub fn gin_cmp_prefix(a: &[u8], b: &[u8]) -> i32 {
    let cmp = ts_compare_string(a, b, true);
    if cmp < 0 {
        // prevent continue scan
        1
    } else {
        cmp
    }
}

fn text_key<'m>(mcx: Mcx<'m>, s: &[u8]) -> PgResult<Datum> {
    let total = 4 + s.len();
    let mut item: PgVec<'m, u8> = mcx::vec_with_capacity_in(mcx, total)?;
    mcx::vec_append_bytes(
        &mut item,
        &::types_tuple::varatt::set_varsize_4b_word(total as u32).to_ne_bytes(),
    )?;
    mcx::vec_append_bytes(&mut item, s)?;
    let p = item.as_ptr();
    core::mem::forget(item);
    Ok(Datum::from_usize(p as usize))
}

pub fn gin_extract_tsvector<'m>(mcx: Mcx<'m>, vector: TsVec<'_>) -> PgResult<PgVec<'m, Datum>> {
    let size = vector.size();
    let mut entries: PgVec<'m, Datum> = mcx::vec_with_capacity_in(mcx, size)?;
    for i in 0..size {
        let we = vector.entry(i);
        entries.push(text_key(mcx, vector.lexeme(we))?);
    }
    Ok(entries)
}

pub struct ExtractedTsquery<'m> {
    pub entries: PgVec<'m, Datum>,
    pub partial_match: PgVec<'m, bool>,
    pub map_item_operand: PgVec<'m, i32>,
    pub search_mode: i32,
}

pub fn gin_extract_tsquery<'m>(
    mcx: Mcx<'m>,
    query: TsQueryRef<'_>,
) -> PgResult<ExtractedTsquery<'m>> {
    let size = query.size();
    let mut out = ExtractedTsquery {
        entries: mcx::vec_new_in(mcx),
        partial_match: mcx::vec_new_in(mcx),
        map_item_operand: mcx::vec_new_in(mcx),
        search_mode: GIN_SEARCH_MODE_DEFAULT,
    };
    if size == 0 {
        return Ok(out);
    }

    out.search_mode = if tsquery_requires_match(query, 0) {
        GIN_SEARCH_MODE_DEFAULT
    } else {
        GIN_SEARCH_MODE_ALL
    };

    let nvals = (0..size)
        .filter(|&i| matches!(query.item(i), Item::Val(_)))
        .count();
    out.entries = mcx::vec_with_capacity_in(mcx, nvals)?;
    out.partial_match = mcx::vec_with_capacity_in(mcx, nvals)?;
    out.map_item_operand = mcx::vec_from_elem_in(mcx, 0i32, size);

    let mut j = 0i32;
    for i in 0..size {
        if let Item::Val(val) = query.item(i) {
            out.entries.push(text_key(mcx, query.operand_str(&val))?);
            out.partial_match.push(val.prefix);
            out.map_item_operand[i] = j;
            j += 1;
        }
    }
    Ok(out)
}

fn checkcondition_gin(
    check: &[GinTernaryValue],
    map_item_operand: &[i32],
    item_index: usize,
    val: &Operand,
    data: Option<&mut ExecPhraseData<'_>>,
) -> Ternary {
    let j = map_item_operand[item_index] as usize;
    let mut result = check[j];
    if result == GIN_TRUE && (val.weight != 0 || data.is_some()) {
        result = GIN_MAYBE;
    }
    match result {
        GIN_FALSE => Ternary::No,
        GIN_TRUE => Ternary::Yes,
        _ => Ternary::Maybe,
    }
}

fn exec_over_check(
    mcx: Mcx<'_>,
    check: &[GinTernaryValue],
    query: TsQueryRef<'_>,
    map_item_operand: &[i32],
) -> PgResult<Ternary> {
    let mut chk = |idx: usize, val: &Operand, data: Option<&mut ExecPhraseData<'_>>| {
        Ok(checkcondition_gin(check, map_item_operand, idx, val, data))
    };
    ts_execute_ternary(mcx, query, TS_EXEC_PHRASE_NO_POS, &mut chk)
}

pub fn gin_tsquery_consistent(
    mcx: Mcx<'_>,
    check: &[GinTernaryValue],
    query: TsQueryRef<'_>,
    map_item_operand: &[i32],
) -> PgResult<(bool, bool)> {
    let mut res = false;
    let mut recheck = false;
    if query.size() > 0 {
        match exec_over_check(mcx, check, query, map_item_operand)? {
            Ternary::No => res = false,
            Ternary::Yes => res = true,
            Ternary::Maybe => {
                res = true;
                recheck = true;
            }
        }
    }
    Ok((res, recheck))
}

pub fn gin_tsquery_triconsistent(
    mcx: Mcx<'_>,
    check: &[GinTernaryValue],
    query: TsQueryRef<'_>,
    map_item_operand: &[i32],
) -> PgResult<GinTernaryValue> {
    if query.size() == 0 {
        return Ok(GIN_FALSE);
    }
    Ok(match exec_over_check(mcx, check, query, map_item_operand)? {
        Ternary::No => GIN_FALSE,
        Ternary::Yes => GIN_TRUE,
        Ternary::Maybe => GIN_MAYBE,
    })
}
