use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub const MAX_LEVENSHTEIN_STRLEN: i32 = 255;

#[inline]
fn rest_of_char_same(s1: &[u8], s2: &[u8], len: i32) -> bool {
    // Back-to-front like C: the distinguishing byte is usually near the end.
    let mut len = len as usize;
    while len > 0 {
        len -= 1;
        if s1[len] != s2[len] {
            return false;
        }
    }
    true
}

#[cold]
#[inline(never)]
fn too_long() -> PgError {
    PgError::error(format!(
        "levenshtein argument exceeds maximum length of {MAX_LEVENSHTEIN_STRLEN} characters"
    ))
    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
}

// upstream e88eb4e76638 (18.6): Avoid overflow in Levenshtein distance calculations.
// Distances are summed in i64; the int4 result is range-checked once.
#[cold]
#[inline(never)]
fn out_of_range() -> PgError {
    PgError::error("levenshtein distance out of range")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[inline]
fn levenshtein_result(res: i64) -> PgResult<i32> {
    i32::try_from(res).map_err(|_| out_of_range().into())
}

pub fn varstr_levenshtein(
    mcx: Mcx<'_>,
    source: &[u8],
    target: &[u8],
    ins_c: i32,
    del_c: i32,
    sub_c: i32,
    trusted: bool,
) -> PgResult<i32> {
    varstr_levenshtein_less_equal(mcx, source, target, ins_c, del_c, sub_c, -1, trusted)
}

// Single body for both levenshtein.c expansions; max_d < 0 is the plain
// varstr_levenshtein (the bound arms compute identically when disabled).
pub fn varstr_levenshtein_less_equal(
    mcx: Mcx<'_>,
    source: &[u8],
    target: &[u8],
    ins_c: i32,
    del_c: i32,
    sub_c: i32,
    mut max_d: i32,
    trusted: bool,
) -> PgResult<i32> {
    let slen = source.len() as i32;
    let tlen = target.len() as i32;
    let ins_c = ins_c as i64;
    let del_c = del_c as i64;
    let mut sub_c = sub_c as i64;

    let m0 = mbutils_seams::pg_mbstrlen_with_len::call(source)?;
    let n0 = mbutils_seams::pg_mbstrlen_with_len::call(target)?;

    if m0 == 0 {
        return levenshtein_result(n0 as i64 * ins_c);
    }
    if n0 == 0 {
        return levenshtein_result(m0 as i64 * del_c);
    }

    if !trusted && (m0 > MAX_LEVENSHTEIN_STRLEN || n0 > MAX_LEVENSHTEIN_STRLEN) {
        return Err(too_long().into());
    }

    let mut start_column: i32 = 0;
    let mut stop_column: i32 = m0 + 1;

    if max_d >= 0 {
        let net_inserts = n0 - m0;
        let min_theo_d: i64 = if net_inserts < 0 {
            -(net_inserts as i64) * del_c
        } else {
            net_inserts as i64 * ins_c
        };
        if min_theo_d > max_d as i64 {
            return levenshtein_result(max_d as i64 + 1);
        }
        if ins_c + del_c < sub_c {
            sub_c = ins_c + del_c;
        }
        let max_theo_d = min_theo_d + sub_c * m0.min(n0) as i64;
        if max_d as i64 >= max_theo_d {
            max_d = -1;
        } else if ins_c + del_c > 0 {
            let slack_d = max_d as i64 - min_theo_d;
            let best_column = if net_inserts < 0 { -net_inserts } else { 0 };
            let tmp = best_column as i64 + (slack_d / (ins_c + del_c)) + 1;
            stop_column = tmp.min(m0 as i64 + 1) as i32;
        }
    }

    let mut s_char_len: Option<PgVec<'_, i32>> = None;
    if m0 != slen || n0 != tlen {
        let mut v: PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, (m0 + 1) as usize)?;
        let mut off = 0usize;
        for _ in 0..m0 {
            let cl = mbutils_seams::pg_mblen_range::call(&source[off..])?;
            v.push(cl);
            off += cl as usize;
        }
        v.push(0);
        s_char_len = Some(v);
    }

    let m = m0 + 1;
    let n = n0 + 1;

    let mut rows: PgVec<'_, i64> = mcx::vec_with_capacity_in(mcx, 2 * m as usize)?;
    rows.resize(2 * m as usize, 0);
    let (mut prev, mut curr) = rows.split_at_mut(m as usize);

    let mut i = start_column;
    while i < stop_column {
        prev[i as usize] = i as i64 * del_c;
        i += 1;
    }

    // C mutates the source pointer as start_column slides right.
    let mut source_off = 0usize;
    let mut y_off = 0usize;

    for j in 1..n {
        let y_char_len = if n != tlen + 1 {
            mbutils_seams::pg_mblen_range::call(&target[y_off..])?
        } else {
            1
        };

        if stop_column < m {
            prev[stop_column as usize] = max_d as i64 + 1;
            stop_column += 1;
        }

        let mut i;
        if start_column == 0 {
            curr[0] = j as i64 * ins_c;
            i = 1;
        } else {
            i = start_column;
        }

        let mut x_off = source_off;
        if let Some(ref scl) = s_char_len {
            while i < stop_column {
                let x_char_len = scl[(i - 1) as usize];
                let ins = prev[i as usize] + ins_c;
                let del = curr[(i - 1) as usize] + del_c;
                let sub = if source[x_off + (x_char_len - 1) as usize]
                    == target[y_off + (y_char_len - 1) as usize]
                    && x_char_len == y_char_len
                    && (x_char_len == 1
                        || rest_of_char_same(&source[x_off..], &target[y_off..], x_char_len))
                {
                    prev[(i - 1) as usize]
                } else {
                    prev[(i - 1) as usize] + sub_c
                };
                curr[i as usize] = ins.min(del).min(sub);
                x_off += x_char_len as usize;
                i += 1;
            }
        } else {
            while i < stop_column {
                let ins = prev[i as usize] + ins_c;
                let del = curr[(i - 1) as usize] + del_c;
                let sub = prev[(i - 1) as usize]
                    + if source[x_off] == target[y_off] { 0 } else { sub_c };
                curr[i as usize] = ins.min(del).min(sub);
                x_off += 1;
                i += 1;
            }
        }

        core::mem::swap(&mut prev, &mut curr);
        y_off += y_char_len as usize;

        if max_d >= 0 {
            let zp = j - (n - m);

            while stop_column > 0 {
                let ii = stop_column - 1;
                let net_inserts = ii - zp;
                let resid = if net_inserts > 0 {
                    net_inserts as i64 * ins_c
                } else {
                    -(net_inserts as i64) * del_c
                };
                if prev[ii as usize] + resid <= max_d as i64 {
                    break;
                }
                stop_column -= 1;
            }

            while start_column < stop_column {
                let net_inserts = start_column - zp;
                let resid = if net_inserts > 0 {
                    net_inserts as i64 * ins_c
                } else {
                    -(net_inserts as i64) * del_c
                };
                if prev[start_column as usize] + resid <= max_d as i64 {
                    break;
                }
                prev[start_column as usize] = max_d as i64 + 1;
                curr[start_column as usize] = max_d as i64 + 1;
                if start_column != 0 {
                    source_off += match s_char_len {
                        Some(ref scl) => scl[(start_column - 1) as usize] as usize,
                        None => 1,
                    };
                }
                start_column += 1;
            }

            if start_column >= stop_column {
                return levenshtein_result(max_d as i64 + 1);
            }
        }
    }

    levenshtein_result(prev[(m - 1) as usize])
}
