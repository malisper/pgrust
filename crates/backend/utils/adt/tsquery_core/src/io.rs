use ::adt_tsvector_core::layout::{MAXSTRLEN, MAXSTRPOS};
use ::adt_tsvector_core::query::*;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, SoftErrorContext};

use crate::parse::{build_query_image, findoprnd, parse_tsquery, pushval_asis};

pub fn tsquery_in_core<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    esc: Option<&mut SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    Ok(parse_tsquery(mcx, input, 0, esc, &mut pushval_asis)?.map(|p| p.img))
}

struct Infix<'a> {
    q: TsQueryRef<'a>,
    cur: usize,
}

fn push_escaped(out: &mut PgVec<'_, u8>, op: &[u8]) {
    let mut k = 0usize;
    while k < op.len() {
        let cl = (::mbutils::pg_mblen(&op[k..]) as usize).min(op.len() - k);
        if op[k] == b'\'' {
            out.push(b'\'');
        } else if op[k] == b'\\' {
            out.push(b'\\');
        }
        out.extend_from_slice(&op[k..k + cl]);
        k += cl;
    }
}

fn push_i32_dec(out: &mut PgVec<'_, u8>, v: i32) {
    let mut buf = [0u8; 11];
    let mut i = buf.len();
    let neg = v < 0;
    let mut v = (v as i64).unsigned_abs();
    loop {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    out.extend_from_slice(&buf[i..]);
}

fn infix<'mcx>(
    mcx: Mcx<'mcx>,
    st: &mut Infix<'_>,
    out: &mut PgVec<'mcx, u8>,
    parent_priority: i32,
    right_phrase_op: bool,
) -> PgResult<()> {
    match st.q.item(st.cur) {
        Item::Val(op) => {
            out.push(b'\'');
            // operand is NUL-terminated in the pool; C walks to the NUL.
            push_escaped(out, st.q.operand_str(&op));
            out.push(b'\'');
            if op.weight != 0 || op.prefix {
                out.push(b':');
                if op.prefix {
                    out.push(b'*');
                }
                if op.weight & (1 << 3) != 0 {
                    out.push(b'A');
                }
                if op.weight & (1 << 2) != 0 {
                    out.push(b'B');
                }
                if op.weight & (1 << 1) != 0 {
                    out.push(b'C');
                }
                if op.weight & 1 != 0 {
                    out.push(b'D');
                }
            }
            st.cur += 1;
            Ok(())
        }
        Item::Opr(opr) if opr.oper == OP_NOT => {
            let priority = op_priority(OP_NOT);
            let paren = priority < parent_priority;
            if paren {
                out.extend_from_slice(b"( ");
            }
            out.push(b'!');
            st.cur += 1;
            infix(mcx, st, out, priority, false)?;
            if paren {
                out.extend_from_slice(b" )");
            }
            Ok(())
        }
        Item::Opr(opr) => {
            let priority = op_priority(opr.oper);
            let need_paren =
                priority < parent_priority || (opr.oper == OP_PHRASE && right_phrase_op);
            st.cur += 1;
            if need_paren {
                out.extend_from_slice(b"( ");
            }
            let mut nrm: PgVec<u8> = vec_with_capacity_in(mcx, 16)?;
            infix(mcx, st, &mut nrm, priority, opr.oper == OP_PHRASE)?;
            infix(mcx, st, out, priority, false)?;
            match opr.oper {
                OP_OR => out.extend_from_slice(b" | "),
                OP_AND => out.extend_from_slice(b" & "),
                OP_PHRASE => {
                    if opr.distance != 1 {
                        out.extend_from_slice(b" <");
                        push_i32_dec(out, opr.distance as i32);
                        out.extend_from_slice(b"> ");
                    } else {
                        out.extend_from_slice(b" <-> ");
                    }
                }
                other => panic!("unrecognized operator type: {other}"),
            }
            out.extend_from_slice(&nrm);
            if need_paren {
                out.extend_from_slice(b" )");
            }
            Ok(())
        }
        Item::ValStop => panic!("infix: QI_VALSTOP in stored tsquery"),
    }
}

pub fn tsquery_out_core<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<u8> = vec_with_capacity_in(mcx, q.payload.len() + 8)?;
    if q.size() != 0 {
        let mut st = Infix { q, cur: 0 };
        infix(mcx, &mut st, &mut out, -1, false)?;
    }
    out.push(0);
    Ok(out)
}

// tsquerytree body: clean_NOT then infix; empty text for empty query, "T" when
// the query degenerates.
pub fn tsquerytree_core<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut out: PgVec<u8> = PgVec::new_in(mcx);
    if q.size() == 0 {
        return Ok(out);
    }
    match crate::cleanup::clean_not(mcx, q)? {
        None => {
            out.push(b'T');
            Ok(out)
        }
        Some(items) => {
            let img = build_query_image(mcx, &items, q.operand_pool())?;
            let q2 = TsQueryRef { payload: &img[4..] };
            let mut st = Infix { q: q2, cur: 0 };
            infix(mcx, &mut st, &mut out, -1, false)?;
            Ok(out)
        }
    }
}

pub fn tsquery_send_core<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
) -> PgResult<::datum::Bytea<'mcx>> {
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, q.size() as u32)?;
    for i in 0..q.size() {
        match q.item(i) {
            Item::Val(op) => {
                ::pqformat::pq_sendint8(&mut buf, QI_VAL as u8)?;
                ::pqformat::pq_sendint8(&mut buf, op.weight)?;
                ::pqformat::pq_sendint8(&mut buf, op.prefix as u8)?;
                ::pqformat::pq_sendstring(&mut buf, q.operand_str(&op))?;
            }
            Item::Opr(opr) => {
                ::pqformat::pq_sendint8(&mut buf, QI_OPR as u8)?;
                ::pqformat::pq_sendint8(&mut buf, opr.oper as u8)?;
                if opr.oper == OP_PHRASE {
                    ::pqformat::pq_sendint16(&mut buf, opr.distance as u16)?;
                }
            }
            Item::ValStop => panic!("tsquerysend: QI_VALSTOP in stored tsquery"),
        }
    }
    Ok(::pqformat::pq_endtypsend(buf))
}

pub fn tsquery_recv_core<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &mut ::stringinfo::StringInfo<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let size = ::pqformat::pq_getmsgint(buf, 4)?;
    if size as usize > MAX_ALLOC_SIZE / QUERYITEM_SIZE {
        return Err(PgError::error("invalid size of tsquery").into());
    }
    let size = size as usize;
    let mut items: PgVec<Item> = vec_with_capacity_in(mcx, size)?;
    let mut pool: PgVec<u8> = PgVec::new_in(mcx);
    for i in 0..size {
        let typ = ::pqformat::pq_getmsgint(buf, 1)? as i8;
        if typ == QI_VAL {
            let weight = ::pqformat::pq_getmsgint(buf, 1)? as u8;
            let prefix = ::pqformat::pq_getmsgint(buf, 1)? as u8;
            let val = ::pqformat::pq_getmsgstring(mcx, buf)?;
            let val = val.as_bytes();
            if weight > 0xF {
                return Err(PgError::error("invalid tsquery: invalid weight bitmap").into());
            }
            if val.len() > MAXSTRLEN {
                return Err(PgError::error("invalid tsquery: operand too long").into());
            }
            if pool.len() > MAXSTRPOS {
                return Err(
                    PgError::error("invalid tsquery: total operand length exceeded").into()
                );
            }
            let valcrc = ::crc32c::legacy_crc32_lexeme(val) as i32;
            items.push(Item::Val(Operand {
                weight,
                prefix: prefix != 0,
                valcrc,
                length: val.len(),
                distance: pool.len(),
            }));
            let owned: &[u8] = val;
            let mut tmp = vec_with_capacity_in(mcx, owned.len())?;
            tmp.extend_from_slice(owned);
            ::mcx::vec_append_bytes(&mut pool, &tmp)?;
            pool.push(0);
        } else if typ == QI_OPR {
            let oper = ::pqformat::pq_getmsgint(buf, 1)? as i8;
            if oper != OP_NOT && oper != OP_OR && oper != OP_AND && oper != OP_PHRASE {
                return Err(PgError::error(format!(
                    "invalid tsquery: unrecognized operator type {oper}"
                ))
                .into());
            }
            if i == size - 1 {
                return Err(PgError::error("invalid pointer to right operand").into());
            }
            let distance = if oper == OP_PHRASE {
                ::pqformat::pq_getmsgint(buf, 2)? as i16
            } else {
                0
            };
            items.push(Item::Opr(Operator { oper, distance, left: 0 }));
        } else {
            return Err(PgError::error(format!("unrecognized tsquery node type: {typ}")).into());
        }
    }

    let mut needcleanup = false;
    findoprnd(&mut items, &mut needcleanup)?;
    debug_assert!(!needcleanup);
    build_query_image(mcx, &items, &pool)
}

pub fn compare_tsq(a: TsQueryRef<'_>, b: TsQueryRef<'_>, mcx: Mcx<'_>) -> PgResult<i32> {
    if a.size() != b.size() {
        return Ok(if a.size() < b.size() { -1 } else { 1 });
    }
    if a.payload.len() != b.payload.len() {
        return Ok(if a.payload.len() < b.payload.len() { -1 } else { 1 });
    }
    if a.size() != 0 {
        let an = crate::util::qt2qtn(mcx, a, 0)?;
        let bn = crate::util::qt2qtn(mcx, b, 0)?;
        return Ok(crate::util::qtnode_compare(&an, &bn));
    }
    Ok(0)
}

// collectTSQueryValues + sort/unique, for tsq_mcontains.
pub fn collect_values<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
) -> PgResult<PgVec<'mcx, PgVec<'mcx, u8>>> {
    let mut vals: PgVec<PgVec<u8>> = PgVec::new_in(mcx);
    vals.try_reserve_exact(q.size()).map_err(|_| mcx.oom(q.size()))?;
    for i in 0..q.size() {
        if let Item::Val(op) = q.item(i) {
            let mut v = vec_with_capacity_in(mcx, op.length)?;
            v.extend_from_slice(q.operand_str(&op));
            vals.push(v);
        }
    }
    vals.sort_by(|a, b| a.as_slice().cmp(b.as_slice()));
    vals.dedup_by(|a, b| a.as_slice() == b.as_slice());
    Ok(vals)
}

pub fn tsq_mcontains_core(
    mcx: Mcx<'_>,
    query: TsQueryRef<'_>,
    ex: TsQueryRef<'_>,
) -> PgResult<bool> {
    let qv = collect_values(mcx, query)?;
    let ev = collect_values(mcx, ex)?;
    if ev.len() > qv.len() {
        return Ok(false);
    }
    let mut j = 0usize;
    for e in &ev {
        while j < qv.len() && qv[j].as_slice() != e.as_slice() {
            j += 1;
        }
        if j == qv.len() {
            return Ok(false);
        }
    }
    Ok(true)
}

