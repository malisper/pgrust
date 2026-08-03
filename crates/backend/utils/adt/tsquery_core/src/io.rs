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

// C tsquery.c INFIX buffer. C grows the infix() output via RESIZEBUF, which
// doubles buflen (initial 32 for the top-level buffer, 16 for an operator's
// sub-buffer) and repallocs; the first doubled buflen above MaxAllocSize
// makes repalloc raise the CATCHABLE "invalid memory alloc request size
// {buflen}" (the doubling powers reach 2^30 = 1073741824 > 0x3FFF_FFFF, so
// the error fires once the needed output crosses 2^29). This port tracks C's
// buflen and runs the same check at the same call sites with the same
// addsize expressions; without it, PgVec's infallible growth would hit the
// allocator ceiling and abort the process where C raises an error.
struct OutBuf<'mcx> {
    v: PgVec<'mcx, u8>,
    buflen: usize,
}

impl<'mcx> OutBuf<'mcx> {
    // C RESIZEBUF(inf, addsize): capacity stays >= buflen > len + addsize + 1,
    // so the pushes an addsize covers never regrow the vec.
    fn resize(&mut self, addsize: usize) -> PgResult<()> {
        while self.v.len() + addsize + 1 >= self.buflen {
            self.buflen *= 2;
            // C: repalloc(buf, buflen) refuses an over-MaxAllocSize request.
            ::mcx::check_alloc_size(self.buflen)?;
        }
        if self.buflen > self.v.capacity() {
            let mcx = *self.v.allocator();
            let add = self.buflen - self.v.len();
            self.v.try_reserve_exact(add).map_err(|_| mcx.oom(self.buflen))?;
        }
        Ok(())
    }
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
    out: &mut OutBuf<'mcx>,
    parent_priority: i32,
    right_phrase_op: bool,
) -> PgResult<()> {
    // C tsquery.c infix(): check_stack_depth(). One frame per tree level.
    ::stack_depth::check_stack_depth()?;
    match st.q.item(st.cur) {
        Item::Val(op) => {
            // C: RESIZEBUF(in, curpol->length * (pg_database_encoding_max_length() + 1) + 2 + 6)
            out.resize(
                op.length * (::mbutils::pg_database_encoding_max_length() as usize + 1) + 2 + 6,
            )?;
            out.v.push(b'\'');
            // operand is NUL-terminated in the pool; C walks to the NUL.
            push_escaped(&mut out.v, st.q.operand_str(&op));
            out.v.push(b'\'');
            if op.weight != 0 || op.prefix {
                out.v.push(b':');
                if op.prefix {
                    out.v.push(b'*');
                }
                if op.weight & (1 << 3) != 0 {
                    out.v.push(b'A');
                }
                if op.weight & (1 << 2) != 0 {
                    out.v.push(b'B');
                }
                if op.weight & (1 << 1) != 0 {
                    out.v.push(b'C');
                }
                if op.weight & 1 != 0 {
                    out.v.push(b'D');
                }
            }
            st.cur += 1;
            Ok(())
        }
        Item::Opr(opr) if opr.oper == OP_NOT => {
            let priority = op_priority(OP_NOT);
            let paren = priority < parent_priority;
            if paren {
                out.resize(2)?;
                out.v.extend_from_slice(b"( ");
            }
            out.resize(1)?;
            out.v.push(b'!');
            st.cur += 1;
            infix(mcx, st, out, priority, false)?;
            if paren {
                out.resize(2)?;
                out.v.extend_from_slice(b" )");
            }
            Ok(())
        }
        Item::Opr(opr) => {
            let priority = op_priority(opr.oper);
            let need_paren =
                priority < parent_priority || (opr.oper == OP_PHRASE && right_phrase_op);
            st.cur += 1;
            if need_paren {
                out.resize(2)?;
                out.v.extend_from_slice(b"( ");
            }
            // C: nrm.buflen = 16; nrm.buf = palloc(nrm.buflen)
            let mut nrm = OutBuf { v: vec_with_capacity_in(mcx, 16)?, buflen: 16 };
            infix(mcx, st, &mut nrm, priority, opr.oper == OP_PHRASE)?;
            infix(mcx, st, out, priority, false)?;
            // C: RESIZEBUF(in, 3 + (2 + 10 /* distance */) + (nrm.cur - nrm.buf))
            out.resize(3 + (2 + 10) + nrm.v.len())?;
            match opr.oper {
                OP_OR => out.v.extend_from_slice(b" | "),
                OP_AND => out.v.extend_from_slice(b" & "),
                OP_PHRASE => {
                    if opr.distance != 1 {
                        out.v.extend_from_slice(b" <");
                        push_i32_dec(&mut out.v, opr.distance as i32);
                        out.v.extend_from_slice(b"> ");
                    } else {
                        out.v.extend_from_slice(b" <-> ");
                    }
                }
                other => panic!("unrecognized operator type: {other}"),
            }
            out.v.extend_from_slice(&nrm.v);
            if need_paren {
                out.resize(2)?;
                out.v.extend_from_slice(b" )");
            }
            Ok(())
        }
        Item::ValStop => panic!("infix: QI_VALSTOP in stored tsquery"),
    }
}

pub fn tsquery_out_core<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>) -> PgResult<PgVec<'mcx, u8>> {
    // C: nrm.buflen = 32. The vec's larger initial reserve is a perf carve;
    // the ceiling ledger stays C's.
    let mut out = OutBuf { v: vec_with_capacity_in(mcx, q.payload.len() + 8)?, buflen: 32 };
    if q.size() != 0 {
        let mut st = Infix { q, cur: 0 };
        infix(mcx, &mut st, &mut out, -1, false)?;
    }
    let mut out = out.v;
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
            // C tsquerytree: nrm.buflen = 32.
            let mut buf = OutBuf { v: out, buflen: 32 };
            infix(mcx, &mut st, &mut buf, -1, false)?;
            Ok(buf.v)
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
        return crate::util::qtnode_compare(&an, &bn);
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
    // C (tsquery_op.c:322-326) pg_qsorts char* by strcmp then quniques.
    // Ties are byte-identical strings and only string CONTENT is ever
    // observed downstream (strcmp at :339), so unstable-vs-stable tie
    // order is a non-surface here; a stable sort is C-equivalent.
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

