use ::adt_tsvector_core::layout::ts_compare_string;
use ::adt_tsvector_core::query::*;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

// tsquery_util.c QTNode as an owned tree; `word` is the operand's bytes.
pub struct QtNode<'mcx> {
    pub item: Item,
    pub word: PgVec<'mcx, u8>,
    pub sign: u32,
    pub flags: u32,
    pub children: PgVec<'mcx, QtNode<'mcx>>,
}

// QTN_NOCHANGE (ts_utils.h): findsubquery must not recurse into a
// just-substituted node.
pub const QTN_NOCHANGE: u32 = 0x01;

// QT2QTN over a flat query payload.
pub fn qt2qtn<'mcx>(mcx: Mcx<'mcx>, q: TsQueryRef<'_>, idx: usize) -> PgResult<QtNode<'mcx>> {
    match q.item(idx) {
        Item::Opr(opr) => {
            let mut children: PgVec<QtNode> = PgVec::new_in(mcx);
            children.try_reserve_exact(2).map_err(|_| mcx.oom(2))?;
            children.push(qt2qtn(mcx, q, idx + 1)?);
            let mut sign = children[0].sign;
            if opr.oper != OP_NOT {
                children.push(qt2qtn(mcx, q, idx + opr.left as usize)?);
                sign |= children[1].sign;
            }
            Ok(QtNode {
                item: Item::Opr(opr),
                word: PgVec::new_in(mcx),
                sign,
                flags: 0,
                children,
            })
        }
        Item::Val(op) => {
            let mut word = vec_with_capacity_in(mcx, op.length)?;
            word.extend_from_slice(q.operand_str(&op));
            Ok(QtNode {
                item: Item::Val(op),
                word,
                sign: 1u32 << ((op.valcrc as u32) % 32),
                flags: 0,
                children: PgVec::new_in(mcx),
            })
        }
        Item::ValStop => Ok(QtNode {
            item: Item::ValStop,
            word: PgVec::new_in(mcx),
            sign: 0,
            flags: 0,
            children: PgVec::new_in(mcx),
        }),
    }
}

pub fn qtnode_compare(a: &QtNode<'_>, b: &QtNode<'_>) -> i32 {
    match (&a.item, &b.item) {
        (Item::Opr(ao), Item::Opr(bo)) => {
            if ao.oper != bo.oper {
                return if ao.oper > bo.oper { -1 } else { 1 };
            }
            if a.children.len() != b.children.len() {
                return if a.children.len() > b.children.len() { -1 } else { 1 };
            }
            for (ca, cb) in a.children.iter().zip(b.children.iter()) {
                let res = qtnode_compare(ca, cb);
                if res != 0 {
                    return res;
                }
            }
            if ao.oper == OP_PHRASE && ao.distance != bo.distance {
                return if ao.distance > bo.distance { -1 } else { 1 };
            }
            0
        }
        (Item::Val(ao), Item::Val(bo)) => {
            if ao.valcrc != bo.valcrc {
                return if ao.valcrc > bo.valcrc { -1 } else { 1 };
            }
            ts_compare_string(&a.word, &b.word, false)
        }
        (ai, bi) => {
            let ta = item_type(ai);
            let tb = item_type(bi);
            if ta > tb {
                -1
            } else {
                1
            }
        }
    }
}

fn item_type(i: &Item) -> i8 {
    match i {
        Item::Val(_) => QI_VAL,
        Item::Opr(_) => QI_OPR,
        Item::ValStop => QI_VALSTOP,
    }
}

pub fn qtn_sort(n: &mut QtNode<'_>) {
    let Item::Opr(opr) = n.item else { return };
    for c in n.children.iter_mut() {
        qtn_sort(c);
    }
    if n.children.len() > 1 && opr.oper != OP_PHRASE {
        n.children.sort_by(|a, b| match qtnode_compare(a, b) {
            x if x < 0 => core::cmp::Ordering::Less,
            0 => core::cmp::Ordering::Equal,
            _ => core::cmp::Ordering::Greater,
        });
    }
}

pub fn qtn_eq(a: &QtNode<'_>, b: &QtNode<'_>) -> bool {
    let sign = a.sign & b.sign;
    if !(sign == a.sign && sign == b.sign) {
        return false;
    }
    qtnode_compare(a, b) == 0
}

// QTNTernary: flatten nested same-operator AND/OR children.
pub fn qtn_ternary(n: &mut QtNode<'_>) {
    let Item::Opr(opr) = n.item else { return };
    for c in n.children.iter_mut() {
        qtn_ternary(c);
    }
    if opr.oper != OP_AND && opr.oper != OP_OR {
        return;
    }
    let mut i = 0usize;
    while i < n.children.len() {
        let same = matches!(n.children[i].item, Item::Opr(co) if co.oper == opr.oper);
        if same {
            let cc = n.children.remove(i);
            let ncc = cc.children.len();
            let mut k = i;
            for gc in cc.children {
                n.children.insert(k, gc);
                k += 1;
            }
            i += ncc;
        } else {
            i += 1;
        }
    }
}

// QTNBinary: rebuild >2-ary nodes as left-deep binaries.
pub fn qtn_binary<'mcx>(mcx: Mcx<'mcx>, n: &mut QtNode<'mcx>) {
    let Item::Opr(opr) = n.item else { return };
    for c in n.children.iter_mut() {
        qtn_binary(mcx, c);
    }
    // C keeps child[2..] in place: children become [nn, old_last, old[2..-1]].
    while n.children.len() > 2 {
        let dummy = || QtNode {
            item: Item::ValStop,
            word: PgVec::new_in(mcx),
            sign: 0,
            flags: 0,
            children: PgVec::new_in(mcx),
        };
        let c0 = core::mem::replace(&mut n.children[0], dummy());
        let c1 = core::mem::replace(&mut n.children[1], dummy());
        let sign = c0.sign | c1.sign;
        let mut sub: PgVec<QtNode> = PgVec::new_in(mcx);
        sub.try_reserve_exact(2).map_err(|_| mcx.oom(2)).expect("qtn_binary alloc");
        sub.push(c0);
        sub.push(c1);
        let nn = QtNode {
            item: Item::Opr(Operator { oper: opr.oper, distance: 0, left: 0 }),
            word: PgVec::new_in(mcx),
            sign,
            flags: 0,
            children: sub,
        };
        let last = n.children.pop().expect("len > 2");
        n.children[1] = last;
        n.children[0] = nn;
    }
}

// QTNCopy.
pub fn qtn_copy<'mcx>(mcx: Mcx<'mcx>, n: &QtNode<'_>) -> PgResult<QtNode<'mcx>> {
    let mut word = vec_with_capacity_in(mcx, n.word.len())?;
    word.extend_from_slice(&n.word);
    let mut children: PgVec<'mcx, QtNode<'mcx>> = PgVec::new_in(mcx);
    children.try_reserve_exact(n.children.len()).map_err(|_| mcx.oom(n.children.len()))?;
    for c in n.children.iter() {
        children.push(qtn_copy(mcx, c)?);
    }
    Ok(QtNode { item: n.item, word, sign: n.sign, flags: n.flags, children })
}

// QTNClearFlags.
pub fn qtn_clear_flags(n: &mut QtNode<'_>, flags: u32) {
    n.flags &= !flags;
    for c in n.children.iter_mut() {
        qtn_clear_flags(c, flags);
    }
}

fn cntsize(n: &QtNode<'_>, sumlen: &mut usize, nnode: &mut usize) {
    *nnode += 1;
    match &n.item {
        Item::Opr(_) => {
            for c in &n.children {
                cntsize(c, sumlen, nnode);
            }
        }
        Item::Val(op) => {
            *sumlen += op.length + 1;
        }
        Item::ValStop => {}
    }
}

struct FillState<'mcx> {
    items: PgVec<'mcx, Item>,
    pool: PgVec<'mcx, u8>,
}

fn fill_qt(st: &mut FillState<'_>, n: &QtNode<'_>) -> PgResult<()> {
    match n.item {
        Item::Val(mut op) => {
            op.distance = st.pool.len();
            ::mcx::vec_append_bytes(&mut st.pool, &n.word)?;
            st.pool.push(0);
            st.items.push(Item::Val(op));
            Ok(())
        }
        Item::Opr(opr) => {
            let cur = st.items.len();
            st.items.push(Item::Opr(opr));
            fill_qt(st, &n.children[0])?;
            if n.children.len() == 2 {
                let left = (st.items.len() - cur) as u32;
                if let Item::Opr(ref mut o) = st.items[cur] {
                    o.left = left;
                }
                fill_qt(st, &n.children[1])?;
            }
            Ok(())
        }
        Item::ValStop => panic!("fill_qt: QI_VALSTOP node"),
    }
}

// QTN2QT: flatten a QtNode tree back to a query image.
pub fn qtn2qt<'mcx>(mcx: Mcx<'mcx>, n: &QtNode<'_>) -> PgResult<PgVec<'mcx, u8>> {
    let mut sumlen = 0usize;
    let mut nnode = 0usize;
    cntsize(n, &mut sumlen, &mut nnode);
    if nnode > (MAX_ALLOC_SIZE - HDRSIZETQ - sumlen) / QUERYITEM_SIZE {
        return Err(PgError::error("tsquery is too large")
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .into());
    }
    let mut st = FillState {
        items: vec_with_capacity_in(mcx, nnode)?,
        pool: vec_with_capacity_in(mcx, sumlen)?,
    };
    fill_qt(&mut st, n)?;
    crate::parse::build_query_image(mcx, &st.items, &st.pool)
}
