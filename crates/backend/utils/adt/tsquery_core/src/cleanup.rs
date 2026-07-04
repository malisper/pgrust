use ::adt_tsvector_core::query::*;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_error::{PgError, PgResult};

// tsquery_cleanup.c NODE; std Box justified: transient per-parse tree,
// C pallocs one node per QueryItem and frees the whole set on return.
enum Node {
    Leaf(Item),
    Op {
        item: Operator,
        left: Option<Box<Node>>,
        right: Box<Node>,
    },
}

fn maketree(items: &[Item], pos: &mut usize) -> Box<Node> {
    let it = items[*pos];
    *pos += 1;
    match it {
        Item::Opr(opr) => {
            let right = maketree(items, pos);
            let left = if opr.oper != OP_NOT {
                Some(maketree(items, pos))
            } else {
                None
            };
            // NODE.left/right mirror C: right = in+1, left = in+left.
            Box::new(Node::Op { item: opr, left, right })
        }
        other => Box::new(Node::Leaf(other)),
    }
}

fn plainnode(out: &mut PgVec<'_, Item>, node: &Node) {
    match node {
        Node::Leaf(it) => out.push(*it),
        Node::Op { item, left, right } => {
            if item.oper == OP_NOT {
                out.push(Item::Opr(Operator { left: 1, ..*item }));
                plainnode(out, right);
            } else {
                let cur = out.len();
                out.push(Item::Opr(*item));
                plainnode(out, right);
                let l = (out.len() - cur) as u32;
                if let Item::Opr(ref mut o) = out[cur] {
                    o.left = l;
                }
                plainnode(out, left.as_ref().expect("binary operator has left"));
            }
        }
    }
}

fn plaintree<'mcx>(mcx: Mcx<'mcx>, root: Option<&Node>) -> PgResult<PgVec<'mcx, Item>> {
    let mut out: PgVec<Item> = vec_with_capacity_in(mcx, 16)?;
    if let Some(root) = root {
        plainnode(&mut out, root);
    }
    Ok(out)
}

fn clean_not_intree(node: Box<Node>) -> Option<Box<Node>> {
    match *node {
        Node::Leaf(Item::Val(_)) => Some(node),
        Node::Leaf(_) => Some(node),
        Node::Op { item, left, right } => {
            if item.oper == OP_NOT {
                return None;
            }
            if item.oper == OP_OR {
                let l = clean_not_intree(left.expect("OR has left"))?;
                let r = clean_not_intree(right)?;
                Some(Box::new(Node::Op { item, left: Some(l), right: r }))
            } else {
                let l = left.and_then(clean_not_intree);
                let r = clean_not_intree(right);
                match (l, r) {
                    (None, None) => None,
                    (Some(l), None) => Some(l),
                    (None, Some(r)) => Some(r),
                    (Some(l), Some(r)) => {
                        Some(Box::new(Node::Op { item, left: Some(l), right: r }))
                    }
                }
            }
        }
    }
}

// clean_NOT: strip NOT subtrees; None = query degenerates to nothing.
pub fn clean_not<'mcx>(
    mcx: Mcx<'mcx>,
    q: TsQueryRef<'_>,
) -> PgResult<Option<PgVec<'mcx, Item>>> {
    let mut items: PgVec<Item> = vec_with_capacity_in(mcx, q.size())?;
    for i in 0..q.size() {
        items.push(q.item(i));
    }
    let mut pos = 0usize;
    let root = maketree(&items, &mut pos);
    match clean_not_intree(root) {
        None => Ok(None),
        Some(root) => Ok(Some(plaintree(mcx, Some(root.as_ref()))?)),
    }
}

fn clean_stopword_intree(
    node: Box<Node>,
    ladd: &mut i32,
    radd: &mut i32,
) -> Option<Box<Node>> {
    *ladd = 0;
    *radd = 0;
    match *node {
        Node::Leaf(Item::Val(_)) => Some(node),
        Node::Leaf(Item::ValStop) => None,
        Node::Leaf(Item::Opr(_)) => unreachable!("operator leaf"),
        Node::Op { mut item, left, right } => {
            if item.oper == OP_NOT {
                let r = clean_stopword_intree(right, ladd, radd)?;
                Some(Box::new(Node::Op { item, left: None, right: r }))
            } else {
                let (mut lladd, mut lradd, mut rladd, mut rradd) = (0, 0, 0, 0);
                let l = left
                    .expect("binary operator has left")
                    .into_clean(&mut lladd, &mut lradd);
                let r = right.into_clean(&mut rladd, &mut rradd);

                let isphrase = item.oper == OP_PHRASE;
                let ndistance = if isphrase { item.distance as i32 } else { 0 };

                match (l, r) {
                    (None, None) => {
                        let v = if isphrase {
                            lladd + ndistance + rladd
                        } else {
                            lladd.max(rladd)
                        };
                        *ladd = v;
                        *radd = v;
                        None
                    }
                    (None, Some(r)) => {
                        if isphrase {
                            *ladd = lladd + ndistance + rladd;
                            *radd = rradd;
                        } else {
                            *ladd = rladd;
                            *radd = rradd;
                        }
                        Some(r)
                    }
                    (Some(l), None) => {
                        if isphrase {
                            *ladd = lladd;
                            *radd = lradd + ndistance + rradd;
                        } else {
                            *ladd = lladd;
                            *radd = lradd;
                        }
                        Some(l)
                    }
                    (Some(l), Some(r)) => {
                        if isphrase {
                            item.distance =
                                (item.distance as i32 + lradd + rladd) as i16;
                            *ladd = lladd;
                            *radd = rradd;
                        }
                        Some(Box::new(Node::Op { item, left: Some(l), right: r }))
                    }
                }
            }
        }
    }
}

impl Node {
    fn into_clean(self: Box<Self>, ladd: &mut i32, radd: &mut i32) -> Option<Box<Node>> {
        clean_stopword_intree(self, ladd, radd)
    }
}

// cleanup_tsquery_stopwords over a flat image; returns the rebuilt image.
pub fn cleanup_tsquery_stopwords<'mcx>(
    mcx: Mcx<'mcx>,
    img: &[u8],
    noisy: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let q = TsQueryRef { payload: &img[4..] };
    if q.size() == 0 {
        let mut out = vec_with_capacity_in(mcx, img.len())?;
        ::mcx::vec_append_bytes(&mut out, img)?;
        return Ok(out);
    }

    let mut items: PgVec<Item> = vec_with_capacity_in(mcx, q.size())?;
    for i in 0..q.size() {
        items.push(q.item(i));
    }
    let mut pos = 0usize;
    let root = maketree(&items, &mut pos);
    let (mut ladd, mut radd) = (0, 0);
    let root = clean_stopword_intree(root, &mut ladd, &mut radd);
    let Some(root) = root else {
        if noisy {
            ::elog::ThrowErrorData(PgError::notice(
                "text-search query contains only stop words or doesn't contain lexemes, ignored",
            ))?;
        }
        return crate::parse::build_query_image(mcx, &[], &[]);
    };

    let out_items = plaintree(mcx, Some(root.as_ref()))?;
    let old_pool = q.operand_pool();
    let mut new_items: PgVec<Item> = vec_with_capacity_in(mcx, out_items.len())?;
    let mut pool: PgVec<u8> = PgVec::new_in(mcx);
    for it in &out_items {
        match it {
            Item::Val(op) => {
                let mut op2 = *op;
                op2.distance = pool.len();
                ::mcx::vec_append_bytes(
                    &mut pool,
                    &old_pool[op.distance..op.distance + op.length],
                )?;
                pool.push(0);
                new_items.push(Item::Val(op2));
            }
            other => new_items.push(*other),
        }
    }
    crate::parse::build_query_image(mcx, &new_items, &pool)
}
