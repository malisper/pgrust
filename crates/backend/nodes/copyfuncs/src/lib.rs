//! copyfuncs.c copyObject. Arms for every node in C 18.3's copy switch whose
//! struct exists in the types_nodes vocabulary live in `generated` (the Rust
//! analog of the generated copyfuncs.funcs.c; see generate.py). Const and
//! A_Const are hand-written here, as in C's copyfuncs.c. Tags with no
//! vocabulary struct fall back to the outfuncs/readfuncs round trip, which is
//! loud for anything unported.

#![allow(non_snake_case)]

mod generated;

use datum::Datum;
use mcx::Mcx;
use types_error::PgResult;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::primnodes::Const;
use types_nodes::rawnodes::{A_Const, A_Star, ValUnion};
use types_nodes::{Node, NodeTag};
use types_tuple::varatt::varsize_any;

pub fn copy_object<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    copy_node(mcx, node)
}

pub fn copy_query<'d>(mcx: Mcx<'d>, src: &types_nodes::parsenodes::Query<'_>) -> PgResult<types_nodes::parsenodes::Query<'d>> {
    generated::copy_Query(mcx, src)
}

pub fn copy_utility_planned_stmt<'d>(
    mcx: Mcx<'d>,
    src: &PlannedStmt<'_>,
) -> PgResult<&'d PlannedStmt<'d>> {
    let copy = generated::copy_PlannedStmt(mcx, src)?;
    Ok(Node::mk(mcx, copy)?.as_planned_stmt().expect("PlannedStmt"))
}

pub(crate) fn copy_node<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    match node.node_tag() {
        NodeTag::T_String => {
            let s = node.as_string().expect("String");
            Node::mk(mcx, types_nodes::String { sval: str_in(mcx, s.sval)? })
        }
        NodeTag::T_Integer => {
            let i = node.as_integer().expect("Integer");
            Node::mk(mcx, types_nodes::Integer { ival: i.ival })
        }
        NodeTag::T_Float => {
            let f = node.as_float().expect("Float");
            Node::mk(mcx, types_nodes::Float { fval: str_in(mcx, f.fval)? })
        }
        NodeTag::T_Boolean => {
            let b = node.as_boolean().expect("Boolean");
            Node::mk(mcx, types_nodes::Boolean { boolval: b.boolval })
        }
        NodeTag::T_BitString => {
            let b = node.as_bitstring().expect("BitString");
            Node::mk(mcx, types_nodes::BitString { bsval: str_in(mcx, b.bsval)? })
        }
        NodeTag::T_List => {
            let l = node.as_list().expect("List");
            // Exact-length preallocation (C list_copy's new_list sizing).
            let mut out = types_nodes::NodeList::with_capacity(mcx, l.len())?;
            for cell in l.iter() {
                out.lappend(mcx, copy_node(mcx, cell)?)?;
            }
            Node::mk_list(mcx, out)
        }
        NodeTag::T_IntList => {
            let l = node.as_int_list().expect("IntList");
            Node::mk_int_list(mcx, types_nodes::list::IntList::from_slice(mcx, l.as_slice())?)
        }
        NodeTag::T_OidList => {
            let l = node.as_oid_list().expect("OidList");
            Node::mk_oid_list(mcx, types_nodes::list::OidList::from_slice(mcx, l.as_slice())?)
        }
        NodeTag::T_XidList => {
            let l = node.as_xid_list().expect("XidList");
            Node::mk_xid_list(mcx, types_nodes::list::XidList::from_slice(mcx, l.as_slice())?)
        }
        NodeTag::T_Bitmapset => {
            let b = node.as_bitmapset().expect("Bitmapset");
            Node::mk_bitmapset(mcx, generated::copy_bms(mcx, b)?)
        }
        NodeTag::T_A_Star => Node::mk(mcx, A_Star),
        NodeTag::T_Const => {
            let c = node.as_variant::<Const>().expect("Const");
            let mut copy = *c;
            if !c.constisnull && !c.constbyval {
                copy.constvalue = datum_copy_in(mcx, c.constvalue, c.constlen)?;
            }
            Node::mk(mcx, copy)
        }
        NodeTag::T_A_Const => {
            let c = node.as_variant::<A_Const>().expect("A_Const");
            let val = match &c.val {
                Some(v) => Some(copy_val(mcx, v)?),
                None => None,
            };
            Node::mk(mcx, A_Const { val, location: c.location })
        }
        _ => match generated::copy_generated(mcx, node)? {
            Some(copy) => Ok(copy),
            None => copy_via_out_read(mcx, node),
        },
    }
}

// datumCopy (datum.c) for a by-ref Const value: constlen -1 varlena, -2
// NUL-terminated cstring, else fixed length.
fn datum_copy_in<'d>(mcx: Mcx<'d>, d: Datum, typlen: i32) -> PgResult<Datum> {
    let p = d.as_usize() as *const u8;
    // SAFETY: a by-ref non-null Const holds a live datum image of the layout
    // constlen describes (makeConst invariant).
    let size = unsafe {
        match typlen {
            -1 => varsize_any(p),
            -2 => {
                let mut n = 0usize;
                while *p.add(n) != 0 {
                    n += 1;
                }
                n + 1
            }
            l => {
                debug_assert!(l > 0);
                l as usize
            }
        }
    };
    // SAFETY: `size` readable bytes at `p` per the invariant above.
    let src = unsafe { core::slice::from_raw_parts(p, size) };
    Ok(Datum::from_usize(mcx::slice_in(mcx, src)?.leak().as_ptr() as usize))
}

fn copy_val<'d>(mcx: Mcx<'d>, v: &ValUnion<'_>) -> PgResult<ValUnion<'d>> {
    Ok(match v {
        ValUnion::Integer(i) => ValUnion::Integer(types_nodes::Integer { ival: i.ival }),
        ValUnion::Float(f) => ValUnion::Float(types_nodes::Float { fval: str_in(mcx, f.fval)? }),
        ValUnion::Boolean(b) => ValUnion::Boolean(types_nodes::Boolean { boolval: b.boolval }),
        ValUnion::String(s) => ValUnion::String(types_nodes::String { sval: str_in(mcx, s.sval)? }),
        ValUnion::BitString(b) => {
            ValUnion::BitString(types_nodes::BitString { bsval: str_in(mcx, b.bsval)? })
        }
    })
}

// Tags with no vocabulary struct (C copies 39 more node types than the
// carried vocabulary holds). outfuncs/readfuncs is loud for anything neither
// side supports, naming the node.
fn copy_via_out_read<'d>(mcx: Mcx<'d>, node: Node<'_>) -> PgResult<Node<'d>> {
    // SAFETY: nodeToString only reads the tree; the unified handle does not
    // outlive the serialize call.
    let node = unsafe { core::mem::transmute::<Node<'_>, Node<'d>>(node) };
    let s = outfuncs::nodeToString(mcx, node)?;
    readfuncs::stringToNode(mcx, s.as_str())
}

pub(crate) fn str_in<'d>(mcx: Mcx<'d>, s: &str) -> PgResult<&'d str> {
    let v = mcx::slice_in(mcx, s.as_bytes())?;
    // SAFETY: the bytes are a verbatim copy of a &str (slice_in memcpy);
    // re-validating UTF-8 per copied string was pure per-replan tax.
    Ok(unsafe { core::str::from_utf8_unchecked(v.leak()) })
}

pub(crate) fn opt_str_in<'d>(mcx: Mcx<'d>, s: Option<&str>) -> PgResult<Option<&'d str>> {
    match s {
        Some(s) => Ok(Some(str_in(mcx, s)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests;
