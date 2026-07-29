use crate::container::*;
use mcx::{Mcx, PgVec};
use types_error::PgResult;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum WjbToken {
    Done,
    Key,
    Value,
    Elem,
    BeginArray,
    EndArray,
    BeginObject,
    EndObject,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum IterState {
    ArrayStart,
    ArrayElem,
    ObjectStart,
    ObjectKey,
    ObjectValue,
}

#[derive(Clone, Copy)]
struct Frame<'a> {
    container: &'a [u8],
    n_elems: u32,
    is_scalar: bool,
    base_off: u32,
    cur_index: u32,
    cur_data_offset: u32,
    cur_value_offset: u32,
    state: IterState,
}

/// One advance of a single nesting level's state machine — the loop body of
/// C's JsonbIteratorNext, minus the frame-stack push/pop, which the two
/// iterator shells (grown vec / fixed inline array) each apply themselves.
enum Step<'a> {
    /// Return this token/value to the caller.
    Emit(WjbToken, JsonbItem<'a>),
    /// This level is exhausted: pop it and return the end token (value Null).
    Pop(WjbToken),
    /// Descend into a nested container (C: iteratorFromContainer push).
    Recurse(&'a [u8]),
}

impl<'a> Frame<'a> {
    // C: iteratorFromContainer.
    fn from_container(container: &'a [u8]) -> Frame<'a> {
        let n_elems = container_size(container);
        let header = container_header(container);
        let (state, base_off, is_scalar) = match header & (JB_FARRAY | JB_FOBJECT) {
            JB_FARRAY => (
                IterState::ArrayStart,
                4 + 4 * n_elems,
                container_is_scalar(container),
            ),
            JB_FOBJECT => (IterState::ObjectStart, 4 + 8 * n_elems, false),
            _ => panic!("unknown type of jsonb container"),
        };
        Frame {
            container,
            n_elems,
            is_scalar,
            base_off,
            cur_index: 0,
            cur_data_offset: 0,
            cur_value_offset: 0,
            state,
        }
    }

    fn step(&mut self, skip_nested: bool) -> Step<'a> {
        match self.state {
            IterState::ArrayStart => {
                let item = JsonbItem::Array {
                    n_elems: self.n_elems,
                    raw_scalar: self.is_scalar,
                };
                self.cur_index = 0;
                self.cur_data_offset = 0;
                self.cur_value_offset = 0;
                self.state = IterState::ArrayElem;
                Step::Emit(WjbToken::BeginArray, item)
            }
            IterState::ArrayElem => {
                if self.cur_index >= self.n_elems {
                    return Step::Pop(WjbToken::EndArray);
                }
                let val = fill_item(
                    self.container,
                    self.cur_index,
                    self.base_off,
                    self.cur_data_offset,
                );
                jbe_advance_offset(
                    &mut self.cur_data_offset,
                    child_jentry(self.container, self.cur_index),
                );
                self.cur_index += 1;
                if let JsonbItem::Binary(child) = val {
                    if !skip_nested {
                        return Step::Recurse(child);
                    }
                }
                Step::Emit(WjbToken::Elem, val)
            }
            IterState::ObjectStart => {
                let item = JsonbItem::Object {
                    n_pairs: self.n_elems,
                };
                self.cur_index = 0;
                self.cur_data_offset = 0;
                self.cur_value_offset = get_jsonb_offset(self.container, self.n_elems);
                self.state = IterState::ObjectKey;
                Step::Emit(WjbToken::BeginObject, item)
            }
            IterState::ObjectKey => {
                if self.cur_index >= self.n_elems {
                    return Step::Pop(WjbToken::EndObject);
                }
                let val = fill_item(
                    self.container,
                    self.cur_index,
                    self.base_off,
                    self.cur_data_offset,
                );
                if !matches!(val, JsonbItem::String(_)) {
                    panic!("unexpected jsonb type as object key");
                }
                self.state = IterState::ObjectValue;
                Step::Emit(WjbToken::Key, val)
            }
            IterState::ObjectValue => {
                self.state = IterState::ObjectKey;
                let val = fill_item(
                    self.container,
                    self.cur_index + self.n_elems,
                    self.base_off,
                    self.cur_value_offset,
                );
                jbe_advance_offset(
                    &mut self.cur_data_offset,
                    child_jentry(self.container, self.cur_index),
                );
                jbe_advance_offset(
                    &mut self.cur_value_offset,
                    child_jentry(self.container, self.cur_index + self.n_elems),
                );
                self.cur_index += 1;
                if let JsonbItem::Binary(child) = val {
                    if !skip_nested {
                        return Step::Recurse(child);
                    }
                }
                Step::Emit(WjbToken::Value, val)
            }
        }
    }
}

/// C: JsonbIterator — the parent chain is a frame stack (C pallocs one
/// iterator per nesting level; one grown vec matches that cost wholesale).
pub struct JsonbIterator<'a, 'mcx> {
    stack: PgVec<'mcx, Frame<'a>>,
}

impl<'a, 'mcx> JsonbIterator<'a, 'mcx> {
    pub fn init(mcx: Mcx<'mcx>, container: &'a [u8]) -> PgResult<JsonbIterator<'a, 'mcx>> {
        let mut stack: PgVec<'mcx, Frame<'a>> = mcx::vec_with_capacity_in(mcx, 4)?;
        stack.push(Frame::from_container(container));
        Ok(JsonbIterator { stack })
    }

    /// C: `(*it)->container` — the container of the current nesting level.
    pub fn current_container(&self) -> &'a [u8] {
        self.stack.last().expect("iterator exhausted").container
    }

    pub fn done(&self) -> bool {
        self.stack.is_empty()
    }

    /// C: JsonbIteratorNext. The returned item is `Null` for tokens that carry
    /// no value (Done/EndArray/EndObject), matching C's val->type = jbvNull.
    pub fn next(&mut self, skip_nested: bool) -> (WjbToken, JsonbItem<'a>) {
        loop {
            let Some(it) = self.stack.last_mut() else {
                return (WjbToken::Done, JsonbItem::Null);
            };
            match it.step(skip_nested) {
                Step::Emit(tok, val) => return (tok, val),
                Step::Pop(tok) => {
                    self.stack.pop();
                    return (tok, JsonbItem::Null);
                }
                Step::Recurse(child) => self.stack.push(Frame::from_container(child)),
            }
        }
    }
}

/// Non-allocating core for the proofs/jsonb-probe cmp family: the same
/// JsonbIteratorNext state machine as `JsonbIterator`, but the frame stack is
/// a fixed-capacity inline array (`[Frame; N]`, N >= 1) instead of an
/// Mcx-backed vec. `next` reports nesting deeper than N as `None` so callers
/// can fall back to the allocating iterator — behavior stays identical for
/// every input at any depth.
pub struct FixedJsonbIterator<'a, const N: usize> {
    /// The current nesting level's frame. Kept as a plain struct field —
    /// NOT a dynamically indexed stack slot — mirroring C's `*it` current
    /// iterator: a dynamically indexed hot slot defeats CBMC's array field
    /// sensitivity and makes every post-mutation `next` call explode
    /// (proofs/jsonb-probe cmp family; same class as the brin-minmax
    /// per-slot-stores lesson).
    cur: Frame<'a>,
    /// Saved parent frames below the current level: when at depth d,
    /// `parents[0..d-1]` hold levels 1..d-1 (the root has no parent; the
    /// last slot is spare). Only touched on recursion into / return from a
    /// nested container, so flat documents never index it.
    parents: [Frame<'a>; N],
    depth: usize,
}

impl<'a, const N: usize> FixedJsonbIterator<'a, N> {
    pub fn init(container: &'a [u8]) -> FixedJsonbIterator<'a, N> {
        const { assert!(N >= 1, "FixedJsonbIterator needs room for the root frame") };
        let root = Frame::from_container(container);
        FixedJsonbIterator {
            cur: root,
            // Frame is Copy; unused slots hold copies of the root frame.
            parents: [root; N],
            depth: 1,
        }
    }

    /// C: JsonbIteratorNext, as in `JsonbIterator::next`; `None` means the
    /// input nests deeper than N frames (never a semantic result).
    ///
    /// The retry loop is written with its exact syntactic bound: one call
    /// loops only on `Recurse`, each of which either aborts (depth == N) or
    /// increments depth (< N), so from depth >= 1 there are at most N - 1
    /// recursions before a mandatory Emit — <= N iterations total. A plain
    /// `loop` here unwinds to the harness bound and its dead copies blow up
    /// the proofs/jsonb-probe cmp formulas exponentially per call (measured
    /// 10x/call); the constant-bound `for` lets symbolic execution stop by
    /// itself. Behavior is identical.
    pub fn next(&mut self, skip_nested: bool) -> Option<(WjbToken, JsonbItem<'a>)> {
        for _ in 0..N {
            if self.depth == 0 {
                return Some((WjbToken::Done, JsonbItem::Null));
            }
            match self.cur.step(skip_nested) {
                Step::Emit(tok, val) => return Some((tok, val)),
                Step::Pop(tok) => {
                    self.depth -= 1;
                    if self.depth > 0 {
                        self.cur = self.parents[self.depth - 1];
                    }
                    return Some((tok, JsonbItem::Null));
                }
                Step::Recurse(child) => {
                    if self.depth == N {
                        return None;
                    }
                    self.parents[self.depth - 1] = self.cur;
                    self.cur = Frame::from_container(child);
                    self.depth += 1;
                }
            }
        }
        // Unreachable: <= N - 1 recursions are possible before a step must
        // emit or abort (see the loop-bound note above).
        panic!("FixedJsonbIterator::next: more than N recursions")
    }
}
