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
            match it.state {
                IterState::ArrayStart => {
                    let item = JsonbItem::Array {
                        n_elems: it.n_elems,
                        raw_scalar: it.is_scalar,
                    };
                    it.cur_index = 0;
                    it.cur_data_offset = 0;
                    it.cur_value_offset = 0;
                    it.state = IterState::ArrayElem;
                    return (WjbToken::BeginArray, item);
                }
                IterState::ArrayElem => {
                    if it.cur_index >= it.n_elems {
                        self.stack.pop();
                        return (WjbToken::EndArray, JsonbItem::Null);
                    }
                    let val =
                        fill_item(it.container, it.cur_index, it.base_off, it.cur_data_offset);
                    jbe_advance_offset(
                        &mut it.cur_data_offset,
                        child_jentry(it.container, it.cur_index),
                    );
                    it.cur_index += 1;
                    if let JsonbItem::Binary(child) = val {
                        if !skip_nested {
                            self.stack.push(Frame::from_container(child));
                            continue;
                        }
                    }
                    return (WjbToken::Elem, val);
                }
                IterState::ObjectStart => {
                    let item = JsonbItem::Object {
                        n_pairs: it.n_elems,
                    };
                    it.cur_index = 0;
                    it.cur_data_offset = 0;
                    it.cur_value_offset = get_jsonb_offset(it.container, it.n_elems);
                    it.state = IterState::ObjectKey;
                    return (WjbToken::BeginObject, item);
                }
                IterState::ObjectKey => {
                    if it.cur_index >= it.n_elems {
                        self.stack.pop();
                        return (WjbToken::EndObject, JsonbItem::Null);
                    }
                    let val =
                        fill_item(it.container, it.cur_index, it.base_off, it.cur_data_offset);
                    if !matches!(val, JsonbItem::String(_)) {
                        panic!("unexpected jsonb type as object key");
                    }
                    it.state = IterState::ObjectValue;
                    return (WjbToken::Key, val);
                }
                IterState::ObjectValue => {
                    it.state = IterState::ObjectKey;
                    let val = fill_item(
                        it.container,
                        it.cur_index + it.n_elems,
                        it.base_off,
                        it.cur_value_offset,
                    );
                    jbe_advance_offset(
                        &mut it.cur_data_offset,
                        child_jentry(it.container, it.cur_index),
                    );
                    jbe_advance_offset(
                        &mut it.cur_value_offset,
                        child_jentry(it.container, it.cur_index + it.n_elems),
                    );
                    it.cur_index += 1;
                    if let JsonbItem::Binary(child) = val {
                        if !skip_nested {
                            self.stack.push(Frame::from_container(child));
                            continue;
                        }
                    }
                    return (WjbToken::Value, val);
                }
            }
        }
    }
}
