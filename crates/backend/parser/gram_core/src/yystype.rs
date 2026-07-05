#![allow(non_snake_case, non_upper_case_globals)]

use core::marker::PhantomData;
use core::ptr::NonNull;

use types_nodes::{Alias, LimitOption, Node, NodeList, OptNodeList};

// func_alias_clause's (alias, coldeflist) pair; arena carrier for the
// non-NIL-coldeflist forms.
pub struct FuncAliasCols<'mcx> {
    pub alias: Option<&'mcx Alias<'mcx>>,
    pub coldeflist: NodeList<'mcx>,
}

// gram.y's SelectLimit carrier (a tagless parsenodes.h struct; lives only
// between limit_clause and insertSelectOptions).
pub struct SelectLimit<'mcx> {
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub offsetLoc: i32,
    pub countLoc: i32,
    pub optionLoc: i32,
}

// gram.h's YYSTYPE union, packed to 16 bytes: C keeps its slot 8B and the
// 32B tagged-enum slot traffic stalled the V2 automaton walk (narrow stores
// wide-reloaded, graviton.md §4.5). `p` is the payload pointer (typed, so
// provenance survives); meta = tag (low 32) | aux (high 32: str/list length,
// ival, bool). Moves replace C's copies (stack slots are read exactly once).
// gram.y's KeyAction/KeyActions carriers (private gram structs, live only
// between key_action and the FK Constraint build).
pub struct KeyAction<'mcx> {
    pub action: u8,
    pub cols: NodeList<'mcx>,
}

impl Default for KeyAction<'_> {
    fn default() -> Self {
        KeyAction { action: 0, cols: NodeList::nil() }
    }
}

pub struct KeyActions<'mcx> {
    pub update_action: KeyAction<'mcx>,
    pub delete_action: KeyAction<'mcx>,
}

// gram.y json_behavior_clause_opt's list_make2(on_empty, on_error) carrier.
#[derive(Default)]
pub struct JsonBehaviors<'mcx> {
    pub on_empty: Option<Node<'mcx>>,
    pub on_error: Option<Node<'mcx>>,
}

// join_qual's list_make2(name_list, opt_alias_clause_for_join_using) pair,
// live only between join_qual and the JoinExpr build.
pub struct JoinQualUsing<'mcx> {
    pub cols: NodeList<'mcx>,
    pub alias: Option<&'mcx Alias<'mcx>>,
}

// transform_element_list's list_make2(fromsql, tosql); cells may be C-NULL,
// which NodeList cannot carry.
#[derive(Default)]
pub struct TransformElements<'mcx> {
    pub fromsql: Option<Node<'mcx>>,
    pub tosql: Option<Node<'mcx>>,
}

pub struct YYSTYPE<'mcx> {
    p: *mut u8,
    meta: u64,
    _arena: PhantomData<&'mcx ()>,
}

const _: () = assert!(core::mem::size_of::<YYSTYPE<'static>>() == 16);
const _: () = assert!(!core::mem::needs_drop::<YYSTYPE<'static>>());

const T_NONE: u32 = 0;
const T_IVAL: u32 = 1;
const T_STR: u32 = 2;
const T_KEYWORD: u32 = 3;
const T_BOOL: u32 = 4;
const T_NODE: u32 = 5;
const T_LIST: u32 = 6;
const T_ALIAS: u32 = 7;
const T_FUNC_ALIAS: u32 = 8;
const T_GROUP: u32 = 9;
const T_GROUP_DISTINCT: u32 = 10;
const T_LIMIT: u32 = 11;
const T_DISTINCT_ALL: u32 = 12;
const T_KEY_ACTION: u32 = 13;
const T_KEY_ACTIONS: u32 = 14;
const T_FUNC_ALIAS_COLS: u32 = 15;
const T_JOIN_USING: u32 = 16;
const T_JSON_BEHAVIORS: u32 = 17;
const T_TRANSFORM_ELEMS: u32 = 18;
const T_OPT_LIST: u32 = 19;

#[cold]
#[inline(never)]
fn confusion(want: &'static str) -> ! {
    panic!("gram_core: grammar value stack type confusion (wanted {want})")
}

impl<'mcx> YYSTYPE<'mcx> {
    pub const None: YYSTYPE<'mcx> = YYSTYPE {
        p: core::ptr::null_mut(),
        meta: T_NONE as u64,
        _arena: PhantomData,
    };

    pub const DistinctAll: YYSTYPE<'mcx> = YYSTYPE {
        p: core::ptr::null_mut(),
        meta: T_DISTINCT_ALL as u64,
        _arena: PhantomData,
    };

    #[inline(always)]
    fn mk(p: *mut u8, tag: u32, aux: u32) -> Self {
        YYSTYPE { p, meta: tag as u64 | ((aux as u64) << 32), _arena: PhantomData }
    }

    #[inline(always)]
    fn tag(&self) -> u32 {
        self.meta as u32
    }

    #[inline(always)]
    fn aux(&self) -> u32 {
        (self.meta >> 32) as u32
    }

    #[inline(always)]
    pub fn Ival(v: i32) -> Self {
        Self::mk(core::ptr::null_mut(), T_IVAL, v as u32)
    }

    #[inline(always)]
    pub fn Str(s: &'mcx str) -> Self {
        Self::mk(s.as_ptr() as *mut u8, T_STR, s.len() as u32)
    }

    #[inline(always)]
    pub fn Keyword(s: &'static str) -> Self {
        Self::mk(s.as_ptr() as *mut u8, T_KEYWORD, s.len() as u32)
    }

    #[inline(always)]
    pub fn Boolean(b: bool) -> Self {
        Self::mk(core::ptr::null_mut(), T_BOOL, b as u32)
    }

    #[inline(always)]
    pub fn Node(n: Option<Node<'mcx>>) -> Self {
        let p = match n {
            Some(n) => n.as_raw().as_ptr() as *mut u8,
            Option::None => core::ptr::null_mut(),
        };
        Self::mk(p, T_NODE, 0)
    }

    #[inline(always)]
    pub fn List(l: NodeList<'mcx>) -> Self {
        let (p, len) = l.into_raw_parts();
        Self::mk(p as *mut u8, T_LIST, len)
    }

    #[inline(always)]
    pub fn Alias(a: Option<&'mcx Alias<'mcx>>) -> Self {
        let p = match a {
            Some(a) => a as *const Alias<'mcx> as *mut u8,
            Option::None => core::ptr::null_mut(),
        };
        Self::mk(p, T_ALIAS, 0)
    }

    #[inline(always)]
    pub fn Limit(l: Option<&'mcx mut SelectLimit<'mcx>>) -> Self {
        let p = match l {
            Some(l) => l as *mut SelectLimit<'mcx> as *mut u8,
            Option::None => core::ptr::null_mut(),
        };
        Self::mk(p, T_LIMIT, 0)
    }

    // func_alias_clause's list_make2(alias, coldeflist): NIL coldeflist packs
    // the bare alias pointer; non-NIL rides a FuncAliasCols arena carrier.
    #[inline(always)]
    pub fn FuncAlias(alias: Option<&'mcx Alias<'mcx>>, coldeflist: NodeList<'mcx>) -> Self {
        assert!(coldeflist.is_nil(), "gram_core: non-NIL coldeflist needs an arena carrier");
        let p = match alias {
            Some(a) => a as *const Alias<'mcx> as *mut u8,
            Option::None => core::ptr::null_mut(),
        };
        Self::mk(p, T_FUNC_ALIAS, 0)
    }

    #[inline(always)]
    pub fn Group(distinct: bool, list: NodeList<'mcx>) -> Self {
        let (p, len) = list.into_raw_parts();
        Self::mk(p as *mut u8, if distinct { T_GROUP_DISTINCT } else { T_GROUP }, len)
    }

    #[inline(always)]
    pub fn KeyActionV(a: &'mcx mut KeyAction<'mcx>) -> Self {
        Self::mk(a as *mut KeyAction<'mcx> as *mut u8, T_KEY_ACTION, 0)
    }

    #[inline(always)]
    pub fn KeyActionsV(a: &'mcx mut KeyActions<'mcx>) -> Self {
        Self::mk(a as *mut KeyActions<'mcx> as *mut u8, T_KEY_ACTIONS, 0)
    }

    #[inline(always)]
    pub fn JoinUsing(j: &'mcx mut JoinQualUsing<'mcx>) -> Self {
        Self::mk(j as *mut JoinQualUsing<'mcx> as *mut u8, T_JOIN_USING, 0)
    }

    #[inline]
    pub fn is_join_using(&self) -> bool {
        self.tag() == T_JOIN_USING
    }

    pub fn join_using(self) -> (NodeList<'mcx>, Option<&'mcx Alias<'mcx>>) {
        if self.tag() != T_JOIN_USING {
            confusion("JoinQualUsing");
        }
        // SAFETY: built by JoinUsing() from &'mcx mut; moved, never duplicated.
        let j = unsafe { &mut *(self.p as *mut JoinQualUsing<'mcx>) };
        (core::mem::replace(&mut j.cols, NodeList::nil()), j.alias)
    }

    pub fn key_action(self) -> &'mcx mut KeyAction<'mcx> {
        if self.tag() != T_KEY_ACTION {
            confusion("KeyAction");
        }
        // SAFETY: built by KeyActionV from &'mcx mut; moved, never duplicated.
        unsafe { &mut *(self.p as *mut KeyAction<'mcx>) }
    }

    #[inline(always)]
    pub fn JsonBehaviorsV(a: &'mcx mut JsonBehaviors<'mcx>) -> Self {
        Self::mk(a as *mut JsonBehaviors<'mcx> as *mut u8, T_JSON_BEHAVIORS, 0)
    }

    pub fn json_behaviors(self) -> &'mcx mut JsonBehaviors<'mcx> {
        if self.tag() != T_JSON_BEHAVIORS {
            confusion("JsonBehaviors");
        }
        // SAFETY: built by JsonBehaviorsV from &'mcx mut; moved, never duplicated.
        unsafe { &mut *(self.p as *mut JsonBehaviors<'mcx>) }
    }

    #[inline(always)]
    pub fn TransformElementsV(a: &'mcx mut TransformElements<'mcx>) -> Self {
        Self::mk(a as *mut TransformElements<'mcx> as *mut u8, T_TRANSFORM_ELEMS, 0)
    }

    pub fn transform_elements(self) -> &'mcx mut TransformElements<'mcx> {
        if self.tag() != T_TRANSFORM_ELEMS {
            confusion("TransformElements");
        }
        // SAFETY: built by TransformElementsV from &'mcx mut; moved, never duplicated.
        unsafe { &mut *(self.p as *mut TransformElements<'mcx>) }
    }

    pub fn key_actions(self) -> &'mcx mut KeyActions<'mcx> {
        if self.tag() != T_KEY_ACTIONS {
            confusion("KeyActions");
        }
        // SAFETY: built by KeyActionsV from &'mcx mut; moved, never duplicated.
        unsafe { &mut *(self.p as *mut KeyActions<'mcx>) }
    }

    pub fn ival(self) -> i32 {
        if self.tag() != T_IVAL {
            confusion("Ival");
        }
        self.aux() as i32
    }

    pub fn str_val(self) -> &'mcx str {
        if self.tag() != T_STR && self.tag() != T_KEYWORD {
            confusion("Str");
        }
        // SAFETY: built by Str/Keyword from a &str with this ptr/len.
        unsafe {
            core::str::from_utf8_unchecked(core::slice::from_raw_parts(
                self.p,
                self.aux() as usize,
            ))
        }
    }

    pub fn boolean(self) -> bool {
        if self.tag() != T_BOOL {
            confusion("Boolean");
        }
        self.aux() != 0
    }

    pub fn node(self) -> Option<Node<'mcx>> {
        if self.tag() != T_NODE {
            confusion("Node");
        }
        // SAFETY: built by Node() from a live Node<'mcx>.
        NonNull::new(self.p).map(|p| unsafe { Node::from_raw(p.cast()) })
    }

    #[inline]
    pub fn is_null_node(&self) -> bool {
        self.tag() == T_NODE && self.p.is_null()
    }

    #[inline]
    pub fn is_distinct_all(&self) -> bool {
        self.tag() == T_DISTINCT_ALL
    }

    pub fn list(self) -> NodeList<'mcx> {
        if self.tag() != T_LIST {
            confusion("List");
        }
        // SAFETY: built by List() via into_raw_parts; arena-live, read once.
        unsafe { NodeList::from_raw_parts(self.p.cast(), self.aux()) }
    }

    #[inline(always)]
    pub fn OptList(l: OptNodeList<'mcx>) -> Self {
        let (p, len) = l.into_raw_parts();
        Self::mk(p as *mut u8, T_OPT_LIST, len)
    }

    pub fn opt_list(self) -> OptNodeList<'mcx> {
        if self.tag() != T_OPT_LIST {
            confusion("OptList");
        }
        // SAFETY: built by OptList() via into_raw_parts; arena-live, read once.
        unsafe { OptNodeList::from_raw_parts(self.p.cast(), self.aux()) }
    }

    pub fn alias(self) -> Option<&'mcx Alias<'mcx>> {
        if self.tag() != T_ALIAS {
            confusion("Alias");
        }
        // SAFETY: built by Alias() from &'mcx Alias.
        unsafe { (self.p as *const Alias<'mcx>).as_ref() }
    }

    pub fn func_alias(self) -> (Option<&'mcx Alias<'mcx>>, NodeList<'mcx>) {
        match self.tag() {
            // SAFETY: built by FuncAlias() from &'mcx Alias (coldeflist NIL-asserted).
            T_FUNC_ALIAS => (unsafe { (self.p as *const Alias<'mcx>).as_ref() }, NodeList::nil()),
            T_FUNC_ALIAS_COLS => {
                // SAFETY: built by FuncAliasV() from &'mcx FuncAliasCols; the
                // list is arena-owned with no drop glue and each carrier is
                // consumed exactly once (table_ref reduce).
                let c = unsafe { &*(self.p as *const FuncAliasCols<'mcx>) };
                (c.alias, unsafe { core::ptr::read(&c.coldeflist) })
            }
            _ => confusion("FuncAlias"),
        }
    }

    #[inline(always)]
    pub fn FuncAliasV(c: &'mcx FuncAliasCols<'mcx>) -> Self {
        Self::mk(c as *const FuncAliasCols<'mcx> as *mut u8, T_FUNC_ALIAS_COLS, 0)
    }

    pub fn group(self) -> (bool, NodeList<'mcx>) {
        let distinct = match self.tag() {
            T_GROUP => false,
            T_GROUP_DISTINCT => true,
            _ => confusion("Group"),
        };
        // SAFETY: built by group() via into_raw_parts (as list()).
        (distinct, unsafe { NodeList::from_raw_parts(self.p.cast(), self.aux()) })
    }

    pub fn limit(self) -> Option<&'mcx mut SelectLimit<'mcx>> {
        if self.tag() != T_LIMIT {
            confusion("Limit");
        }
        // SAFETY: built by Limit() from &'mcx mut SelectLimit; moved, never duplicated.
        unsafe { (self.p as *mut SelectLimit<'mcx>).as_mut() }
    }
}

impl Default for YYSTYPE<'_> {
    #[inline(always)]
    fn default() -> Self {
        YYSTYPE::None
    }
}
