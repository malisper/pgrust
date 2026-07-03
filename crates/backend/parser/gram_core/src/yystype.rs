#![allow(non_snake_case)]

use types_nodes::{Alias, LimitOption, Node, NodeList};

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

// gram.h's YYSTYPE union; variants cover the ported reduction paths and grow
// with them. Moves replace C's copies (stack slots are read exactly once).
#[derive(Default)]
pub enum YYSTYPE<'mcx> {
    #[default]
    None,
    Ival(i32),
    Str(&'mcx str),
    Keyword(&'static str),
    Boolean(bool),
    Node(Option<Node<'mcx>>),
    List(NodeList<'mcx>),
    Alias(Option<&'mcx Alias<'mcx>>),
    Group {
        distinct: bool,
        list: NodeList<'mcx>,
    },
    Limit(Option<&'mcx mut SelectLimit<'mcx>>),
    // distinct_clause DISTINCT (C: list_make1(NIL)); DISTINCT ON stays List.
    DistinctAll,
}

const _: () = assert!(!core::mem::needs_drop::<YYSTYPE<'static>>());

#[cold]
#[inline(never)]
fn confusion(want: &'static str) -> ! {
    panic!("gram_core: grammar value stack type confusion (wanted {want})")
}

impl<'mcx> YYSTYPE<'mcx> {
    pub fn ival(self) -> i32 {
        match self {
            YYSTYPE::Ival(v) => v,
            _ => confusion("Ival"),
        }
    }

    pub fn str_val(self) -> &'mcx str {
        match self {
            YYSTYPE::Str(s) => s,
            YYSTYPE::Keyword(s) => s,
            _ => confusion("Str"),
        }
    }

    pub fn boolean(self) -> bool {
        match self {
            YYSTYPE::Boolean(b) => b,
            _ => confusion("Boolean"),
        }
    }

    pub fn node(self) -> Option<Node<'mcx>> {
        match self {
            YYSTYPE::Node(n) => n,
            _ => confusion("Node"),
        }
    }

    pub fn list(self) -> NodeList<'mcx> {
        match self {
            YYSTYPE::List(l) => l,
            _ => confusion("List"),
        }
    }

    pub fn alias(self) -> Option<&'mcx Alias<'mcx>> {
        match self {
            YYSTYPE::Alias(a) => a,
            _ => confusion("Alias"),
        }
    }

    pub fn group(self) -> (bool, NodeList<'mcx>) {
        match self {
            YYSTYPE::Group { distinct, list } => (distinct, list),
            _ => confusion("Group"),
        }
    }

    pub fn limit(self) -> Option<&'mcx mut SelectLimit<'mcx>> {
        match self {
            YYSTYPE::Limit(l) => l,
            _ => confusion("Limit"),
        }
    }
}
