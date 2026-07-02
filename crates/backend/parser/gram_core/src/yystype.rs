use types_nodes::{Alias, Node, NodeList};

// gram.h's YYSTYPE union; variants cover the ported reduction paths and grow
// with them. Moves replace C's copies (stack slots are read exactly once).
#[derive(Default)]
pub enum YYSTYPE<'mcx> {
    #[default]
    None,
    Ival(i32),
    Str(&'mcx str),
    Keyword(&'static str),
    Node(Option<Node<'mcx>>),
    List(NodeList<'mcx>),
    Alias(Option<&'mcx Alias<'mcx>>),
    Group {
        distinct: bool,
        list: NodeList<'mcx>,
    },
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
}
