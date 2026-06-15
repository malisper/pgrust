use core::ffi::{c_char, c_int};

use crate::NodeTag;

pub const T_TypeName: NodeTag = 68;
pub const T_A_Star: NodeTag = 77;
pub const T_DefElem: NodeTag = 93;
pub const T_Integer: NodeTag = 465;
pub const T_Float: NodeTag = 466;
pub const T_Boolean: NodeTag = 467;
pub const T_String: NodeTag = 468;
pub const T_BitString: NodeTag = 469;

// Command parse-node tags (nodes/nodetags.h, generated for PostgreSQL 18.3) —
// consumed by the data/maintenance/COPY command crates.
pub const T_CopyStmt: NodeTag = 157;
pub const T_ClusterStmt: NodeTag = 238;
pub const T_VacuumStmt: NodeTag = 239;
pub const T_VacuumRelation: NodeTag = 240;
pub const T_ExplainStmt: NodeTag = 241;

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Integer {
    pub type_: NodeTag,
    pub ival: c_int,
}

impl Integer {
    pub fn new(ival: c_int) -> Self {
        Self {
            type_: T_Integer,
            ival,
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn ival(&self) -> c_int {
        self.ival
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Float {
    pub type_: NodeTag,
    pub fval: *mut c_char,
}

impl Float {
    pub fn new(fval: *mut c_char) -> Self {
        Self {
            type_: T_Float,
            fval,
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn fval(&self) -> *mut c_char {
        self.fval
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Boolean {
    pub type_: NodeTag,
    pub boolval: bool,
}

impl Boolean {
    pub fn new(boolval: bool) -> Self {
        Self {
            type_: T_Boolean,
            boolval,
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn boolval(&self) -> bool {
        self.boolval
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StringNode {
    pub type_: NodeTag,
    pub sval: *mut c_char,
}

impl StringNode {
    pub fn new(sval: *mut c_char) -> Self {
        Self {
            type_: T_String,
            sval,
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn sval(&self) -> *mut c_char {
        self.sval
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BitString {
    pub type_: NodeTag,
    pub bsval: *mut c_char,
}

impl BitString {
    pub fn new(bsval: *mut c_char) -> Self {
        Self {
            type_: T_BitString,
            bsval,
        }
    }

    pub fn node_tag(&self) -> NodeTag {
        self.type_
    }

    pub fn bsval(&self) -> *mut c_char {
        self.bsval
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{align_of, offset_of, size_of};

    #[test]
    fn value_node_layouts_match_postgres_abi() {
        assert_eq!(size_of::<Integer>(), 8);
        assert_eq!(align_of::<Integer>(), 4);
        assert_eq!(offset_of!(Integer, type_), 0);
        assert_eq!(offset_of!(Integer, ival), 4);

        assert_eq!(size_of::<Float>(), 16);
        assert_eq!(align_of::<Float>(), 8);
        assert_eq!(offset_of!(Float, type_), 0);
        assert_eq!(offset_of!(Float, fval), 8);

        assert_eq!(size_of::<Boolean>(), 8);
        assert_eq!(align_of::<Boolean>(), 4);
        assert_eq!(offset_of!(Boolean, type_), 0);
        assert_eq!(offset_of!(Boolean, boolval), 4);

        assert_eq!(size_of::<StringNode>(), 16);
        assert_eq!(align_of::<StringNode>(), 8);
        assert_eq!(offset_of!(StringNode, type_), 0);
        assert_eq!(offset_of!(StringNode, sval), 8);

        assert_eq!(size_of::<BitString>(), 16);
        assert_eq!(align_of::<BitString>(), 8);
        assert_eq!(offset_of!(BitString, type_), 0);
        assert_eq!(offset_of!(BitString, bsval), 8);
    }
}
