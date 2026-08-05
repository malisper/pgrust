// Values verified against PG 18.3 nodes/nodes.h JoinType.
#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JoinType {
    #[default]
    JOIN_INNER = 0,
    JOIN_LEFT = 1,
    JOIN_FULL = 2,
    JOIN_RIGHT = 3,
    JOIN_SEMI = 4,
    JOIN_ANTI = 5,
    JOIN_RIGHT_SEMI = 6,
    JOIN_RIGHT_ANTI = 7,
    JOIN_UNIQUE_OUTER = 8,
    JOIN_UNIQUE_INNER = 9,
}

impl JoinType {
    /// nodes.h IS_OUTER_JOIN(jointype).
    #[inline]
    pub const fn is_outer_join(self) -> bool {
        matches!(
            self,
            JoinType::JOIN_LEFT
                | JoinType::JOIN_FULL
                | JoinType::JOIN_RIGHT
                | JoinType::JOIN_ANTI
                | JoinType::JOIN_RIGHT_ANTI
        )
    }
}
