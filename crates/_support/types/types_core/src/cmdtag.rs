/// C `CommandTag` as a value-checked newtype: the values are positional
/// indices in `tcop/cmdtaglist.h` (PG 18.3) and must stay index-exact.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct CommandTag(pub i32);

impl CommandTag {
    pub const UNKNOWN: CommandTag = CommandTag(0);
    pub const ALTER_SEQUENCE: CommandTag = CommandTag(29);
    pub const CREATE_SEQUENCE: CommandTag = CommandTag(84);
    pub const REFRESH_MATERIALIZED_VIEW: CommandTag = CommandTag(169);
    pub const SELECT: CommandTag = CommandTag(179);
}
