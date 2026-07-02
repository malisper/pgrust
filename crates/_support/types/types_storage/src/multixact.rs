// DoesMultiXactIdConflict's bool return + *current_is_member out param.
#[derive(Clone, Copy, Debug)]
pub struct MultiXactConflict {
    pub conflict: bool,
    pub current_is_member: bool,
}
