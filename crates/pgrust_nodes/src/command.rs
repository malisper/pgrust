#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandType {
    Select,
    Insert,
    Update,
    Delete,
    Utility,
}
