use crate::Value;

#[derive(Debug, Clone)]
pub struct CopyToNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub position: Option<usize>,
}

#[derive(Debug, Clone)]
pub enum CopyToDmlEvent {
    Notice(CopyToNotice),
    Row(Vec<Value>),
}
