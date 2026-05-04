#[derive(Debug, Clone)]
pub struct CopyInState<C> {
    pub copy: C,
    pub sql: String,
    pub pending: Vec<u8>,
    pub continuation: Vec<String>,
}

impl<C> CopyInState<C> {
    pub fn new(copy: C, sql: String, continuation: Vec<String>) -> Self {
        Self {
            copy,
            sql,
            pending: Vec::new(),
            continuation,
        }
    }

    pub fn append_data(&mut self, body: &[u8]) {
        self.pending.extend_from_slice(body);
    }

    pub fn pending_text_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.pending)
    }
}
