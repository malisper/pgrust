thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<BackendNotice>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendNotice {
    pub severity: &'static str,
    pub sqlstate: &'static str,
    pub message: String,
    pub detail: Option<String>,
    pub position: Option<usize>,
}

pub fn push_notice(message: impl Into<String>) {
    push_backend_notice("NOTICE", "00000", message, None, None);
}

pub fn push_warning(message: impl Into<String>) {
    push_backend_notice("WARNING", "01000", message, None, None);
}

pub fn push_backend_notice(
    severity: &'static str,
    sqlstate: &'static str,
    message: impl Into<String>,
    detail: Option<String>,
    position: Option<usize>,
) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(BackendNotice {
            severity,
            sqlstate,
            message: message.into(),
            detail,
            position,
        });
    });
}

pub fn take_notices() -> Vec<BackendNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}
