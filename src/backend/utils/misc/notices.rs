thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<BackendNotice>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendNotice {
    pub message: String,
    pub detail: Option<String>,
}

pub fn push_notice(message: impl Into<String>) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(BackendNotice {
            message: message.into(),
            detail: None,
        })
    });
}

pub fn push_notice_with_detail(message: impl Into<String>, detail: impl Into<String>) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(BackendNotice {
            message: message.into(),
            detail: Some(detail.into()),
        })
    });
}

pub fn take_notices() -> Vec<BackendNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}
