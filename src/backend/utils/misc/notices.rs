#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendNoticeLevel {
    Notice,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendNotice {
    pub level: BackendNoticeLevel,
    pub message: String,
    pub detail: Option<String>,
}

thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<BackendNotice>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn push_notice(message: impl Into<String>) {
    push_notice_with_level_and_detail(BackendNoticeLevel::Notice, message, None::<String>);
}

pub fn push_notice_with_detail(message: impl Into<String>, detail: impl Into<String>) {
    push_notice_with_level_and_detail(
        BackendNoticeLevel::Notice,
        message,
        Some(detail.into()),
    );
}

pub fn push_warning(message: impl Into<String>) {
    push_notice_with_level_and_detail(BackendNoticeLevel::Warning, message, None::<String>);
}

pub fn take_notice_entries() -> Vec<BackendNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn take_notices() -> Vec<BackendNotice> {
    take_notice_entries()
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}

fn push_notice_with_level_and_detail(
    level: BackendNoticeLevel,
    message: impl Into<String>,
    detail: Option<String>,
) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(BackendNotice {
            level,
            message: message.into(),
            detail,
        })
    });
}
