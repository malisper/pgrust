#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendNoticeLevel {
    Notice,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendNotice {
    pub level: BackendNoticeLevel,
    pub message: String,
}

thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<BackendNotice>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn push_notice(message: impl Into<String>) {
    push_notice_with_level(BackendNoticeLevel::Notice, message);
}

pub fn push_warning(message: impl Into<String>) {
    push_notice_with_level(BackendNoticeLevel::Warning, message);
}

pub fn take_notice_entries() -> Vec<BackendNotice> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn take_notices() -> Vec<String> {
    take_notice_entries()
        .into_iter()
        .map(|notice| notice.message)
        .collect()
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}

fn push_notice_with_level(level: BackendNoticeLevel, message: impl Into<String>) {
    NOTICE_QUEUE.with(|queue| {
        queue.borrow_mut().push(BackendNotice {
            level,
            message: message.into(),
        })
    });
}
