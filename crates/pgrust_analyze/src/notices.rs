use std::cell::RefCell;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyzerNotice {
    pub message: String,
    pub detail: Option<String>,
}

thread_local! {
    static NOTICES: RefCell<Vec<AnalyzerNotice>> = const { RefCell::new(Vec::new()) };
}

pub fn push_notice(message: impl Into<String>) {
    NOTICES.with(|notices| {
        notices.borrow_mut().push(AnalyzerNotice {
            message: message.into(),
            detail: None,
        });
    });
}

pub fn push_notice_with_detail(message: impl Into<String>, detail: impl Into<String>) {
    NOTICES.with(|notices| {
        notices.borrow_mut().push(AnalyzerNotice {
            message: message.into(),
            detail: Some(detail.into()),
        });
    });
}

pub fn take_notices() -> Vec<AnalyzerNotice> {
    NOTICES.with(|notices| notices.borrow_mut().drain(..).collect())
}

pub fn clear_notices() {
    NOTICES.with(|notices| notices.borrow_mut().clear());
}
