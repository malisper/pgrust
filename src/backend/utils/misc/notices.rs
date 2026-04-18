thread_local! {
    static NOTICE_QUEUE: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

pub fn push_notice(message: impl Into<String>) {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().push(message.into()));
}

pub fn take_notices() -> Vec<String> {
    NOTICE_QUEUE.with(|queue| std::mem::take(&mut *queue.borrow_mut()))
}

pub fn clear_notices() {
    NOTICE_QUEUE.with(|queue| queue.borrow_mut().clear());
}
