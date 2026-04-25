use std::cell::RefCell;

use crate::backend::executor::ExecError;
use crate::backend::parser::ParseError;

pub const DEFAULT_MAX_STACK_DEPTH_KB: u32 = 2048;
#[cfg(debug_assertions)]
pub const DEBUG_DEFAULT_MAX_STACK_DEPTH_KB: u32 = 64 * 1024;
pub const MIN_MAX_STACK_DEPTH_KB: u32 = 100;

const STACK_DEPTH_SLOP_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Copy)]
struct StackDepthState {
    depth: usize,
    baseline_remaining: Option<usize>,
    max_stack_depth_kb: u32,
}

impl Default for StackDepthState {
    fn default() -> Self {
        Self {
            depth: 0,
            baseline_remaining: None,
            max_stack_depth_kb: effective_default_max_stack_depth_kb(),
        }
    }
}

thread_local! {
    static STACK_DEPTH_STATE: RefCell<StackDepthState> = RefCell::new(StackDepthState::default());
}

pub struct StackDepthGuard {
    active: bool,
}

impl StackDepthGuard {
    pub fn enter(max_stack_depth_kb: u32) -> Self {
        STACK_DEPTH_STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.depth == 0 {
                state.baseline_remaining = stacker::remaining_stack();
                state.max_stack_depth_kb = max_stack_depth_kb;
            }
            state.depth += 1;
        });
        Self { active: true }
    }

    pub fn run<T>(self, f: impl FnOnce() -> T) -> T {
        f()
    }
}

impl Drop for StackDepthGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        STACK_DEPTH_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.depth = state.depth.saturating_sub(1);
            if state.depth == 0 {
                *state = StackDepthState::default();
            }
        });
    }
}

pub fn check_stack_depth(max_stack_depth_kb: u32) -> Result<(), ExecError> {
    if stack_is_too_deep(max_stack_depth_kb) {
        Err(stack_depth_limit_error(max_stack_depth_kb))
    } else {
        Ok(())
    }
}

pub fn check_parse_stack_depth() -> Result<(), ParseError> {
    let max_stack_depth_kb = active_max_stack_depth_kb();
    if stack_is_too_deep(max_stack_depth_kb) {
        Err(parse_stack_depth_limit_error(max_stack_depth_kb))
    } else {
        Ok(())
    }
}

pub fn stack_is_too_deep(max_stack_depth_kb: u32) -> bool {
    let Some(remaining) = stacker::remaining_stack() else {
        return false;
    };

    let max_depth_bytes = max_stack_depth_kb as usize * 1024;
    STACK_DEPTH_STATE.with(|state| {
        let state = state.borrow();
        if let Some(baseline) = state.baseline_remaining {
            baseline.saturating_sub(remaining) > max_depth_bytes
                || remaining < STACK_DEPTH_SLOP_BYTES
        } else {
            false
        }
    })
}

pub fn stack_depth_limit_error(max_stack_depth_kb: u32) -> ExecError {
    ExecError::DetailedError {
        message: "stack depth limit exceeded".into(),
        detail: None,
        hint: Some(stack_depth_limit_hint(max_stack_depth_kb)),
        sqlstate: "54001",
    }
}

fn parse_stack_depth_limit_error(max_stack_depth_kb: u32) -> ParseError {
    ParseError::DetailedError {
        message: "stack depth limit exceeded".into(),
        detail: None,
        hint: Some(stack_depth_limit_hint(max_stack_depth_kb)),
        sqlstate: "54001",
    }
}

fn stack_depth_limit_hint(max_stack_depth_kb: u32) -> String {
    format!(
        "Increase the configuration parameter \"max_stack_depth\" (currently {max_stack_depth_kb}kB), after ensuring the platform's stack depth limit is adequate."
    )
}

fn active_max_stack_depth_kb() -> u32 {
    STACK_DEPTH_STATE.with(|state| state.borrow().max_stack_depth_kb)
}

pub fn effective_default_max_stack_depth_kb() -> u32 {
    #[cfg(debug_assertions)]
    {
        return DEBUG_DEFAULT_MAX_STACK_DEPTH_KB;
    }

    #[cfg(not(debug_assertions))]
    effective_default_max_stack_depth_kb_from_rlimit(stack_depth_rlimit_bytes())
}

pub fn max_stack_depth_limit_kb() -> Option<u32> {
    max_stack_depth_limit_kb_from_rlimit(stack_depth_rlimit_bytes())
}

fn effective_default_max_stack_depth_kb_from_rlimit(stack_rlimit_bytes: Option<usize>) -> u32 {
    let Some(limit_kb) = max_stack_depth_limit_kb_from_rlimit(stack_rlimit_bytes) else {
        return DEFAULT_MAX_STACK_DEPTH_KB;
    };
    if limit_kb > MIN_MAX_STACK_DEPTH_KB {
        limit_kb.min(DEFAULT_MAX_STACK_DEPTH_KB)
    } else {
        MIN_MAX_STACK_DEPTH_KB
    }
}

fn max_stack_depth_limit_kb_from_rlimit(stack_rlimit_bytes: Option<usize>) -> Option<u32> {
    let stack_rlimit_bytes = stack_rlimit_bytes?;
    let safe_bytes = stack_rlimit_bytes.saturating_sub(STACK_DEPTH_SLOP_BYTES);
    let safe_kb = safe_bytes / 1024;
    u32::try_from(safe_kb).ok()
}

#[cfg(not(target_arch = "wasm32"))]
fn stack_depth_rlimit_bytes() -> Option<usize> {
    let mut rlim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_STACK, &mut rlim) };
    if rc != 0 {
        return None;
    }
    if rlim.rlim_cur == libc::RLIM_INFINITY {
        return Some(usize::MAX);
    }
    usize::try_from(rlim.rlim_cur).ok()
}

#[cfg(target_arch = "wasm32")]
fn stack_depth_rlimit_bytes() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_MAX_STACK_DEPTH_KB, MIN_MAX_STACK_DEPTH_KB,
        effective_default_max_stack_depth_kb_from_rlimit, max_stack_depth_limit_kb_from_rlimit,
    };

    #[test]
    fn effective_default_max_stack_depth_matches_postgres_cap() {
        assert_eq!(
            effective_default_max_stack_depth_kb_from_rlimit(Some(8 * 1024 * 1024)),
            DEFAULT_MAX_STACK_DEPTH_KB
        );
        assert_eq!(
            effective_default_max_stack_depth_kb_from_rlimit(Some(1024 * 1024)),
            512
        );
        assert_eq!(
            effective_default_max_stack_depth_kb_from_rlimit(Some(400 * 1024)),
            MIN_MAX_STACK_DEPTH_KB
        );
        assert_eq!(
            effective_default_max_stack_depth_kb_from_rlimit(None),
            DEFAULT_MAX_STACK_DEPTH_KB
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn debug_builds_default_max_stack_depth_to_64mb() {
        assert_eq!(super::effective_default_max_stack_depth_kb(), 64 * 1024);
    }

    #[test]
    fn max_stack_depth_limit_applies_postgres_safety_margin() {
        assert_eq!(
            max_stack_depth_limit_kb_from_rlimit(Some(8 * 1024 * 1024)),
            Some(7680)
        );
        assert_eq!(
            max_stack_depth_limit_kb_from_rlimit(Some(1024 * 1024)),
            Some(512)
        );
        assert_eq!(max_stack_depth_limit_kb_from_rlimit(None), None);
    }
}
