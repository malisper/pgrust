use std::cell::RefCell;

use crate::backend::executor::ExecError;
use crate::backend::parser::ParseError;

pub const DEFAULT_MAX_STACK_DEPTH_KB: u32 = 2048;

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
            max_stack_depth_kb: DEFAULT_MAX_STACK_DEPTH_KB,
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
