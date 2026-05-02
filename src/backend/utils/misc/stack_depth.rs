use crate::backend::executor::ExecError;
use crate::backend::parser::ParseError;

pub use pgrust_core::stack_depth::{
    DEFAULT_MAX_STACK_DEPTH_KB, MIN_MAX_STACK_DEPTH_KB, StackDepthGuard, active_max_stack_depth_kb,
    effective_default_max_stack_depth_kb, max_stack_depth_limit_kb, stack_depth_limit_hint,
    stack_is_too_deep,
};

#[cfg(debug_assertions)]
pub use pgrust_core::stack_depth::DEBUG_DEFAULT_MAX_STACK_DEPTH_KB;

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
