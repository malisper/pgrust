// src/pl/plpgsql (phase 1): pl_scanner.c, pl_gram.y, pl_comp.c, pl_funcs.c
// (namespace stack), pl_exec.c, pl_handler.c — each trimmed to the phase-1
// statement set with named louds for the rest (see module headers).
#![allow(non_snake_case)]

pub mod ast;
pub mod comp;
mod errcodes;
pub mod exec;
pub mod gram;
pub mod handler;
pub mod scanner;

pub fn init_seams() {
    handler::init_seams();
}
