//! Generator (WS-GEN): grammar, weights, budgets, screens, lazy plan generator.

pub mod budget;
pub mod generator;
pub mod knobs;
pub mod noise;
pub mod prodreg;
pub mod profile;
pub mod schema;
pub mod screens;
pub mod weights;

pub use generator::{generate_plan, generate_plan_traced, Generator};
