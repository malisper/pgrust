//! Minimal repro: reads through Err(Box<E>) of a Result<f64, Box<E>>-returning
//! function are corrupted (garbage fields + spurious drop/dealloc failures).
//! The identical shape with Result<f32, Box<E>> verifies cleanly.

pub struct MyError {
    pub code: u32,
    pub level: i32,
    pub message: String,
}

fn err(code: u32) -> Box<MyError> {
    Box::new(MyError { code, level: 21, message: String::new() })
}

pub fn div_f64(a: f64, b: f64) -> Result<f64, Box<MyError>> {
    if b == 0.0 { return Err(err(22012)); }
    Ok(a / b)
}

pub fn div_f32(a: f32, b: f32) -> Result<f32, Box<MyError>> {
    if b == 0.0 { return Err(err(22012)); }
    Ok(a / b)
}

#[cfg(kani)]
mod proofs {
    use super::*;

    /// EXPECTED (correct semantics): pass. OBSERVED on kani 0.67.0: fails.
    #[kani::proof]
    fn f64_err_box_witness() {
        match div_f64(2.0, -0.0) {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.code == 22012 && e.level == 21),
        }
    }

    /// Identical shape at f32: passes.
    #[kani::proof]
    fn f32_err_box_control() {
        match div_f32(2.0, -0.0) {
            Ok(_) => assert!(false),
            Err(e) => assert!(e.code == 22012 && e.level == 21),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn native_semantics_correct() {
        let e = div_f64(2.0, -0.0).unwrap_err();
        assert!(e.code == 22012 && e.level == 21);
    }
}
