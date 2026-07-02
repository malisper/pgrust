// s_lock.h SpinDelayStatus; `init_local_spin_delay` is `new` at the call site.
#[derive(Debug)]
pub struct SpinDelayStatus {
    pub spins: i32,
    pub delays: i32,
    pub cur_delay: i32,
    pub file: &'static str,
    pub line: i32,
    pub func: &'static str,
}

impl SpinDelayStatus {
    pub const fn new(file: &'static str, line: i32, func: &'static str) -> Self {
        Self {
            spins: 0,
            delays: 0,
            cur_delay: 0,
            file,
            line,
            func,
        }
    }
}

seam_core::seam!(
    pub fn perform_spin_delay<'a>(status: &'a mut SpinDelayStatus)
);

seam_core::seam!(
    pub fn finish_spin_delay<'a>(status: &'a SpinDelayStatus)
);

seam_core::seam!(
    pub fn set_spins_per_delay(shared_spins_per_delay: i32)
);

seam_core::seam!(
    pub fn update_spins_per_delay(shared_spins_per_delay: i32) -> i32
);
