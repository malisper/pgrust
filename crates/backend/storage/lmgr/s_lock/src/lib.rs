//! s_lock.c: contended-spinlock backoff. The TAS/S_UNLOCK primitives live in
//! types_storage::Spinlock; this unit owns the delay policy.

use std::cell::Cell;

use s_lock_seams::SpinDelayStatus;

#[cfg(test)]
mod tests;

const DEFAULT_SPINS_PER_DELAY: i32 = 100;
const MIN_SPINS_PER_DELAY: i32 = 10;
const MAX_SPINS_PER_DELAY: i32 = 1000;
const NUM_DELAYS: i32 = 1000;
const MIN_DELAY_USEC: i32 = 1000;
const MAX_DELAY_USEC: i32 = 1_000_000;

const WAIT_EVENT_SPIN_DELAY: u32 = 0x0900_0000 | 6;

thread_local! {
    static SPINS_PER_DELAY: Cell<i32> = const { Cell::new(DEFAULT_SPINS_PER_DELAY) };
}

pub fn perform_spin_delay(status: &mut SpinDelayStatus) {
    core::hint::spin_loop();

    status.spins += 1;
    if status.spins >= SPINS_PER_DELAY.get() {
        status.delays += 1;
        if status.delays > NUM_DELAYS {
            panic!(
                "stuck spinlock detected at {}, {}:{}",
                status.func, status.file, status.line
            );
        }

        if status.cur_delay == 0 {
            status.cur_delay = MIN_DELAY_USEC;
        }

        waitevent_seams::pgstat_report_wait_start::call(WAIT_EVENT_SPIN_DELAY);
        // Waiter-based backoff (M0 lane C): rides the DST clock hook instead
        // of a raw thread::sleep. A pending unpark aimed at this thread ends
        // the delay early — harmless for a backoff (the latch protocol
        // re-tests is_set before parking, so a consumed notify is never a
        // lost wake).
        waiter::sleep(std::time::Duration::from_micros(status.cur_delay as u64));
        waitevent_seams::pgstat_report_wait_end::call();

        let frac = pg_prng::global_prng(|p| p.next_f64());
        status.cur_delay += (status.cur_delay as f64 * frac + 0.5) as i32;
        if status.cur_delay > MAX_DELAY_USEC {
            status.cur_delay = MIN_DELAY_USEC;
        }

        status.spins = 0;
    }
}

pub fn finish_spin_delay(status: &SpinDelayStatus) {
    let spins_per_delay = SPINS_PER_DELAY.get();
    if status.cur_delay == 0 {
        if spins_per_delay < MAX_SPINS_PER_DELAY {
            SPINS_PER_DELAY.set((spins_per_delay + 100).min(MAX_SPINS_PER_DELAY));
        }
    } else if spins_per_delay > MIN_SPINS_PER_DELAY {
        SPINS_PER_DELAY.set((spins_per_delay - 1).max(MIN_SPINS_PER_DELAY));
    }
}

pub fn set_spins_per_delay(shared_spins_per_delay: i32) {
    SPINS_PER_DELAY.set(shared_spins_per_delay);
}

pub fn update_spins_per_delay(shared_spins_per_delay: i32) -> i32 {
    (shared_spins_per_delay * 15 + SPINS_PER_DELAY.get()) / 16
}

pub fn init_seams() {
    s_lock_seams::perform_spin_delay::set(perform_spin_delay);
    s_lock_seams::finish_spin_delay::set(finish_spin_delay);
    s_lock_seams::set_spins_per_delay::set(set_spins_per_delay);
    s_lock_seams::update_spins_per_delay::set(update_spins_per_delay);
}
