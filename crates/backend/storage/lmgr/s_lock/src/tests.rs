use s_lock_seams::SpinDelayStatus;

#[test]
fn spins_per_delay_adapts_like_c() {
    super::set_spins_per_delay(100);
    let no_delay = SpinDelayStatus::new("f", 1, "t");
    super::finish_spin_delay(&no_delay);
    assert_eq!(super::update_spins_per_delay(100), (100 * 15 + 200) / 16);

    let delayed = SpinDelayStatus { cur_delay: 1000, ..SpinDelayStatus::new("f", 1, "t") };
    super::set_spins_per_delay(100);
    super::finish_spin_delay(&delayed);
    assert_eq!(super::update_spins_per_delay(100), (100 * 15 + 99) / 16);
}

#[test]
fn perform_spin_delay_counts_spins() {
    super::set_spins_per_delay(1000);
    let mut st = SpinDelayStatus::new("f", 1, "t");
    super::perform_spin_delay(&mut st);
    assert_eq!(st.spins, 1);
    assert_eq!(st.delays, 0);
}
