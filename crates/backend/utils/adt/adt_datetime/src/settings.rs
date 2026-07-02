use core::cell::Cell;

use crate::consts::{DATEORDER_MDY, INTSTYLE_POSTGRES, USE_ISO_DATES};

thread_local! {
    static DATE_STYLE: Cell<i32> = const { Cell::new(USE_ISO_DATES) };
    static DATE_ORDER: Cell<i32> = const { Cell::new(DATEORDER_MDY) };
    static INTERVAL_STYLE: Cell<i32> = const { Cell::new(INTSTYLE_POSTGRES) };
}

#[inline]
pub fn date_style() -> i32 {
    DATE_STYLE.with(Cell::get)
}

#[inline]
pub fn set_date_style(style: i32) {
    DATE_STYLE.with(|c| c.set(style));
}

#[inline]
pub fn date_order() -> i32 {
    DATE_ORDER.with(Cell::get)
}

#[inline]
pub fn set_date_order(order: i32) {
    DATE_ORDER.with(|c| c.set(order));
}

#[inline]
pub fn interval_style() -> i32 {
    INTERVAL_STYLE.with(Cell::get)
}

#[inline]
pub fn set_interval_style(style: i32) {
    INTERVAL_STYLE.with(|c| c.set(style));
}
