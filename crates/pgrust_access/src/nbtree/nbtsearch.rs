use std::cmp::Ordering;

pub fn lower_bound_by<T>(items: &[T], mut cmp: impl FnMut(&T) -> Ordering) -> usize {
    let mut left = 0usize;
    let mut right = items.len();
    while left < right {
        let mid = left + (right - left) / 2;
        if cmp(&items[mid]) == Ordering::Greater {
            right = mid;
        } else {
            left = mid + 1;
        }
    }
    left
}

pub fn first_greater_or_equal_by<T>(items: &[T], mut cmp: impl FnMut(&T) -> Ordering) -> usize {
    let mut left = 0usize;
    let mut right = items.len();
    while left < right {
        let mid = left + (right - left) / 2;
        match cmp(&items[mid]) {
            Ordering::Less => left = mid + 1,
            Ordering::Equal | Ordering::Greater => right = mid,
        }
    }
    left
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lower_bound_by_returns_insert_position_after_equals() {
        let items = [1, 2, 2, 4];
        let pos = lower_bound_by(&items, |item| item.cmp(&2));
        assert_eq!(pos, 3);
    }

    #[test]
    fn first_greater_or_equal_by_finds_first_match() {
        let items = [1, 2, 2, 4];
        let pos = first_greater_or_equal_by(&items, |item| item.cmp(&2));
        assert_eq!(pos, 1);
    }
}
