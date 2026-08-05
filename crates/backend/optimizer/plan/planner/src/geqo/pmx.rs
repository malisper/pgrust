//! `geqo_pmx.c` — partially matched crossover [PMX] (Goldberg & Lingle).
//! C: `#if defined(PMX)`.
#![allow(dead_code)]
#![allow(unused_assignments)]

use super::random::geqo_randint;
use super::{Gene, GeqoState};

const DAD: i32 = 1; // gene from dad
const MOM: i32 = 0; // gene from mom

/// `pmx(tour1, tour2, offspring, num_gene)` — partially matched crossover.
/// `unused_assignments`: STEP 2's `mx_fail--` result is never read, as in C.
pub(super) fn pmx(
    state: &mut GeqoState,
    tour1: &[Gene],
    tour2: &[Gene],
    offspring: &mut [Gene],
    num_gene: i32,
) {
    let ng = num_gene as usize;
    let mut failed = vec![-1i32; ng + 1];
    let mut from = vec![-1i32; ng + 1];
    let mut indx = vec![0i32; ng + 1];
    let mut check_list = vec![0i32; ng + 1];

    for k in 0..ng {
        failed[k] = -1;
        from[k] = -1;
        check_list[k + 1] = 0;
    }

    let mut left = geqo_randint(state, num_gene - 1, 0);
    let mut right = geqo_randint(state, num_gene - 1, 0);
    if left > right {
        core::mem::swap(&mut left, &mut right);
    }

    for k in 0..ng {
        offspring[k] = tour2[k];
        from[k] = DAD;
        check_list[tour2[k] as usize] += 1;
    }
    for k in left as usize..=right as usize {
        check_list[offspring[k] as usize] -= 1;
        offspring[k] = tour1[k];
        from[k] = MOM;
        check_list[tour1[k] as usize] += 1;
    }

    let mut mx_fail = 0usize;

    // STEP 1
    for k in left as usize..=right as usize {
        let found;
        if tour1[k] == tour2[k] {
            found = 1;
        } else {
            let mut f = 0;
            let mut j = 0usize;
            while f == 0 && j < ng {
                if offspring[j] == tour1[k] && from[j] == DAD {
                    check_list[offspring[j] as usize] -= 1;
                    offspring[j] = tour2[k];
                    f = 1;
                    check_list[tour2[k] as usize] += 1;
                }
                j += 1;
            }
            found = f;
        }
        if found == 0 {
            failed[mx_fail] = tour1[k];
            indx[mx_fail] = k as i32;
            mx_fail += 1;
        }
    }

    // STEP 2
    if mx_fail > 0 {
        let mx_hold = mx_fail;
        for k in 0..mx_hold {
            let mut found = 0;
            let mut j = 0usize;
            while found == 0 && j < ng {
                if failed[k] == offspring[j] && from[j] == DAD {
                    check_list[offspring[j] as usize] -= 1;
                    offspring[j] = tour2[indx[k] as usize];
                    check_list[tour2[indx[k] as usize] as usize] += 1;
                    found = 1;
                    failed[k] = -1;
                    mx_fail -= 1;
                }
                j += 1;
            }
        }
    }

    // STEP 3
    for k in 1..=ng {
        if check_list[k] > 1 {
            let mut i = 0usize;
            while i < ng {
                if offspring[i] == k as Gene && from[i] == DAD {
                    let mut j = 1usize;
                    while j <= ng {
                        if check_list[j] == 0 {
                            offspring[i] = j as Gene;
                            check_list[k] -= 1;
                            check_list[j] += 1;
                            i = ng + 1;
                            j = i;
                        }
                        j += 1;
                    }
                }
                i += 1;
            }
        }
    }
}
