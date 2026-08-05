//! H8 cursor position model — the oracle side of the C1/C2 cursor
//! properties. The harness generated the cursor's underlying query (ORDER BY
//! the unique key over known rows), so the ENTIRE result sequence is known
//! at generation time and every FETCH/MOVE outcome is pure position
//! arithmetic (PostgreSQL portal semantics, DECLARE/FETCH docs):
//!
//!   position ∈ 0..=n+1   0 = before first row, i = ON row i (1-based),
//!                        n+1 = after last row.
//!
//! The model is deliberately the DOCUMENTED semantics, written without
//! looking at either engine's code — in diff-c campaigns the C leg
//! cross-checks the model, so a modeling error surfaces as a loud
//! clean-base violation on BOTH detectors, never as silent slack.

use crate::oracle::check::Row;

/// One cursor movement. `Forward`/`Backward` counts are kept small by the
/// generator (the property tables are small); `All` variants sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorOp {
    Forward(u32),
    Backward(u32),
    Absolute(i64),
    Relative(i64),
    All,
    BackwardAll,
    First,
    Last,
}

impl CursorOp {
    /// The direction clause as it appears in FETCH/MOVE statements.
    pub fn sql(&self) -> String {
        match self {
            CursorOp::Forward(n) => format!("FORWARD {n}"),
            CursorOp::Backward(n) => format!("BACKWARD {n}"),
            CursorOp::Absolute(k) => format!("ABSOLUTE {k}"),
            CursorOp::Relative(r) => format!("RELATIVE {r}"),
            CursorOp::All => "FORWARD ALL".into(),
            CursorOp::BackwardAll => "BACKWARD ALL".into(),
            CursorOp::First => "FIRST".into(),
            CursorOp::Last => "LAST".into(),
        }
    }

    /// True when the op can move the cursor backward from any reachable
    /// position — such ops require a SCROLL cursor.
    pub fn needs_scroll(&self) -> bool {
        !matches!(self, CursorOp::Forward(_) | CursorOp::All)
    }
}

/// Cursor position model over a known, ordered result sequence.
#[derive(Debug, Clone)]
pub struct CursorModel {
    rows: Vec<Row>,
    /// 0..=n+1 (see module doc).
    pos: usize,
}

impl CursorModel {
    pub fn new(rows: Vec<Row>) -> Self {
        CursorModel { rows, pos: 0 }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    fn row(&self, i: usize) -> Row {
        self.rows[i - 1].clone() // 1-based
    }

    /// Apply one op; returns the rows a FETCH would emit, in emission order.
    /// (A MOVE emits no rows but repositions identically; its command tag
    /// count is `returned.len()`.)
    pub fn apply(&mut self, op: CursorOp) -> Vec<Row> {
        let n = self.rows.len();
        match op {
            CursorOp::Forward(c) => {
                let c = c as usize;
                let start = self.pos + 1;
                let end = (self.pos + c).min(n);
                let out: Vec<Row> = (start..=end).map(|i| self.row(i)).collect();
                self.pos = (self.pos + c).min(n + 1);
                // Landing past the last returned row means after-last.
                if self.pos > n {
                    self.pos = n + 1;
                }
                out
            }
            CursorOp::All => {
                let start = self.pos + 1;
                let out: Vec<Row> = (start..=n).map(|i| self.row(i)).collect();
                self.pos = n + 1;
                out
            }
            CursorOp::Backward(c) => {
                let c = c as usize;
                let mut out = Vec::new();
                if self.pos >= 2 {
                    let stop = self.pos.saturating_sub(c).max(1);
                    let mut i = self.pos - 1;
                    while i >= stop {
                        out.push(self.row(i));
                        if i == stop {
                            break;
                        }
                        i -= 1;
                    }
                }
                self.pos = self.pos.saturating_sub(c);
                out
            }
            CursorOp::BackwardAll => {
                let mut out = Vec::new();
                if self.pos >= 2 {
                    for i in (1..self.pos).rev() {
                        out.push(self.row(i));
                    }
                }
                self.pos = 0;
                out
            }
            CursorOp::Absolute(k) => self.absolute(k),
            CursorOp::First => self.absolute(1),
            CursorOp::Last => self.absolute(-1),
            CursorOp::Relative(r) => {
                if r == 0 {
                    return if (1..=n).contains(&self.pos) {
                        vec![self.row(self.pos)]
                    } else {
                        Vec::new()
                    };
                }
                let t = self.pos as i64 + r;
                if t >= 1 && t <= n as i64 {
                    self.pos = t as usize;
                    vec![self.row(self.pos)]
                } else if t < 1 {
                    self.pos = 0;
                    Vec::new()
                } else {
                    self.pos = n + 1;
                    Vec::new()
                }
            }
        }
    }

    fn absolute(&mut self, k: i64) -> Vec<Row> {
        let n = self.rows.len() as i64;
        if k == 0 {
            self.pos = 0;
            return Vec::new();
        }
        let target = if k > 0 { k } else { n + 1 + k };
        if target >= 1 && target <= n {
            self.pos = target as usize;
            vec![self.row(self.pos)]
        } else if target < 1 {
            self.pos = 0;
            Vec::new()
        } else {
            self.pos = self.rows.len() + 1;
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oracle::check::Value;

    fn rows(n: i64) -> Vec<Row> {
        (1..=n).map(|k| Row(vec![Value::Int(k)])).collect()
    }

    fn ints(rs: &[Row]) -> Vec<i64> {
        rs.iter()
            .map(|r| match r.0[0] {
                Value::Int(i) => i,
                _ => panic!("int rows"),
            })
            .collect()
    }

    #[test]
    fn documented_semantics_walk() {
        let mut m = CursorModel::new(rows(5));
        assert_eq!(ints(&m.apply(CursorOp::Forward(3))), [1, 2, 3]); // on row 3
        assert_eq!(ints(&m.apply(CursorOp::Backward(2))), [2, 1]); // on row 1
        assert_eq!(ints(&m.apply(CursorOp::Backward(1))), [] as [i64; 0]); // before first
        assert_eq!(ints(&m.apply(CursorOp::All)), [1, 2, 3, 4, 5]); // after last
        assert_eq!(ints(&m.apply(CursorOp::Backward(2))), [5, 4]); // on row 4
        assert_eq!(ints(&m.apply(CursorOp::Absolute(2))), [2]);
        assert_eq!(ints(&m.apply(CursorOp::Relative(0))), [2]);
        assert_eq!(ints(&m.apply(CursorOp::Relative(2))), [4]);
        assert_eq!(ints(&m.apply(CursorOp::Relative(-3))), [1]);
        assert_eq!(ints(&m.apply(CursorOp::Last)), [5]);
        assert_eq!(ints(&m.apply(CursorOp::First)), [1]);
        assert_eq!(ints(&m.apply(CursorOp::Absolute(-2))), [4]);
        assert_eq!(ints(&m.apply(CursorOp::Absolute(9))), [] as [i64; 0]); // after last
        assert_eq!(ints(&m.apply(CursorOp::BackwardAll))[..2], [5, 4]);
        // Forward past end from on-row-N: empty, lands after-last.
        let mut m2 = CursorModel::new(rows(3));
        assert_eq!(ints(&m2.apply(CursorOp::Forward(3))), [1, 2, 3]); // on row 3
        assert_eq!(ints(&m2.apply(CursorOp::Forward(1))), [] as [i64; 0]);
        assert_eq!(ints(&m2.apply(CursorOp::Backward(1))), [3]);
    }
}
