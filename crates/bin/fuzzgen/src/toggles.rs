//! Feature-module registry and toggle vector. Modules are the swarm-testing
//! axis: each has an on/off toggle and a selection weight, sampled per
//! session (seeded) or pinned via `--modules`.

use crate::rng::Rng;

#[derive(Clone, Copy, Debug)]
pub struct ModuleSpec {
    pub name: &'static str,
    pub default_weight: f64,
}

/// All known feature modules. F0 ships only scalar expressions; later
/// milestones append here (joins, aggregates, subqueries, DML, DDL, ...).
pub const REGISTRY: &[ModuleSpec] = &[ModuleSpec { name: "expr", default_weight: 1.0 }];

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Toggle {
    pub on: bool,
    pub weight: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ToggleVector {
    /// Parallel to `REGISTRY`.
    entries: Vec<Toggle>,
}

impl ToggleVector {
    pub fn all_on() -> ToggleVector {
        ToggleVector {
            entries: REGISTRY
                .iter()
                .map(|m| Toggle { on: true, weight: m.default_weight })
                .collect(),
        }
    }

    /// Swarm-random sampling: each module independently on with probability
    /// 1/2, re-rolled until at least one module is on. Draws only from the
    /// session PRNG.
    pub fn swarm(rng: &mut Rng) -> ToggleVector {
        loop {
            let entries: Vec<Toggle> = REGISTRY
                .iter()
                .map(|m| Toggle { on: rng.chance(1, 2), weight: m.default_weight })
                .collect();
            if entries.iter().any(|t| t.on) {
                return ToggleVector { entries };
            }
        }
    }

    /// Parse a `--modules` spec like `expr=on,joins=off` or
    /// `expr=on:2.5` (weight after the colon). Unknown modules are errors.
    pub fn parse(spec: &str) -> Result<ToggleVector, String> {
        let mut tv = ToggleVector::all_on();
        for part in spec.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            let (name, value) = part
                .split_once('=')
                .ok_or_else(|| format!("bad module spec {:?}: expected name=on|off", part))?;
            let idx = REGISTRY
                .iter()
                .position(|m| m.name == name)
                .ok_or_else(|| format!("unknown module {:?}", name))?;
            let (state, weight) = match value.split_once(':') {
                Some((s, w)) => {
                    let w: f64 = w
                        .parse()
                        .map_err(|_| format!("bad weight {:?} for module {:?}", w, name))?;
                    if !(w > 0.0 && w.is_finite()) {
                        return Err(format!("weight for module {:?} must be finite and > 0", name));
                    }
                    (s, Some(w))
                }
                None => (value, None),
            };
            let on = match state {
                "on" => true,
                "off" => false,
                _ => {
                    return Err(format!(
                        "bad state {:?} for module {:?}: expected on or off",
                        state, name
                    ))
                }
            };
            tv.entries[idx].on = on;
            if let Some(w) = weight {
                tv.entries[idx].weight = w;
            }
        }
        if !tv.entries.iter().any(|t| t.on) {
            return Err("toggle vector disables every module".to_string());
        }
        Ok(tv)
    }

    /// Canonical spec string; `parse(spec_string(tv)) == tv`.
    pub fn spec_string(&self) -> String {
        let mut out = String::new();
        for (m, t) in REGISTRY.iter().zip(&self.entries) {
            if !out.is_empty() {
                out.push(',');
            }
            out.push_str(m.name);
            out.push('=');
            out.push_str(if t.on { "on" } else { "off" });
            if t.weight != m.default_weight {
                out.push(':');
                out.push_str(&format!("{}", t.weight));
            }
        }
        out
    }

    pub fn is_on(&self, name: &str) -> bool {
        REGISTRY
            .iter()
            .position(|m| m.name == name)
            .is_some_and(|i| self.entries[i].on)
    }

    /// Weighted pick among enabled modules; returns the module name.
    pub fn pick_module(&self, rng: &mut Rng) -> &'static str {
        let enabled: Vec<(usize, f64)> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(_, t)| t.on)
            .map(|(i, t)| (i, t.weight))
            .collect();
        debug_assert!(!enabled.is_empty());
        let total: f64 = enabled.iter().map(|(_, w)| w).sum();
        let mut x = rng.f64_unit() * total;
        for &(i, w) in &enabled {
            if x < w {
                return REGISTRY[i].name;
            }
            x -= w;
        }
        REGISTRY[enabled.last().unwrap().0].name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_round_trip() {
        let tv = ToggleVector::parse("expr=on").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        let tv = ToggleVector::parse("expr=on:2.5").unwrap();
        assert_eq!(ToggleVector::parse(&tv.spec_string()).unwrap(), tv);
        assert!(tv.is_on("expr"));
    }

    #[test]
    fn parse_rejects_unknown_and_bad() {
        assert!(ToggleVector::parse("joins=on").is_err());
        assert!(ToggleVector::parse("expr=maybe").is_err());
        assert!(ToggleVector::parse("expr=on:0").is_err());
        assert!(ToggleVector::parse("expr=off").is_err()); // all-off
    }

    #[test]
    fn swarm_is_seed_deterministic() {
        let a = ToggleVector::swarm(&mut Rng::new(9));
        let b = ToggleVector::swarm(&mut Rng::new(9));
        assert_eq!(a, b);
        assert!(a.entries.iter().any(|t| t.on));
    }

    #[test]
    fn pick_module_returns_enabled() {
        let tv = ToggleVector::all_on();
        let mut rng = Rng::new(3);
        for _ in 0..64 {
            let m = tv.pick_module(&mut rng);
            assert!(tv.is_on(m));
        }
    }
}
