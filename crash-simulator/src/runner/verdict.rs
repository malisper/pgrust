//! Verdict + artifact conventions (contract §4.1.7, house style, exact):
//!   stdout: `SIMHARNESS|<class>|<n>` one line per observed class, then
//!           `SIMHARNESS-VERDICT|PASS` or `SIMHARNESS-VERDICT|FAIL:<p1>,...`
//!   files:  `<out>/outcomes.tsv`, `findings.jsonl`, `summary.json`
//!           (triage.py conventions).
//! The class→severity table mirrors the §1.3 pinned vocabulary; at
//! integration WS-ORACLE's `src/vocab.rs` becomes the single source and this
//! table is replaced by an import (worklog coordination item).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

/// Pinned severity for verdict purposes (P1 classes fail the run).
/// Post-integration the single source is `crate::vocab` (§1.3 pinned classes
/// + the folded harness-mechanics table).
pub fn severity(class: &str) -> &'static str {
    crate::vocab::severity_of(class).as_str()
}

#[derive(Debug, Default, Clone)]
pub struct Census {
    pub counts: BTreeMap<String, u64>,
}

impl Census {
    pub fn add(&mut self, class: &str, n: u64) {
        *self.counts.entry(class.to_string()).or_insert(0) += n;
    }

    pub fn merge(&mut self, other: &BTreeMap<String, u64>) {
        for (k, v) in other {
            self.add(k, *v);
        }
    }

    pub fn p1_classes(&self) -> Vec<String> {
        self.counts
            .iter()
            .filter(|(k, v)| **v > 0 && severity(k) == "P1")
            .map(|(k, _)| k.clone())
            .collect()
    }

    /// Emit the exact house grammar to `w`. BTreeMap order = deterministic.
    pub fn emit(&self, w: &mut dyn Write) -> std::io::Result<()> {
        for (k, v) in &self.counts {
            writeln!(w, "SIMHARNESS|{}|{}", k, v)?;
        }
        let p1 = self.p1_classes();
        if p1.is_empty() {
            writeln!(w, "SIMHARNESS-VERDICT|PASS")
        } else {
            writeln!(w, "SIMHARNESS-VERDICT|FAIL:{}", p1.join(","))
        }
    }
}

pub struct ArtifactWriter {
    outcomes: std::fs::File,
    findings: std::fs::File,
    out_dir: std::path::PathBuf,
    idx: u64,
    findings_per_class: BTreeMap<String, u64>,
}

const MAX_FINDINGS_PER_CLASS: u64 = 25; // triage.py convention

impl ArtifactWriter {
    pub fn new(out_dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(out_dir).map_err(|e| e.to_string())?;
        let mut outcomes =
            std::fs::File::create(out_dir.join("outcomes.tsv")).map_err(|e| e.to_string())?;
        writeln!(outcomes, "idx\tseed\tclass\tsev\tdetail\tstmt_head").map_err(|e| e.to_string())?;
        let findings =
            std::fs::File::create(out_dir.join("findings.jsonl")).map_err(|e| e.to_string())?;
        Ok(ArtifactWriter {
            outcomes,
            findings,
            out_dir: out_dir.to_path_buf(),
            idx: 0,
            findings_per_class: BTreeMap::new(),
        })
    }

    pub fn record(&mut self, seed: u64, class: &str, sev: &str, detail: &str, stmt_head: &str) {
        let clean = |s: &str| s.replace(['\t', '\n'], " ");
        let _ = writeln!(
            self.outcomes,
            "{}\t{}\t{}\t{}\t{}\t{}",
            self.idx,
            seed,
            class,
            sev,
            clean(detail),
            clean(stmt_head)
        );
        if sev != "fine" {
            let n = self.findings_per_class.entry(class.to_string()).or_insert(0);
            if *n < MAX_FINDINGS_PER_CLASS {
                *n += 1;
                let _ = writeln!(
                    self.findings,
                    "{}",
                    serde_json::json!({
                        "idx": self.idx, "seed": seed, "class": class, "sev": sev,
                        "detail": detail, "stmt": stmt_head,
                    })
                );
            }
        }
        self.idx += 1;
    }

    pub fn finish(mut self, census: &Census, extra: serde_json::Value) -> Result<(), String> {
        let _ = self.outcomes.flush();
        let _ = self.findings.flush();
        let summary = serde_json::json!({
            "counts": census.counts,
            "p1_classes": census.p1_classes(),
            "extra": extra,
        });
        std::fs::write(
            self.out_dir.join("summary.json"),
            serde_json::to_string_pretty(&summary).map_err(|e| e.to_string())? + "\n",
        )
        .map_err(|e| e.to_string())
    }
}
