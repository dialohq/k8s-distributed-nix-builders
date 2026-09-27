use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub version: u64,
    pub roots: Vec<String>,
    pub paths: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub realisations: BTreeMap<String, Value>,
}
pub fn realisation_path(value: &Value) -> Result<String> {
    let path = format!(
        "/nix/store/{}",
        value.as_str().context("realisation output path")?
    );
    ensure!(valid_path(&path), "invalid realisation output path");
    Ok(path)
}
pub fn valid_path(p: &str) -> bool {
    let Some(s) = p.strip_prefix("/nix/store/") else {
        return false;
    };
    s.len() > 33
        && s.as_bytes()[32] == b'-'
        && s.as_bytes()[..32]
            .iter()
            .all(|b| b"0123456789abcdfghijklmnpqrsvwxyz".contains(b))
        && s.as_bytes()[33..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || b"+-._?=".contains(b))
}
impl Manifest {
    pub fn parse(v: Value) -> Result<Self> {
        let m: Self = serde_json::from_value(v)?;
        m.validate()?;
        Ok(m)
    }
    pub fn read(p: &Path) -> Result<Self> {
        Self::parse(crate::util::read_json(p)?)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == 1 && !self.paths.is_empty() && !self.roots.is_empty(),
            "unsupported or empty manifest"
        );
        ensure!(
            self.roots.iter().collect::<BTreeSet<_>>().len() == self.roots.len(),
            "duplicate roots"
        );
        for (p, info) in &self.paths {
            ensure!(valid_path(p), "invalid store path: {p}");
            ensure!(
                info["narHash"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("sha256-") && s.len() == 51),
                "missing/invalid NAR hash: {p}"
            );
            ensure!(
                info["narSize"].as_u64().is_some_and(|n| n > 0),
                "missing/invalid NAR size: {p}"
            );
            for r in info["references"]
                .as_array()
                .context("missing references array")?
            {
                let r = r.as_str().context("reference must be a string")?;
                ensure!(
                    valid_path(r) && self.paths.contains_key(r),
                    "closure missing reference: {r}"
                );
            }
            if let Some(d) = info.get("deriver").filter(|d| !d.is_null()) {
                ensure!(d.as_str().is_some_and(valid_path), "invalid deriver");
            }
        }
        ensure!(
            self.roots.iter().all(|p| self.paths.contains_key(p)),
            "root missing from closure"
        );
        for (id, r) in &self.realisations {
            ensure!(r["id"].as_str() == Some(id), "realisation ID mismatch");
            ensure!(
                self.paths.contains_key(&realisation_path(&r["outPath"])?),
                "realisation output missing from closure"
            );
            for (dep, path) in r["dependentRealisations"]
                .as_object()
                .context("realisation dependencies")?
            {
                ensure!(
                    self.realisations
                        .get(dep)
                        .is_some_and(|r| r["outPath"] == *path),
                    "missing realisation dependency"
                );
            }
        }
        Ok(())
    }
    pub fn id(&self) -> Result<String> {
        // Value's map uses sorted keys, independent of input JSON key ordering.
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&serde_json::to_value(self)?)?)
        ))
    }

    pub fn realisation_closure(&self, ids: &[String]) -> Result<Self> {
        let mut result = Self {
            version: self.version,
            roots: Vec::new(),
            paths: BTreeMap::new(),
            realisations: BTreeMap::new(),
        };
        let mut pending = ids.to_vec();
        let mut roots = BTreeSet::new();
        while let Some(id) = pending.pop() {
            if result.realisations.contains_key(&id) {
                continue;
            }
            let record = self.realisations.get(&id).context("missing realisation")?;
            roots.insert(realisation_path(&record["outPath"])?);
            pending.extend(
                record["dependentRealisations"]
                    .as_object()
                    .context("realisation dependencies")?
                    .keys()
                    .cloned(),
            );
            result.realisations.insert(id, record.clone());
        }
        result.roots = roots.into_iter().collect();
        let mut pending = result.roots.clone();
        while let Some(path) = pending.pop() {
            if result.paths.contains_key(&path) {
                continue;
            }
            let info = self.paths.get(&path).context("missing closure path")?;
            pending.extend(
                info["references"]
                    .as_array()
                    .context("references")?
                    .iter()
                    .map(|v| v.as_str().map(String::from).context("reference path"))
                    .collect::<Result<Vec<_>>>()?,
            );
            result.paths.insert(path, info.clone());
        }
        result.validate()?;
        Ok(result)
    }

    pub fn blocked_realisations(&self, mut blocked: BTreeSet<String>) -> Result<BTreeSet<String>> {
        loop {
            let previous = blocked.len();
            for (id, record) in &self.realisations {
                if record["dependentRealisations"]
                    .as_object()
                    .context("realisation dependencies")?
                    .keys()
                    .any(|dep| blocked.contains(dep))
                {
                    blocked.insert(id.clone());
                }
            }
            if blocked.len() == previous {
                return Ok(blocked);
            }
        }
    }
}
