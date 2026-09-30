//! Eviction hints from completed clients; these never become GC roots.
use crate::{gc::Snapshot, manifest::valid_path};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};
use std::{
    fs,
    path::Path,
    time::{Duration, UNIX_EPOCH},
};

pub(crate) struct CacheUsage(Connection);

impl CacheUsage {
    pub(crate) fn open(base: &Path) -> Result<Self> {
        let connection = Connection::open(base.join("cache-usage.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.execute_batch("CREATE TABLE IF NOT EXISTS uses (path TEXT PRIMARY KEY NOT NULL, used_at INTEGER NOT NULL) WITHOUT ROWID;")?;
        Ok(Self(connection))
    }

    pub(crate) fn record(&mut self, roots: &Path) -> Result<()> {
        let tx = self.0.transaction()?;
        {
            let mut insert = tx.prepare("INSERT INTO uses VALUES (?1,?2) ON CONFLICT(path) DO UPDATE SET used_at=MAX(used_at,excluded.used_at)")?;
            for entry in fs::read_dir(roots)? {
                let entry = entry?;
                let target = fs::read_link(entry.path())?;
                let path = target.to_str().context("client root path")?;
                ensure!(valid_path(path), "invalid client root");
                let used_at = fs::symlink_metadata(entry.path())?
                    .modified()?
                    .duration_since(UNIX_EPOCH)?
                    .as_secs();
                insert.execute(params![path, used_at])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn apply(&mut self, snapshot: &mut Snapshot) -> Result<()> {
        let tx = self.0.transaction()?;
        {
            let mut select = tx.prepare("SELECT path,used_at FROM uses")?;
            let mut delete = tx.prepare("DELETE FROM uses WHERE path=?1")?;
            let records = select
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            for (path, used_at) in records {
                if snapshot.graph.contains_key(&path) {
                    snapshot.last_used.insert(path, used_at);
                } else {
                    delete.execute([path])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{collections::BTreeSet, os::unix::fs::symlink};

    #[test]
    fn completed_usage_survives_restart_without_rooting_or_leaking_missing_paths() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let roots = dir.path().join("roots");
        fs::create_dir(&roots)?;
        let path = "/nix/store/11111111111111111111111111111111-reused";
        symlink(path, roots.join("reused"))?;
        let mut usage = CacheUsage::open(dir.path())?;
        usage.record(&roots)?;
        let mut snapshot = Snapshot::default();
        snapshot.graph.insert(path.into(), BTreeSet::new());
        usage.apply(&mut snapshot)?;
        let recorded = snapshot.last_used[path];
        ensure!(recorded > 0 && snapshot.live.is_empty());
        usage.record(&roots)?;
        drop(usage);
        fs::remove_dir_all(&roots)?;
        let mut usage = CacheUsage::open(dir.path())?;
        snapshot.last_used.clear();
        usage.apply(&mut snapshot)?;
        ensure!(snapshot.last_used[path] == recorded);
        usage.apply(&mut Snapshot::default())?;
        snapshot.last_used.clear();
        usage.apply(&mut snapshot)?;
        ensure!(snapshot.last_used.is_empty());
        Ok(())
    }
}
