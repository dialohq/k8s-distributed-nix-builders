//! Local admission bookkeeping, separate from the native Nix database.
use crate::{
    node::{Journal, Kind, Status},
    util::{failpoint, syncdir},
};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::{collections::BTreeMap, fs, path::Path, time::Duration};

pub struct Admissions(Connection);

impl Admissions {
    /// The caller must fence admission and GC directory replacement for this handle's lifetime.
    pub fn open(directory: &Path) -> Result<Self> {
        fs::create_dir_all(directory)?;
        let mut connection = Connection::open(directory.join("admissions.sqlite"))?;
        connection.busy_timeout(Duration::from_secs(30))?;
        connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA;")?;
        let version: u32 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(
            version <= 1,
            "unsupported admission database version: {version}"
        );
        if version == 0 {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(
                "CREATE TABLE batches (
                    id TEXT PRIMARY KEY NOT NULL,
                    manifest TEXT NOT NULL,
                    plan TEXT NOT NULL,
                    committed INTEGER NOT NULL CHECK(committed IN (0, 1))
                );
                CREATE TABLE paths (
                    path TEXT PRIMARY KEY NOT NULL,
                    kind TEXT NOT NULL CHECK(kind IN ('local','symlink','copy','mount-dir','mount-file'))
                ) WITHOUT ROWID;",
            )?;
            tx.execute_batch("PRAGMA user_version=1;")?;
            tx.commit()?;
            syncdir(directory)?;
            if let Some(parent) = directory.parent() {
                syncdir(parent)?;
            }
        }
        connection.execute_batch("CREATE TABLE IF NOT EXISTS relocations (path TEXT PRIMARY KEY NOT NULL, info TEXT NOT NULL) WITHOUT ROWID;")?;
        Ok(Self(connection))
    }

    pub fn queue_relocations(&mut self, paths: &BTreeMap<String, serde_json::Value>) -> Result<()> {
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (path, info) in paths {
            tx.execute("INSERT INTO relocations VALUES (?1,?2) ON CONFLICT(path) DO NOTHING", params![path, serde_json::to_string(info)?])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn relocations(&self) -> Result<BTreeMap<String, serde_json::Value>> {
        let rows = self
            .0
            .prepare("SELECT path,info FROM relocations ORDER BY path")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter()
            .map(|(path, info)| Ok((path, serde_json::from_str(&info)?)))
            .collect()
    }

    pub fn relocated(&mut self, kinds: &BTreeMap<String, Kind>) -> Result<()> {
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let replacements = serde_json::to_string(kinds)?;
        // Update only keys already in each batch; manifests remain unchanged.
        tx.execute("UPDATE batches SET plan=(SELECT json_group_object(key, coalesce(json_extract(?1, '$.\"' || key || '\"'), value)) FROM json_each(batches.plan)) WHERE EXISTS (SELECT 1 FROM json_each(batches.plan) p JOIN json_each(?1) n ON p.key=n.key)", [&replacements])?;
        for (path, kind) in kinds {
            let kind = serde_json::to_value(kind)?.as_str().unwrap().to_owned();
            ensure!(
                tx.execute(
                    "UPDATE paths SET kind=?2 WHERE path=?1",
                    params![path, kind]
                )? == 1,
                "missing relocated path"
            );
            tx.execute("DELETE FROM relocations WHERE path=?1", [path])?;
        }
        failpoint("relocation-before-database-commit");
        tx.commit()?;
        Ok(())
    }

    fn insert(connection: &Connection, journal: &Journal) -> Result<()> {
        let id = journal.manifest.id()?;
        journal.validate(&id)?;
        let mut insert =
            connection.prepare("INSERT INTO paths VALUES (?1, ?2) ON CONFLICT(path) DO NOTHING")?;
        let mut lookup = connection.prepare("SELECT kind FROM paths WHERE path=?1")?;
        for (path, kind) in &journal.plan {
            let kind = serde_json::to_value(kind)?.as_str().unwrap().to_owned();
            insert.execute(params![path, kind])?;
            let previous: String = lookup.query_row([path], |r| r.get(0))?;
            ensure!(previous == kind, "inconsistent admission plan: {path}");
        }
        connection.execute(
            "INSERT INTO batches VALUES (?1, ?2, ?3, ?4)",
            params![
                id,
                serde_json::to_string(&journal.manifest)?,
                serde_json::to_string(&journal.plan)?,
                matches!(journal.status, Status::Committed)
            ],
        )?;
        Ok(())
    }

    pub fn begin(&mut self, journal: &Journal) -> Result<()> {
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Self::insert(&tx, journal)?;
        failpoint("admissions-before-commit");
        tx.commit()?;
        Ok(())
    }

    pub fn commit(&self, id: &str) -> Result<()> {
        ensure!(
            self.0
                .execute("UPDATE batches SET committed=1 WHERE id=?1", [id])?
                == 1,
            "missing admission batch: {id}"
        );
        Ok(())
    }

    pub fn get(&self, id: &str) -> Result<Option<Journal>> {
        let row: Option<(String, String, bool)> = self
            .0
            .query_row(
                "SELECT manifest, plan, committed FROM batches WHERE id=?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        row.map(|(manifest, plan, committed)| {
            let journal = Journal {
                manifest: serde_json::from_str(&manifest)?,
                plan: serde_json::from_str(&plan)?,
                status: if committed {
                    Status::Committed
                } else {
                    Status::Pending
                },
            };
            journal.validate(id)?;
            ensure!(
                self.known(journal.plan.keys())? == journal.plan,
                "admission index differs from batch plan"
            );
            Ok(journal)
        })
        .transpose()
    }

    pub fn known<'a>(
        &self,
        paths: impl IntoIterator<Item = &'a String>,
    ) -> Result<BTreeMap<String, Kind>> {
        let tx = self.0.unchecked_transaction()?;
        let mut statement = tx.prepare("SELECT kind FROM paths WHERE path=?1")?;
        let mut result = BTreeMap::new();
        for path in paths {
            let kind: Option<String> = statement.query_row([path], |r| r.get(0)).optional()?;
            if let Some(kind) = kind {
                result.insert(
                    path.clone(),
                    serde_json::from_value(serde_json::Value::String(kind))?,
                );
            }
        }
        Ok(result)
    }

    pub fn ids(&self) -> Result<Vec<String>> {
        Ok(self
            .0
            .prepare("SELECT id FROM batches ORDER BY id")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    pub fn for_each(&self, mut visit: impl FnMut(Journal) -> Result<()>) -> Result<()> {
        for id in self.ids()? {
            visit(self.get(&id)?.expect("fenced admission batch"))?;
        }
        Ok(())
    }
}
