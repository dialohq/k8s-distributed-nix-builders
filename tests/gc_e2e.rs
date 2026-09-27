//! Real coordinated GC, including durable pause/retry and native root semantics.
use anyhow::{Context, Result, ensure};
use distributed_nix::{
    cluster::Cluster,
    node::{BIN, ORIGIN, ROOT},
    util::*,
};
use serde_json::{Value, json};
use std::{
    os::unix::process::ExitStatusExt,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Suite {
    c: Cluster,
    tag: String,
    runtime: Value,
    evidence: Value,
}
impl Suite {
    fn check(&mut self, name: &str, yes: bool) -> Result<()> {
        self.evidence["checks"][name] = json!(yes);
        self.save()?;
        ensure!(yes, "FAILED {name}");
        println!("PASS {name}");
        Ok(())
    }
    fn save(&self) -> Result<()> {
        durable(
            &self.c.repo.join("results/distributed-nix/gc-e2e.json"),
            &self.evidence,
        )
    }
    fn expr(&self, suffix: &str, body: &str, extra: &str) -> String {
        format!(
            "builtins.derivation {{ name={}; system=\"x86_64-linux\"; builder=(builtins.storePath {})+\"/bin/bash\"; tools=builtins.storePath {}; {extra} args=[\"-c\" {}]; }}",
            json!(format!("{}-{suffix}", self.tag)),
            self.runtime["bash"],
            self.runtime["coreutils"],
            json!(body)
        )
    }
    fn build(&self, node: usize, suffix: &str, body: &str, extra: &str) -> Result<String> {
        let file = format!("/work/{}-{suffix}.nix", self.tag);
        self.c.put(
            node,
            &format!("{ROOT}{file}"),
            self.expr(suffix, body, extra).as_bytes(),
        )?;
        Ok(String::from_utf8(
            self.c
                .exec(node, &format!("nix-build {file} --no-out-link"))?
                .stdout,
        )?
        .trim()
        .into())
    }
    fn exists(&self, node: usize, path: &str) -> Result<bool> {
        Ok(self
            .c
            .remote_unchecked(node, &format!("test -e {}", sh(path)))?
            .status
            .success())
    }
    fn wait(&self, node: usize, command: &str) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            if self.c.remote_unchecked(node, command)?.status.success() {
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "timeout: {command}");
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    fn drain_outbox(&self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let mut empty = true;
            for n in 0..3 {
                empty &= self
                    .c
                    .call_json(n, &["outbox".into()])?
                    .as_array()
                    .unwrap()
                    .is_empty();
            }
            if empty {
                return Ok(());
            }
            ensure!(Instant::now() < deadline, "publication queue did not drain");
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    fn killed_gc(&self, point: &str) -> Result<()> {
        let o = Command::new(env!("CARGO_BIN_EXE_distributed-nix"))
            .arg("gc")
            .env("DISTRIBUTED_NIX_FAILPOINT", point)
            .output()?;
        ensure!(
            o.status.signal() == Some(9),
            "GC failpoint not reached: {} {}",
            o.status,
            String::from_utf8_lossy(&o.stderr)
        );
        Ok(())
    }
    fn epoch(&self) -> Result<String> {
        Ok(self
            .c
            .call_json(0, &["gc-master".into(), "status".into()])?["id"]
            .as_str()
            .context("epoch")?
            .into())
    }
    fn run(&mut self) -> Result<()> {
        let core = self.runtime["coreutils"].as_str().unwrap().to_string();
        let dep = self.build(
            0,
            "dep",
            &format!("{core}/bin/mkdir -p \"$out\"; echo dependency > \"$out/data\""),
            "",
        )?;
        self.c.publish(0, std::slice::from_ref(&dep))?;
        let keep = self.build(
            0,
            "keep",
            "printf %s \"$dep\" > \"$out\"",
            &format!("dep=builtins.storePath {};", json!(dep)),
        )?;
        self.c.publish(0, std::slice::from_ref(&keep))?;
        // The only real root for this shared closure is an indirect root on node 1.
        self.c.exec(
            1,
            &format!(
                "nix-store --add-root /work/{}-keep --indirect --realise {keep}",
                self.tag
            ),
        )?;
        let local = self.build(
            2,
            "local",
            "printf %s \"$dep\" > \"$out\"",
            &format!("dep=builtins.storePath {};", json!(dep)),
        )?;
        self.c.exec(
            2,
            &format!("ln -s {local} /nix/var/nix/profiles/{}-profile", self.tag),
        )?;
        let dead = self.build(
            0,
            "dead",
            &format!("{core}/bin/mkdir -p \"$out\"; echo garbage > \"$out/data\""),
            "",
        )?;
        let publication = self.c.publish(0, std::slice::from_ref(&dead))?;
        let batch = publication["batch"].as_str().unwrap().to_string();
        // Native mount/file/symlink deletion all use the same immutable plan.
        let large = self.build(
            0,
            "large",
            &format!("{core}/bin/head -c 131072 /dev/zero > \"$out\""),
            "",
        )?;
        let symlink = self.build(
            0,
            "link",
            &format!("{core}/bin/ln -s {dep}/data \"$out\""),
            "",
        )?;
        self.c.publish(0, &[large.clone(), symlink.clone()])?;
        let small = self.build(0, "small", "echo tiny > \"$out\"", "")?;
        self.c.publish(0, std::slice::from_ref(&small))?;
        self.drain_outbox()?;
        let preview = self.c.gc(true)?;
        self.check(
            "dry_run_keeps_indirect_root_and_its_reference",
            preview["plan"]["keep"]
                .as_array()
                .unwrap()
                .contains(&json!(keep))
                && preview["plan"]["keep"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(dep)),
        )?;
        self.check(
            "dry_run_finds_garbage_without_deleting",
            preview["plan"]["origin"]
                .as_array()
                .unwrap()
                .contains(&json!(dead))
                && self.exists(0, &format!("{ORIGIN}{dead}/data"))?,
        )?;
        self.check(
            "profile_root_is_kept",
            preview["plan"]["keep"]
                .as_array()
                .unwrap()
                .contains(&json!(local)),
        )?;
        // Already-offline participant: no node should enter maintenance or delete.
        self.c.remote(2, &format!("sync -f {ROOT}"))?;
        run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["stop", "2"]))?;
        let failed = self.c.gc(false).is_err();
        let restarted = run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["start", "2"]));
        self.check(
            "offline_node_prevents_deletion",
            failed && self.exists(0, &format!("{ORIGIN}{dead}/data"))?,
        )?;
        self.check(
            "offline_preflight_does_not_pause_online_clients",
            self.c.exec(1, "true").is_ok(),
        )?;
        restarted?;
        self.wait(2, "systemctl is-active --quiet distributed-nix-recover")?;
        // Native-client draining is checked by default_tooling. Here exercise
        // the durable coordinator failure boundary and its exact saved plan.
        self.drain_outbox()?;
        self.killed_gc("gc-coordinator-after-plan")?;
        self.check(
            "killed_coordinator_leaves_clients_and_recovery_gated",
            self.c.exec(1, "true").is_err() && self.c.call(2, &["recover".into()]).is_err(),
        )?;
        self.check(
            "origin_not_deleted_before_worker_acknowledgements",
            self.exists(0, &format!("{ORIGIN}{dead}/data"))?,
        )?;
        let id = self.epoch()?;
        let o = self.c.remote_unchecked(
            2,
            &format!("env DISTRIBUTED_NIX_FAILPOINT=gc-after-journal-rename {BIN} node gc-sweep {id} 2"),
        )?;
        self.check("journal_swap_crash_injected", !o.status.success())?;
        let o = self.c.remote_unchecked(
            1,
            &format!("env DISTRIBUTED_NIX_FAILPOINT=gc-after-unmount {BIN} node gc-sweep {id} 1"),
        )?;
        self.check("unmount_crash_injected", !o.status.success())?;
        // A reboot during GC must not replay the retired, full admission journal.
        let oldboot = String::from_utf8(
            self.c
                .remote(2, "cat /proc/sys/kernel/random/boot_id")?
                .stdout,
        )?;
        self.c.remote_unchecked(2, "systemctl reboot")?;
        self.wait(
            2,
            &format!(
                "test \"$(cat /proc/sys/kernel/random/boot_id)\" != {}",
                sh(oldboot.trim())
            ),
        )?;
        self.check("gc_pause_survives_reboot", self.c.exec(2, "true").is_err())?;
        let completed = self.c.gc(false)?;
        self.check("retry_uses_same_plan", completed["id"] == id)?;
        for n in 0..3 {
            for p in [&dead, &large, &symlink, &small] {
                self.check(
                    &format!(
                        "dead_path_absent_node_{n}_{}",
                        p.rsplit('-').next().unwrap()
                    ),
                    !self.exists(n, &format!("{ROOT}{p}"))?,
                )?;
            }
            self.c.exec(
                n,
                &format!("cat {dep}/data; nix store verify --no-trust {keep} {dep}"),
            )?;
        }
        self.check(
            "shared_files_and_old_publication_removed",
            !self.exists(0, &format!("{ORIGIN}{dead}"))?
                && !self.exists(0, &format!("{ORIGIN}/.distributed-nix-publications/{batch}.json"))?,
        )?;
        self.check(
            "retired_publication_cannot_be_reconciled",
            self.c.reconcile(&batch).is_err(),
        )?;
        self.check(
            "rooted_local_output_survives",
            self.c
                .exec(2, &format!("nix store verify --no-trust {local}"))
                .is_ok(),
        )?;
        // Rebuild the identical name immediately. Refreshed NFS views must not
        // hand back stale file handles from the just-collected incarnation.
        let again = self.build(
            0,
            "dead",
            &format!("{core}/bin/mkdir -p \"$out\"; echo garbage > \"$out/data\""),
            "",
        )?;
        ensure!(again == dead, "rebuilt path changed");
        self.c.publish(0, std::slice::from_ref(&again))?;
        self.c.parallel(|n| {
            self.c.exec(
                n,
                &format!("cat {again}/data; nix store verify --no-trust {again}"),
            )
        })?;
        self.check(
            "collected_path_can_be_rebuilt_and_republished_immediately",
            true,
        )?;
        // Second cycle: crash after all worker deletions, then during origin retirement.
        self.drain_outbox()?;
        self.killed_gc("gc-coordinator-after-workers")?;
        let second = self.epoch()?;
        self.check(
            "workers_deleted_before_origin",
            !self.exists(1, &format!("{ROOT}{again}"))?
                && self.exists(0, &format!("{ORIGIN}{again}"))?,
        )?;
        let o=self.c.remote_unchecked(0,&format!("env DISTRIBUTED_NIX_FAILPOINT=gc-after-publication-retire {BIN} node gc-sweep {second} origin"))?;
        self.check("origin_retirement_crash_injected", !o.status.success())?;
        self.c.gc(false)?;
        self.check(
            "origin_retry_completes_deletion",
            !self.exists(0, &format!("{ORIGIN}{again}"))?,
        )?;
        // Remove real roots: their transitive shared packages now become eligible.
        self.c.exec(1, &format!("rm /work/{}-keep", self.tag))?;
        self.c
            .exec(2, &format!("rm /nix/var/nix/profiles/{}-profile", self.tag))?;
        self.c.gc(false)?;
        self.check(
            "removing_last_roots_collects_shared_dependency",
            !self.exists(0, &format!("{ORIGIN}{dep}"))?
                && !self.exists(0, &format!("{ORIGIN}{keep}"))?,
        )?;
        for n in 0..3 {
            self.c.call(n, &["recover".into()])?;
            self.check(
                &format!("recovery_does_not_resurrect_gc_paths_{n}"),
                !self.exists(n, &format!("{ROOT}{dep}"))?,
            )?;
            let integrity=String::from_utf8(self.c.remote(n,&format!("sqlite3 {ROOT}/nix/var/nix/db/db.sqlite 'PRAGMA integrity_check; PRAGMA foreign_key_check;'"))?.stdout)?;
            self.check(&format!("database_integrity_{n}"), integrity.trim() == "ok")?;
        }
        self.evidence["complete"] = json!(true);
        self.evidence["first_gc"] = completed;
        self.save()
    }
}
#[test]
#[ignore = "three-VM destructive GC test: collects unrooted packages and restarts node2"]
fn coordinated_gc_end_to_end() -> Result<()> {
    let c = Cluster::new()?;
    let runtime = read_json(&c.repo.join("manifest.json"))?;
    let tag = format!(
        "gc-e2e-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut s = Suite {
        c,
        tag: tag.clone(),
        runtime,
        evidence: json!({"tag":tag,"checks":{},"complete":false}),
    };
    let result = s.run();
    if let Err(e) = &result {
        s.evidence["error"] = json!(format!("{e:#}"));
        s.save()?;
    }
    result
}
