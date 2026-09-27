//! Historical fault-injection suite for an isolated, initialized three-node lab.
//! Production ARC validation lives in cibox/tests; these tests remain opt-in.
use anyhow::{Context, Result, ensure};
use distributed_nix::{
    cluster::Cluster,
    manifest::Manifest,
    node::{BASE, BIN, ORIGIN, ROOT},
    util::*,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Suite {
    c: Cluster,
    tag: String,
    runtime: Value,
    report: Value,
    evidence: PathBuf,
}
impl Suite {
    fn new() -> Result<Self> {
        let c = Cluster::new()?;
        let tag = format!(
            "rust-e2e-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let runtime = read_json(&c.repo.join("manifest.json"))?;
        let evidence = c.repo.join("results/distributed-nix/e2e-latest.json");
        Ok(Self {
            c,
            tag: tag.clone(),
            runtime,
            report: json!({"tag":tag,"checks":{},"complete":false}),
            evidence,
        })
    }
    fn check(&mut self, name: &str, value: bool) -> Result<()> {
        self.report["checks"][name] = json!(value);
        durable(&self.evidence, &self.report)?;
        ensure!(value, "FAILED {name}");
        println!("PASS {name}");
        Ok(())
    }
    fn record(&mut self, key: &str, v: Value) -> Result<()> {
        self.report[key] = v;
        durable(&self.evidence, &self.report)
    }
    fn raw_ok(&self, n: usize, cmd: &str) -> Result<bool> {
        Ok(self.c.remote_unchecked(n, cmd)?.status.success())
    }
    fn wait(&self, n: usize, cmd: &str, seconds: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline {
            if self.raw_ok(n, cmd)? {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        anyhow::bail!("timeout waiting on node {n}: {cmd}")
    }
    fn expr(&self, name: &str, body: &str, extra: &str) -> String {
        format!(
            "builtins.derivation {{ name={}; system=\"x86_64-linux\"; builder=(builtins.storePath {})+\"/bin/bash\"; tools=builtins.storePath {}; {extra} args=[\"-c\" {}]; }}",
            json!(name),
            self.runtime["bash"],
            self.runtime["coreutils"],
            json!(body)
        )
    }
    fn build_expr(&self, n: usize, name: &str, expr: &str) -> Result<String> {
        self.c
            .put(n, &format!("{ROOT}/work/{name}.nix"), expr.as_bytes())?;
        let o = self
            .c
            .exec(n, &format!("nix-build /work/{name}.nix --no-out-link"))?;
        Ok(String::from_utf8(o.stdout)?.trim().to_string())
    }
    fn tiny(&self, n: usize, suffix: &str) -> Result<String> {
        let name = format!("{}-{suffix}", self.tag);
        let core = self.runtime["coreutils"].as_str().unwrap();
        let script = format!(
            "#!{}/bin/bash\necho {name}\n",
            self.runtime["bash"].as_str().unwrap()
        );
        let body = format!(
            "{core}/bin/mkdir -p \"$out/bin\"; printf %s {} > \"$out/bin/probe\"; {core}/bin/chmod +x \"$out/bin/probe\"",
            sh(&script)
        );
        self.build_expr(n, &name, &self.expr(&name, &body, ""))
    }
    fn accepted(&self, m: &Manifest) -> Result<()> {
        let f = self.c.receive(0, m)?;
        self.c.call(0, &["commit".into(), f])?;
        Ok(())
    }
    fn run(&mut self) -> Result<()> {
        self.c.parallel(|n| self.c.call(n, &["recover".into()]))?;
        let seed: Vec<String> = self.runtime["seed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().into())
            .collect();
        let expected = self.c.dump(0, &seed, true)?;
        for n in 0..3 {
            self.check(
                &format!("linked_abi_without_helper_node_{n}"),
                self.raw_ok(n, &format!("test ! -e {BASE}/bin/design2-register"))?,
            )?;
        }
        for n in 0..3 {
            let local = self.c.dump(n, &seed, false)?;
            let exact = expected.paths.keys().eq(local.paths.keys())
                && expected.paths.iter().all(|(p, i)| {
                    [
                        "narHash",
                        "narSize",
                        "references",
                        "ca",
                        "deriver",
                        "signatures",
                    ]
                    .iter()
                    .all(|k| i.get(k) == local.paths[p].get(k))
                });
            self.check(&format!("full_seed_metadata_node_{n}"), exact)?;
        }
        self.record("seed_records", json!(expected.paths.len()))?;
        self.check(
            "seed_has_content_address_metadata",
            expected.paths.values().any(|v| v["ca"].is_string()),
        )?;
        let live = self.tiny(1, "live")?;
        self.c.exec(1,&format!("nix-store --generate-binary-cache-key rust-e2e /work/{}.key /work/{}.pub; nix store sign --key-file /work/{}.key {live}",self.tag,self.tag,self.tag))?;
        self.c.put(
            1,
            &format!("{ROOT}/work/{}.txt", self.tag),
            self.tag.as_bytes(),
        )?;
        let ca = String::from_utf8(
            self.c
                .exec(1, &format!("nix-store --add /work/{}.txt", self.tag))?
                .stdout,
        )?
        .trim()
        .to_string();
        let watch = format!("{}-watch", self.tag);
        let wd = format!("{ROOT}/work/{watch}");
        self.c.remote(2, &format!("mkdir -p {wd}"))?;
        let cmd = format!(
            "test ! -e {live}; if nix path-info {live}; then exit 1; fi; touch /work/{watch}/ready; while test ! -e /work/{watch}/go; do sleep 0.05; done; {live}/bin/probe > /work/{watch}/result; nix store verify --no-trust {live}; touch /work/{watch}/done"
        );
        self.c.remote(
            2,
            &format!(
                "systemd-run --setenv=PATH=/run/current-system/sw/bin --collect --unit={} {BIN} node exec {}",
                self.tag,
                sh(&cmd)
            ),
        )?;
        self.wait(2, &format!("test -e {wd}/ready"), 20)?;
        let pub1 = self.c.publish(1, &[live.clone(), ca.clone()])?;
        self.c.remote(2, &format!("touch {wd}/go"))?;
        self.wait(2, &format!("test -e {wd}/done"), 30)?;
        self.check(
            "running_namespace_sees_new_package_after_negative_lookup",
            String::from_utf8(self.c.remote(2, &format!("cat {wd}/result"))?.stdout)?.trim()
                == format!("{}-live", self.tag),
        )?;
        let info = self.c.dump(2, &[live.clone(), ca.clone()], false)?;
        self.check(
            "new_signature_preserved",
            !info.paths[&live]["signatures"]
                .as_array()
                .unwrap()
                .is_empty(),
        )?;
        self.check(
            "new_content_address_preserved",
            info.paths[&ca]["ca"].is_string(),
        )?;
        self.check(
            "small_source_copied_not_mounted",
            !self.raw_ok(2, &format!("mountpoint -q {ROOT}{ca}"))?,
        )?;
        self.check(
            "shared_package_read_only",
            !self.raw_ok(2, &format!("touch {ROOT}{live}/forbidden"))?,
        )?;
        self.check(
            "node_gc_keeps_shared_path",
            self.c
                .exec(2, &format!("nix-store --store local --delete {live}"))
                .is_err(),
        )?;
        self.check(
            "origin_gc_keeps_shared_path",
            !self.raw_ok(0, &format!("nix-store --store {ORIGIN} --delete {live}"))?,
        )?;
        let pub2 = self.c.publish(1, &[live.clone(), ca])?;
        self.check(
            "publication_retry_has_same_identity",
            pub1["batch"] == pub2["batch"],
        )?;
        self.record("live_publication", pub1)?;
        // Eight simultaneous admission retries exercise controller locks and idempotence.
        let live_m = self.c.dump(1, std::slice::from_ref(&live), false)?;
        self.c.publish(1, std::slice::from_ref(&live))?;
        std::thread::scope(|s| -> Result<()> {
            let jobs: Vec<_> = (0..8)
                .map(|_| s.spawn(|| self.c.admit(2, &live_m)))
                .collect();
            for j in jobs {
                j.join().unwrap()?;
            }
            Ok(())
        })?;
        self.check("concurrent_same_batch_admission", true)?;
        let mut bad = live_m.clone();
        bad.paths.get_mut(&live).unwrap()["narSize"] = json!(1);
        self.check(
            "unpublished_metadata_rejected",
            self.c.admit(2, &bad).is_err(),
        )?;
        let f = self.c.receive(0, &bad)?;
        let error = self
            .c
            .call(0, &["reserve".into(), f])
            .unwrap_err()
            .to_string();
        self.check(
            "native_identity_conflict_rejected",
            error.contains("conflicting already-registered"),
        )?;
        self.check(
            "conflict_leaves_original_executable",
            self.c.exec(2, &format!("{live}/bin/probe")).is_ok(),
        )?;
        // A forged store path is rejected by receive, before a journal or mount can exist.
        let mut malicious = serde_json::to_value(&live_m)?;
        malicious["roots"] = json!(["/etc/passwd"]);
        let bad_path = format!("{BASE}/malicious.json");
        self.c.put(2, &bad_path, &serde_json::to_vec(&malicious)?)?;
        self.check(
            "invalid_root_rejected",
            self.c.call(2, &["admit".into(), bad_path]).is_err(),
        )?;
        for fp in ["after-journal", "after-mounts", "after-register"] {
            let path = self.tiny(1, fp)?;
            self.c.publish_to(1, std::slice::from_ref(&path), &[0, 1])?;
            let m = self.c.dump(1, std::slice::from_ref(&path), false)?;
            let file = self.c.receive(2, &m)?;
            let killed = self.c.remote_unchecked(
                2,
                &format!(
                    "env DISTRIBUTED_NIX_FAILPOINT={fp} {BIN} node admit {}",
                    sh(&file)
                ),
            )?;
            self.check(&format!("{fp}_killed"), !killed.status.success())?;
            let query = format!("SELECT committed FROM batches WHERE id='{}'", m.id()?);
            let status = self.c.remote(
                2,
                &format!("sqlite3 {BASE}/admissions/admissions.sqlite {}", sh(&query)),
            )?;
            self.check(&format!("{fp}_pending_journal"), status.stdout == b"0\n")?;
            self.c.call(2, &["recover".into()])?;
            self.check(
                &format!("{fp}_recovered_and_verified"),
                self.c
                    .exec(
                        2,
                        &format!("{path}/bin/probe; nix store verify --no-trust {path}"),
                    )
                    .is_ok(),
            )?;
        }
        self.c.remote(2, "systemctl stop distributed-nix-daemon")?;
        self.check(
            "clients_require_native_daemon",
            self.c
                .exec(
                    2,
                    &format!("nix path-info {}", self.runtime["bash"].as_str().unwrap()),
                )
                .is_err(),
        )?;
        self.c.remote(2, &format!("umount {ROOT}{live}"))?;
        self.check(
            "missing_mount_is_reconstructed",
            !self.raw_ok(2, &format!("test -e {ROOT}{live}/bin/probe"))?,
        )?;
        self.c.call(2, &["recover".into()])?;
        self.check(
            "daemon_restarts_after_restoring_mounts",
            self.c.exec(2, &format!("{live}/bin/probe")).is_ok(),
        )?;
        // Journal corruption must prevent daemon startup and leave clients gated.
        let id = live_m.id()?;
        let query = format!("SELECT plan FROM batches WHERE id='{id}'");
        let original = String::from_utf8(
            self.c
                .remote(
                    2,
                    &format!("sqlite3 {BASE}/admissions/admissions.sqlite {}", sh(&query)),
                )?
                .stdout,
        )?;
        let mut corrupted: Value = serde_json::from_str(&original)?;
        corrupted["/etc/passwd"] = json!("local");
        let update = |plan: &str| {
            format!(
                "sqlite3 {BASE}/admissions/admissions.sqlite {}",
                sh(&format!(
                    "UPDATE batches SET plan='{}' WHERE id='{id}'",
                    plan.replace('\'', "''")
                ))
            )
        };
        self.c
            .remote(2, &update(&serde_json::to_string(&corrupted)?))?;
        let failed = self.c.call(2, &["recover".into()]).is_err();
        let gated = self.c.exec(2, "true").is_err();
        self.c.remote(2, &update(original.trim()))?;
        self.c.call(2, &["recover".into()])?;
        self.check(
            "tampered_journal_blocks_recovery_and_clients",
            failed && gated,
        )?;
        // Local registration progresses while the origin owns SQLite's write lock.
        let sql = format!(
            "BEGIN IMMEDIATE;\n.shell touch {BASE}/writer-held\n.shell sleep 6\nROLLBACK;\n.shell rm {BASE}/writer-held\n"
        );
        self.c.put(0, &format!("{BASE}/hold.sql"), sql.as_bytes())?;
        self.c.remote(
            0,
            &format!(
                "systemd-run --setenv=PATH=/run/current-system/sw/bin --collect --unit={}-sqlite bash -c {}",
                self.tag,
                sh(&format!(
                    "sqlite3 {ORIGIN}/nix/var/nix/db/db.sqlite < {BASE}/hold.sql"
                ))
            ),
        )?;
        self.wait(0, &format!("test -e {BASE}/writer-held"), 10)?;
        let start = Instant::now();
        self.c.put(
            2,
            &format!("{ROOT}/work/{}-write", self.tag),
            self.tag.as_bytes(),
        )?;
        self.c
            .exec(2, &format!("nix-store --add /work/{}-write", self.tag))?;
        self.check(
            "local_write_does_not_wait_for_origin_sqlite",
            self.raw_ok(0, &format!("test -e {BASE}/writer-held"))?,
        )?;
        self.record("local_write_seconds", json!(start.elapsed().as_secs_f64()))?;
        self.wait(0, &format!("test ! -e {BASE}/writer-held"), 10)?;
        self.file_shapes_and_origin_crash()?;
        self.sparse()?;
        let outputs = self.concurrent_builds()?;
        self.offline_and_reboot(&live, &outputs[2])?;
        self.conflicting_builds()?;
        for n in 0..3 {
            self.c.exec(
                n,
                &format!("{live}/bin/probe; nix store verify --no-trust {live}"),
            )?;
            let result=String::from_utf8(self.c.remote(n,&format!("sqlite3 {ROOT}/nix/var/nix/db/db.sqlite 'PRAGMA integrity_check; PRAGMA foreign_key_check;'"))?.stdout)?;
            self.check(
                &format!("native_database_integrity_node_{n}"),
                result.trim() == "ok",
            )?;
        }
        self.record("complete", json!(true))
    }
    fn file_shapes_and_origin_crash(&mut self) -> Result<()> {
        let core = self.runtime["coreutils"].as_str().unwrap().to_string();
        let bash = self.runtime["bash"].as_str().unwrap().to_string();
        let large_name = format!("{}-large-file", self.tag);
        let large = self.build_expr(
            1,
            &large_name,
            &self.expr(
                &large_name,
                &format!("{core}/bin/head -c 131072 /dev/urandom > \"$out\""),
                "",
            ),
        )?;
        let link_name = format!("{}-symlink", self.tag);
        let symlink = self.build_expr(
            1,
            &link_name,
            &self.expr(
                &link_name,
                &format!("{core}/bin/ln -s {bash}/bin/bash \"$out\""),
                "",
            ),
        )?;
        self.c.publish(1, &[large.clone(), symlink.clone()])?;
        self.check(
            "large_regular_file_bind_mounted",
            self.raw_ok(2, &format!("mountpoint -q {ROOT}{large}"))?,
        )?;
        self.check(
            "large_regular_file_read_only",
            !self.raw_ok(2, &format!("echo bad >> {ROOT}{large}"))?,
        )?;
        self.check("store_symlink_preserved_and_executable",self.c.exec(2,&format!("test -L {symlink}; {symlink} -c true; nix store verify --no-trust {large} {symlink}")).is_ok())?;
        // Create an origin-local path, kill the initial commit after its durable marker,
        // and complete fanout using only the committed origin manifest.
        let file = format!("{BASE}/{}-origin-crash", self.tag);
        self.c.put(0, &file, self.tag.as_bytes())?;
        let path = String::from_utf8(
            self.c
                .remote(0, &format!("nix-store --store {ORIGIN} --add {file}"))?
                .stdout,
        )?
        .trim()
        .to_string();
        let m = self.c.dump(0, std::slice::from_ref(&path), true)?;
        let incoming = self.c.receive(0, &m)?;
        let killed = self.c.remote_unchecked(
            0,
            &format!(
                "env DISTRIBUTED_NIX_FAILPOINT=after-origin-commit {BIN} node commit {}",
                sh(&incoming)
            ),
        )?;
        self.check(
            "origin_commit_killed_after_durable_marker",
            !killed.status.success(),
        )?;
        self.c.reconcile(&m.id()?)?;
        self.check(
            "origin_commit_reconciles_without_source_worker",
            self.c
                .exec(
                    2,
                    &format!("nix store verify --no-trust {path}; test -f {path}"),
                )
                .is_ok(),
        )?;
        // Recovery drains the native daemon connection, without a shell lease.
        let name = format!("{}-recovery-build", self.tag);
        let core = self.runtime["coreutils"].as_str().unwrap();
        let expr = self.expr(
            &name,
            &format!("{core}/bin/sleep 4; echo done > \"$out\""),
            "",
        );
        std::thread::scope(|s| -> Result<()> {
            let active = s.spawn(|| self.build_expr(2, &name, &expr));
            self.wait(2, "pgrep -f '/bin/[s]leep 4' >/dev/null", 10)?;
            let before = Instant::now();
            self.c.call(2, &["recover".into()])?;
            ensure!(
                before.elapsed() >= Duration::from_secs(3),
                "recovery bypassed native build"
            );
            active.join().unwrap()?;
            Ok(())
        })?;
        self.check("recovery_waits_for_active_native_build", true)
    }

    fn sparse(&mut self) -> Result<()> {
        let source = format!("{BASE}/{}-large-source", self.tag);
        self.c.remote(0,&format!("mkdir -p {source}; dd if=/dev/urandom of={source}/payload bs=1M count=64 status=none"))?;
        let expected = String::from_utf8(
            self.c
                .remote(
                    0,
                    &format!(
                        "dd if={source}/payload bs=4096 skip=9472 count=1 status=none | sha256sum"
                    ),
                )?
                .stdout,
        )?
        .split_whitespace()
        .next()
        .unwrap()
        .to_string();
        let path = String::from_utf8(
            self.c
                .remote(0, &format!("nix-store --store {ORIGIN} --add {source}"))?
                .stdout,
        )?
        .trim()
        .to_string();
        let m = self.c.dump(0, std::slice::from_ref(&path), true)?;
        self.accepted(&m)?;
        self.c.admit(2, &m)?;
        let name = format!("{}-sparse", self.tag);
        let body = format!(
            "{}/bin/dd if=\"$src/payload\" of=\"$out\" bs=4096 skip=9472 count=1 status=none",
            self.runtime["coreutils"].as_str().unwrap()
        );
        let expr = self.expr(
            &name,
            &body,
            &format!("src=builtins.storePath {};", json!(path)),
        );
        let rx = |c: &Cluster| -> Result<u64> {
            Ok(String::from_utf8(
                c.remote(2, "cat /sys/class/net/eth0/statistics/rx_bytes")?
                    .stdout,
            )?
            .trim()
            .parse()?)
        };
        let before = rx(&self.c)?;
        let start = Instant::now();
        let output = self.build_expr(2, &name, &expr)?;
        let seconds = start.elapsed().as_secs_f64();
        std::thread::sleep(Duration::from_secs(1));
        let received = rx(&self.c)? - before;
        let actual = String::from_utf8(self.c.exec(2, &format!("sha256sum {output}"))?.stdout)?
            .split_whitespace()
            .next()
            .unwrap()
            .to_string();
        self.check("sparse_source_native_build_correct", actual == expected)?;
        self.check(
            "sparse_source_not_fully_downloaded",
            received < 8 * 1024 * 1024,
        )?;
        self.record("sparse",json!({"source":path,"file_bytes":67108864,"read_bytes":4096,"received_bytes":received,"seconds":seconds,"output":output}))
    }
    fn concurrent_builds(&mut self) -> Result<Vec<String>> {
        for n in 0..3 {
            self.c.remote(
                n,
                &format!("cp -r {ROOT}/work/fixture {ROOT}/work/{}-compile", self.tag),
            )?;
            self.c.put(
                n,
                &format!("{ROOT}/work/{}-compile/nonce", self.tag),
                format!("{}-node{n}\n", self.tag).as_bytes(),
            )?;
        }
        let start = Instant::now();
        let cmd = format!(
            "cd /work/{}-compile; nix build --impure --no-write-lock-file --no-link --print-out-paths",
            self.tag
        );
        let rows = self.c.parallel(|n| {
            let barrier = std::sync::Barrier::new(8);
            std::thread::scope(|s| -> Result<Vec<String>> {
                let jobs: Vec<_> = (0..8)
                    .map(|_| {
                        s.spawn(|| {
                            barrier.wait();
                            Ok(String::from_utf8(self.c.exec(n, &cmd)?.stdout)?
                                .trim()
                                .to_string())
                        })
                    })
                    .collect();
                jobs.into_iter().map(|j| j.join().unwrap()).collect()
            })
        })?;
        self.check(
            "eight_callers_per_node_share_one_output",
            rows.iter().all(|r| r.iter().all(|p| p == &r[0])),
        )?;
        let outputs: Vec<_> = rows.iter().map(|r| r[0].clone()).collect();
        self.check(
            "builds_are_distinct_across_three_nodes",
            outputs[0] != outputs[1] && outputs[1] != outputs[2] && outputs[0] != outputs[2],
        )?;
        self.record(
            "concurrent_builds",
            json!({"callers_per_node":8,"seconds":start.elapsed().as_secs_f64(),"outputs":outputs}),
        )?;
        let publications = self
            .c
            .parallel(|n| self.c.publish(n, &[outputs[n].clone()]))?;
        self.record("concurrent_publications", json!(publications))?;
        for n in 0..3 {
            for (index, path) in outputs.iter().enumerate() {
                self.c.exec(
                    n,
                    &format!("{path}/bin/cibox-workload; nix store verify --no-trust {path}"),
                )?;
                self.check(
                    &format!("compiled_output_{index}_verified_on_node_{n}"),
                    true,
                )?;
            }
            let m = self.c.dump(n, &[outputs[n].clone()], false)?;
            let drv = m.paths[&outputs[n]]["deriver"]
                .as_str()
                .context("deriver")?;
            let audit = String::from_utf8(
                self.c
                    .remote(n, &format!("cat {BASE}/audit/build-audit.log"))?
                    .stdout,
            )?;
            self.check(
                &format!("exactly_one_compilation_node_{n}"),
                audit.lines().filter(|l| l.contains(drv)).count() == 1,
            )?;
        }
        self.c.parallel(|n|self.c.exec(n,"cd /work/fixture; nix develop --impure --no-write-lock-file -c bash -c 'test \"$CIBOX_READY\" = 1; gcc --version'"))?;
        self.check("native_development_shells_all_nodes", true)?;
        Ok(outputs)
    }
    fn offline_and_reboot(&mut self, live: &str, local: &str) -> Result<()> {
        let path = self.tiny(1, "offline")?;
        let m = self.c.dump(1, std::slice::from_ref(&path), false)?;
        let id = m.id()?;
        let oldboot = String::from_utf8(
            self.c
                .remote(2, "cat /proc/sys/kernel/random/boot_id")?
                .stdout,
        )?;
        // This tests offline publication/recovery, not loss of unsynced native
        // Nix imports when the host terminates QEMU. Checkpoint guest storage.
        self.c.remote(2, &format!("sync -f {ROOT}"))?;
        run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["stop", "2"]))?;
        let publish = self.c.publish(1, std::slice::from_ref(&path));
        // Restart even if the assertion fails, avoiding a stranded test VM.
        let started = run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["start", "2"]));
        self.check(
            "offline_node_reports_partial_publication",
            publish
                .as_ref()
                .err()
                .is_some_and(|e| e.to_string().contains("origin committed batch")),
        )?;
        self.check(
            "origin_retains_committed_manifest_for_reconciliation",
            self.raw_ok(
                0,
                &format!("test -e {ORIGIN}/.distributed-nix-publications/{id}.json"),
            )?,
        )?;
        started?;
        self.wait(
            2,
            "systemctl is-active --quiet distributed-nix-recover",
            180,
        )?;
        self.check(
            "automatic_boot_recovery_changed_boot_id",
            String::from_utf8(
                self.c
                    .remote(2, "cat /proc/sys/kernel/random/boot_id")?
                    .stdout,
            )? != oldboot,
        )?;
        self.c.reconcile(&id)?;
        self.check(
            "native_development_shell_survives_offline_restart",
            self.c
                .exec(
                    2,
                    "cd /work/fixture; nix develop --impure --no-write-lock-file -c true",
                )
                .is_ok(),
        )?;
        self.check(
            "offline_node_catches_up_from_origin",
            self.c
                .exec(
                    2,
                    &format!("{path}/bin/probe; nix store verify --no-trust {path}"),
                )
                .is_ok(),
        )?;
        self.check(
            "shared_package_survives_boot",
            self.c.exec(2, &format!("{live}/bin/probe")).is_ok(),
        )?;
        self.check(
            "local_build_survives_boot",
            self.c
                .exec(2, &format!("{local}/bin/cibox-workload"))
                .is_ok(),
        )?;
        // A clean reboot independently exercises persistent recovery units.
        let old = String::from_utf8(
            self.c
                .remote(2, "cat /proc/sys/kernel/random/boot_id")?
                .stdout,
        )?;
        let start = Instant::now();
        self.c.remote_unchecked(2, "systemctl reboot")?;
        let deadline = Instant::now() + Duration::from_secs(180);
        loop {
            let o = self
                .c
                .remote_unchecked(2, "cat /proc/sys/kernel/random/boot_id")?;
            if o.status.success()
                && String::from_utf8_lossy(&o.stdout) != old
                && self.raw_ok(2, "systemctl is-active --quiet distributed-nix-recover")?
            {
                break;
            }
            ensure!(Instant::now() < deadline, "clean reboot timeout");
            std::thread::sleep(Duration::from_secs(1));
        }
        self.check(
            "clean_reboot_recovers_without_manual_commands",
            self.c
                .exec(2, &format!("{path}/bin/probe; {local}/bin/cibox-workload"))
                .is_ok(),
        )?;
        self.record("clean_reboot_seconds", json!(start.elapsed().as_secs_f64()))
    }
    fn conflicting_builds(&mut self) -> Result<()> {
        let name = format!("{}-nondeterministic", self.tag);
        let core = self.runtime["coreutils"].as_str().unwrap();
        let body = format!(
            "{core}/bin/mkdir -p \"$out\"; {core}/bin/head -c 64 /dev/urandom > \"$out/token\""
        );
        let expr = self.expr(&name, &body, "");
        let a = self.build_expr(0, &name, &expr)?;
        let b = self.build_expr(1, &name, &expr)?;
        self.check("nondeterministic_builds_same_store_path", a == b)?;
        let ma = self.c.dump(0, std::slice::from_ref(&a), false)?;
        let mb = self.c.dump(1, std::slice::from_ref(&b), false)?;
        self.check(
            "nondeterministic_builds_different_nar_hashes",
            ma.paths[&a]["narHash"] != mb.paths[&b]["narHash"],
        )?;
        self.c.publish_to(0, std::slice::from_ref(&a), &[0, 2])?;
        let error = self
            .c
            .publish_to(1, std::slice::from_ref(&b), &[1])
            .unwrap_err()
            .to_string();
        self.check(
            "conflicting_publication_rejected_before_copy",
            error.contains("conflicting already-registered"),
        )?;
        self.check(
            "accepted_origin_identity_unchanged",
            self.c.dump(0, std::slice::from_ref(&a), true)?.paths[&a]["narHash"]
                == ma.paths[&a]["narHash"],
        )?;
        let basename = a.rsplit('/').next().unwrap();
        self.c.remote(
            1,
            &format!("rm {ROOT}/nix/var/nix/gcroots/distributed-nix/{basename}"),
        )?;
        self.c.call(1, &["acknowledge".into(), a.clone()])?;
        self.c
            .exec(1, &format!("nix-store --store local --delete {a}"))?;
        self.c.admit(1, &ma)?;
        self.check(
            "conflicting_test_worker_reconciled",
            self.c.dump(1, std::slice::from_ref(&a), false)?.paths[&a]["narHash"]
                == ma.paths[&a]["narHash"],
        )
    }
}
#[test]
#[ignore = "privileged three-VM lab: builds, stops/restarts node2, and reboots it"]
fn three_node_native_nix_end_to_end() -> Result<()> {
    let mut s = Suite::new()?;
    s.c.remote(0, "systemctl stop distributed-nix-publisher")?;
    let result = s.run();
    s.c.remote(0, "systemctl start distributed-nix-publisher")?;
    if let Err(e) = &result {
        s.report["error"] = json!(format!("{e:#}"));
        durable(&s.evidence, &s.report)?;
    }
    fs::create_dir_all(s.c.repo.join("results/distributed-nix"))?;
    fs::copy(
        &s.evidence,
        s.c.repo
            .join(format!("results/distributed-nix/{}.json", s.tag)),
    )?;
    result
}
