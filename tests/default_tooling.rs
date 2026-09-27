//! Client-side tests use stock Nix binaries and chroot only to enter the lab's
//! filesystem. No cibox exec/build/publish command or client lease is involved.
use anyhow::{Result, ensure};
use distributed_nix::{
    cluster::Cluster,
    node::{BASE, ORIGIN, ROOT},
    util::*,
};
use serde_json::{Value, json};
use std::{
    process::Command,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Suite {
    c: Cluster,
    runtime: Value,
    tag: String,
    evidence: Value,
}
impl Suite {
    fn raw(&self, node: usize, cmd: &str) -> Result<std::process::Output> {
        let path = self.runtime["runtime"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| format!("{}/bin", p.as_str().unwrap()))
            .collect::<Vec<_>>()
            .join(":");
        self.c.remote(
            node,
            &format!(
                "chroot {ROOT} {}/bin/env -i HOME=/root USER=root PATH={} {}/bin/bash -c {}",
                self.runtime["coreutils"].as_str().unwrap(),
                sh(&path),
                self.runtime["bash"].as_str().unwrap(),
                sh(cmd)
            ),
        )
    }
    fn text(&self, node: usize, cmd: &str) -> Result<String> {
        Ok(String::from_utf8(self.raw(node, cmd)?.stdout)?
            .trim()
            .into())
    }
    fn check(&mut self, name: &str, pass: bool) -> Result<()> {
        self.evidence["checks"][name] = json!(pass);
        self.save()?;
        ensure!(pass, "FAILED {name}");
        println!("PASS {name}");
        Ok(())
    }
    fn save(&self) -> Result<()> {
        durable(
            &self
                .c
                .repo
                .join("results/distributed-nix/default-tooling.json"),
            &self.evidence,
        )
    }
    fn wait(&self, f: impl Fn() -> Result<bool>) -> Result<()> {
        let end = Instant::now() + Duration::from_secs(180);
        while !f()? {
            ensure!(Instant::now() < end, "wait timed out");
            std::thread::sleep(Duration::from_millis(200));
        }
        Ok(())
    }
    fn shared(&self, path: &str) -> Result<()> {
        self.wait(|| {
            for n in 0..3 {
                if self
                    .raw(n, &format!("nix-store --check-validity {}", sh(path)))
                    .is_err()
                {
                    return Ok(false);
                }
                if !self
                    .c
                    .remote_unchecked(
                        n,
                        &format!(
                            "test ! -e {BASE}/outbox/{}.json",
                            path.rsplit('/').next().unwrap()
                        ),
                    )?
                    .status
                    .success()
                {
                    return Ok(false);
                }
            }
            Ok(self
                .c
                .remote_unchecked(0, &format!("test -e {ORIGIN}{path}"))?
                .status
                .success())
        })
    }
    fn file(&self, suffix: &str, body: &str) -> Result<String> {
        let file = format!("/work/{}-{suffix}.nix", self.tag);
        let expr = format!(
            "builtins.derivation {{ name={}; system=\"x86_64-linux\"; builder=(builtins.storePath {})+\"/bin/bash\"; tools=builtins.storePath {}; args=[\"-c\" {}]; }}",
            json!(format!("{}-{suffix}", self.tag)),
            self.runtime["bash"],
            self.runtime["coreutils"],
            json!(body)
        );
        for n in 0..3 {
            self.c.put(n, &format!("{ROOT}{file}"), expr.as_bytes())?;
        }
        Ok(file)
    }
    fn run(&mut self) -> Result<()> {
        let core = self.runtime["coreutils"].as_str().unwrap().to_string();
        for n in 0..3 {
            self.check(
                &format!("default_socket_ping_{n}"),
                self.raw(n, "nix store ping").is_ok(),
            )?;
        }
        self.check(
            "ordinary_login_shell_uses_default_store",
            self.c
                .remote(
                    1,
                    &format!("chroot {ROOT} /bin/bash --login -c 'nix store ping'"),
                )
                .is_ok(),
        )?;
        let mut paths = Vec::new();
        for n in 0..3 {
            let file = self.file(
                &format!("build-{n}"),
                &format!("{core}/bin/mkdir -p \"$out/bin\"; echo node-{n} > \"$out/data\""),
            )?;
            let path = self.text(n, &format!("nix build --impure --file {file} --out-link /work/{}-result-{n} --print-out-paths", self.tag))?;
            ensure!(path.starts_with("/nix/store/"), "bad build result: {path}");
            self.shared(&path)?;
            for reader in 0..3 {
                self.check(
                    &format!("ordinary_build_{n}_automatically_shared_to_{reader}"),
                    self.text(reader, &format!("cat {path}/data"))? == format!("node-{n}"),
                )?;
            }
            paths.push(path);
        }
        let legacy = self.file("legacy", "echo legacy > \"$out\"")?;
        let old = self.text(
            1,
            &format!("nix-build {legacy} --out-link /work/{}-legacy", self.tag),
        )?;
        self.shared(&old)?;
        self.check(
            "legacy_nix_build_publishes_without_wrapper",
            self.text(2, &format!("cat {old}"))? == "legacy",
        )?;
        // Source imports and nix copy never invoke post-build-hook.
        self.raw(
            2,
            &format!("echo source-{} > /work/{}-source", self.tag, self.tag),
        )?;
        let source = self.text(2, &format!("nix store add-file /work/{}-source", self.tag))?;
        self.shared(&source)?;
        self.check(
            "source_import_automatically_published",
            self.text(0, &format!("cat {source}"))? == format!("source-{}", self.tag),
        )?;
        self.raw(
            1,
            &format!("echo copied-{} > /work/{}-copy", self.tag, self.tag),
        )?;
        let copied = self.text(
            1,
            &format!(
                "nix-store --store /work/{}-staging --add /work/{}-copy",
                self.tag, self.tag
            ),
        )?;
        self.raw(
            1,
            &format!(
                "nix copy --from /work/{}-staging --to daemon {copied}",
                self.tag
            ),
        )?;
        self.shared(&copied)?;
        self.check(
            "native_copy_import_automatically_published",
            self.text(2, &format!("cat {copied}"))? == format!("copied-{}", self.tag),
        )?;
        self.check(
            "ordinary_nix_develop",
            self.raw(
                2,
                "cd /work/fixture && nix develop --impure --no-write-lock-file -c gcc --version",
            )
            .is_ok(),
        )?;
        self.check("ordinary_legacy_nix_shell", self.raw(1,
            "nix-shell --expr 'let p = import /nix/store/4k79ns9drp02wyvcix81c7nnz4hn8psi-source {}; in p.mkShell { packages = [p.gcc]; }' --run 'gcc --version'").is_ok())?;
        self.check(
            "ordinary_nix_shell",
            self.raw(
                1,
                &format!("nix shell {} -c bash -c 'test -n \"$PATH\"'", paths[0]),
            )
            .is_ok(),
        )?;
        // A development shell leaves a path in its environment after its Nix
        // connection closes. GC must see runtime roots, not a wrapper lifetime.
        let runtime_file = self.file(
            "runtime",
            &format!("{core}/bin/mkdir -p \"$out/bin\"; echo runtime-alive > \"$out/data\""),
        )?;
        let live = self.text(0, &format!("nix-build {runtime_file} --no-out-link"))?;
        self.shared(&live)?;
        self.c.remote(0, "systemctl stop distributed-nix-publisher")?;
        let ready = format!("/work/{}-shell-ready", self.tag);
        let finish = format!("/work/{}-shell-finish", self.tag);
        let runtime_result = std::thread::scope(|scope| -> Result<()> {
            let reader = scope.spawn(|| self.raw(2, &format!("nix shell {live} -c bash -c {}", sh(&format!("touch {ready}; while ! test -e {finish}; do sleep 0.1; done; cat {live}/data")))));
            let result = (|| -> Result<()> {
                self.wait(|| {
                    Ok(self
                        .c
                        .remote_unchecked(2, &format!("test -e {ROOT}{ready}"))?
                        .status
                        .success())
                })?;
                self.wait(|| {
                    Ok(self
                        .c
                        .remote_unchecked(2, &format!("flock -n -x {BASE}/maintenance.lock true"))?
                        .status
                        .success())
                })?;
                self.raw(1, "nix store gc")?;
                ensure!(
                    self.text(2, &format!("cat {live}/data"))? == "runtime-alive",
                    "running shell package collected"
                );
                Ok(())
            })();
            self.c.remote(2, &format!("touch {ROOT}{finish}"))?;
            let read = reader.join().unwrap()?;
            ensure!(
                String::from_utf8(read.stdout)?.trim() == "runtime-alive",
                "shell lost package"
            );
            result
        });
        self.c.remote(0, "systemctl start distributed-nix-publisher")?;
        runtime_result?;
        self.check("standard_gc_preserves_shell_after_daemon_disconnect", true)?;
        // Once that runtime root disappears, native GC collects the shared copy.
        self.raw(0, "nix-collect-garbage")?;
        self.check(
            "native_gc_collects_after_shell_exit",
            !self
                .c
                .remote_unchecked(0, &format!("test -e {ORIGIN}{live}"))?
                .status
                .success(),
        )?;
        for (n, path) in paths.iter().enumerate() {
            self.check(
                &format!("ordinary_result_root_kept_{n}"),
                self.raw(n, &format!("cat {path}/data")).is_ok(),
            )?;
        }
        // Recreate exactly the collected store name, without remounting retained
        // packages or asking clients to reconnect to their environment.
        let rebuilt = self.text(0, &format!("nix-build {runtime_file} --no-out-link"))?;
        ensure!(rebuilt == live, "store path changed");
        self.shared(&rebuilt)?;
        self.check(
            "immediate_rebuild_after_gc_has_no_stale_nfs_handle",
            self.text(2, &format!("cat {rebuilt}/data"))? == "runtime-alive",
        )?;
        // Publication survives a service restart with durable work still pending.
        self.c.remote(0, "systemctl stop distributed-nix-publisher")?;
        let retry_file = self.file("retry", "echo retry > \"$out\"")?;
        let retry = self.text(2, &format!("nix-build {retry_file} --no-out-link"))?;
        let pending = self
            .c
            .remote_unchecked(
                2,
                &format!(
                    "test -e {BASE}/outbox/{}.json",
                    retry.rsplit('/').next().unwrap()
                ),
            )?
            .status
            .success();
        self.c.remote(0, "systemctl start distributed-nix-publisher")?;
        self.check("successful_build_has_durable_outbox", pending)?;
        self.shared(&retry)?;
        self.check(
            "publisher_restart_retries_without_user_action",
            self.text(0, &format!("cat {retry}"))? == "retry",
        )?;
        // A successful database commit followed by an abrupt worker death must
        // still publish. The application did not get a successful build reply.
        self.c.remote(0, "systemctl stop distributed-nix-publisher")?;
        self.c.remote(
            2,
            "mkdir -p /etc/systemd/system.control/distributed-nix-daemon.service.d",
        )?;
        self.c.put(
            2,
            "/etc/systemd/system.control/distributed-nix-daemon.service.d/99-test-crash.conf",
            b"[Service]\nEnvironment=DISTRIBUTED_NIX_FAILPOINT=outbox-after-register\n",
        )?;
        self.c.remote(
            2,
            "systemctl daemon-reload; systemctl restart distributed-nix-daemon",
        )?;
        self.raw(2, &format!("echo survived > /work/{}-crash", self.tag))?;
        let interrupted = self
            .raw(2, &format!("nix store add-file /work/{}-crash", self.tag))
            .is_err();
        self.c.remote(2, "rm /etc/systemd/system.control/distributed-nix-daemon.service.d/99-test-crash.conf; systemctl daemon-reload; systemctl restart distributed-nix-daemon")?;
        let pending: Vec<String> =
            serde_json::from_value(self.c.call_json(2, &["outbox".into()])?)?;
        let committed = pending
            .into_iter()
            .find(|p| p.ends_with(&format!("{}-crash", self.tag)));
        self.c.remote(0, "systemctl start distributed-nix-publisher")?;
        self.check(
            "native_worker_crashed_after_database_commit",
            interrupted && committed.is_some(),
        )?;
        let committed = committed.unwrap();
        self.shared(&committed)?;
        self.check(
            "committed_registration_survives_worker_crash",
            self.text(1, &format!("cat {committed}"))? == "survived",
        )?;
        // Independent ordinary clients retain Nix's native build deduplication.
        let parallel_file = self.file(
            "parallel",
            &format!("{core}/bin/sleep 1; echo parallel > \"$out\""),
        )?;
        let simultaneous = std::thread::scope(|scope| -> Result<Vec<String>> {
            let workers: Vec<_> = (0..6)
                .map(|_| {
                    scope
                        .spawn(|| self.text(1, &format!("nix-build {parallel_file} --no-out-link")))
                })
                .collect();
            workers.into_iter().map(|w| w.join().unwrap()).collect()
        })?;
        ensure!(
            simultaneous.iter().all(|p| p == &simultaneous[0]),
            "concurrent results differ"
        );
        self.shared(&simultaneous[0])?;
        let audit = String::from_utf8(
            self.c
                .remote(1, &format!("cat {BASE}/audit/build-audit.log"))?
                .stdout,
        )?;
        self.check(
            "six_plain_clients_compile_once",
            audit
                .lines()
                .filter(|line| line.contains(&format!("{}-parallel.drv", self.tag)))
                .count()
                == 1,
        )?;
        // Freeze must wait for a native client connection, without a CLI lease.
        let slow_file = self.file(
            "slow",
            &format!("{core}/bin/sleep 6; echo complete > \"$out\""),
        )?;
        let start = Instant::now();
        std::thread::scope(|scope| -> Result<()> {
            let build = scope.spawn(|| {
                self.text(
                    2,
                    &format!("nix-build {slow_file} --out-link /work/{}-slow", self.tag),
                )
            });
            self.wait(|| {
                Ok(self
                    .c
                    .remote_unchecked(2, "pgrep -f '/bin/[s]leep 6' >/dev/null")?
                    .status
                    .success())
            })?;
            self.raw(1, "nix store gc")?;
            let result = build.join().unwrap()?;
            ensure!(
                self.text(2, &format!("cat {result}"))? == "complete",
                "GC broke active native build"
            );
            Ok(())
        })?;
        self.check(
            "native_gc_drains_unwrapped_build",
            start.elapsed() >= Duration::from_secs(6),
        )?;
        // Stock GC must fail closed with an unavailable participant, and recovery
        // must not require a user to replay publication manifests.
        self.c.remote(2, &format!("sync -f {ROOT}"))?;
        run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["stop", "2"]))?;
        let failed = self.raw(1, "nix store gc").is_err();
        let offline_file = self.file_on_online("offline", "echo offline > \"$out\"")?;
        let offline_result = self.text(
            1,
            &format!(
                "nix-build {offline_file} --out-link /work/{}-offline",
                self.tag
            ),
        );
        run(Command::new(self.c.repo.join("scripts/vms.sh")).args(["start", "2"]))?;
        self.check("standard_gc_refuses_offline_node", failed)?;
        let offline = offline_result?;
        self.wait(|| Ok(self.raw(2, "nix store ping").is_ok()))?;
        self.shared(&offline)?;
        self.check(
            "offline_peer_catches_up_automatically",
            self.text(2, &format!("cat {offline}"))? == "offline",
        )?;
        for n in 0..3 {
            let integrity = self.c.remote(n, &format!("sqlite3 {ROOT}/nix/var/nix/db/db.sqlite 'PRAGMA integrity_check; PRAGMA foreign_key_check;'"))?;
            self.check(
                &format!("sqlite_integrity_{n}"),
                String::from_utf8(integrity.stdout)?.trim() == "ok",
            )?;
        }
        self.evidence["complete"] = json!(true);
        self.save()
    }
    fn file_on_online(&self, suffix: &str, body: &str) -> Result<String> {
        let file = format!("/work/{}-{suffix}.nix", self.tag);
        let expr = format!(
            "builtins.derivation {{ name={}; system=\"x86_64-linux\"; builder=(builtins.storePath {})+\"/bin/bash\"; args=[\"-c\" {}]; }}",
            json!(format!("{}-{suffix}", self.tag)),
            self.runtime["bash"],
            json!(body)
        );
        self.c.put(1, &format!("{ROOT}{file}"), expr.as_bytes())?;
        Ok(file)
    }
}
#[test]
#[ignore = "three-VM native tooling test; collects garbage and restarts node2"]
fn ordinary_nix_clients_end_to_end() -> Result<()> {
    let c = Cluster::new()?;
    let runtime = read_json(&c.repo.join("manifest.json"))?;
    let tag = format!(
        "native-e2e-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let mut s = Suite {
        c,
        runtime,
        tag: tag.clone(),
        evidence: json!({"tag":tag,"checks":{},"complete":false}),
    };
    let start = Instant::now();
    let result = s.run();
    s.evidence["seconds"] = json!(start.elapsed().as_secs_f64());
    if let Err(e) = &result {
        s.evidence["error"] = json!(format!("{e:#}"));
        s.save()?;
    }
    s.save()?;
    result
}

#[test]
#[ignore = "three-VM test: stock GC must retain a package held only by an open descriptor"]
fn open_descriptor_is_native_gc_root() -> Result<()> {
    let c = Cluster::new()?;
    let runtime = read_json(&c.repo.join("manifest.json"))?;
    let tag = format!(
        "native-fd-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let s = Suite {
        c,
        runtime,
        tag: tag.clone(),
        evidence: Value::Null,
    };
    s.raw(
        0,
        &format!("head -c 131072 /dev/zero > /work/{tag}; echo descriptor-only >> /work/{tag}"),
    )?;
    let path = s.text(0, &format!("nix store add-file /work/{tag}"))?;
    s.shared(&path)?;
    s.c.remote(2, &format!("mountpoint -q {ROOT}{path}"))?;
    s.c.remote(0, "systemctl stop distributed-nix-publisher")?;
    let result = std::thread::scope(|scope| -> Result<()> {
        // `path` appears in argv, never in the process environment. Only fd 9
        // keeps it alive; the shell itself is from the unrelated bootstrap set.
        let reader = scope.spawn(|| s.raw(2, &format!(
            "exec 9<{path}; touch /work/{tag}-ready; while ! test -e /work/{tag}-finish; do sleep 0.1; done; tail -c 16 <&9")));
        let result = (|| -> Result<()> {
            s.wait(|| {
                Ok(s.c
                    .remote_unchecked(2, &format!("test -e {ROOT}/work/{tag}-ready"))?
                    .status
                    .success())
            })?;
            s.c.remote(2, &format!("flock -n -x {BASE}/maintenance.lock true"))?;
            s.raw(0, "nix store gc")?;
            s.raw(
                2,
                &format!("nix-store --check-validity {path}; test -f {path}"),
            )?;
            s.c.remote(0, &format!("test -f {ORIGIN}{path}"))?;
            Ok(())
        })();
        s.c.remote(2, &format!("touch {ROOT}/work/{tag}-finish"))?;
        let output = reader.join().unwrap()?;
        ensure!(
            String::from_utf8(output.stdout)?.trim() == "descriptor-only",
            "reader lost data"
        );
        result
    });
    s.c.remote(0, "systemctl start distributed-nix-publisher")?;
    result?;
    s.raw(0, "nix store gc")?;
    ensure!(
        !s.c.remote_unchecked(0, &format!("test -e {ORIGIN}{path}"))?
            .status
            .success(),
        "closed descriptor still retains source"
    );
    durable(
        &s.c.repo.join("results/distributed-nix/runtime-fd.json"),
        &json!({
            "complete":true, "path":path, "checks":{
                "reader_has_no_controller_lease":true,
            "reader_uses_nfs_bind_mount":true,
                "gc_keeps_path_held_only_by_descriptor":true,
                "reader_retains_contents":true,
                "closing_descriptor_allows_collection":true
            }
        }),
    )
}
