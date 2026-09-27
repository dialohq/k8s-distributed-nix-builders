//! Standard Nix socket, native worker protocol, and durable automatic publication.
use crate::{
    cluster::Cluster,
    manifest::{Manifest, realisation_path, valid_path},
    node::{BASE, BIN, Node},
    util::*,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{BufReader, Read},
    net::Shutdown,
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::{
            fs::{PermissionsExt, symlink},
            net::{UnixListener, UnixStream},
            process::CommandExt,
        },
    },
    path::Path,
    process::{Command, Stdio},
    sync::atomic::{AtomicI32, Ordering},
    time::Duration,
};
static CONNECTION_GATE: AtomicI32 = AtomicI32::new(-1);

fn ca_key(id: &str) -> String {
    format!("{:x}", Sha256::digest(id.as_bytes()))
}
fn enqueue_realisation(base: &Path, root: &Path, value: &Value) -> Result<()> {
    let id = value["id"].as_str().context("realisation ID")?;
    let path = realisation_path(&value["outPath"])?;
    let key = ca_key(id);
    durable(&base.join("ca-outbox").join(format!("{key}.json")), value)?;
    let roots = root.join("nix/var/nix/gcroots/distributed-nix-ca-outbox");
    fs::create_dir_all(&roots)?;
    let link = roots.join(key);
    match symlink(&path, &link) {
        Ok(()) => (),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => (),
        Err(e) => return Err(e.into()),
    }
    syncdir(&roots)?;
    Ok(())
}

fn enqueue(paths: &[String]) -> Result<Value> {
    let base = Path::new("/run/distributed-nix");
    let roots = Path::new("/nix/var/nix/gcroots/distributed-nix-outbox");
    fs::create_dir_all(roots)?;
    for p in paths {
        ensure!(valid_path(p), "invalid registration event");
        let name = p.rsplit('/').next().unwrap();
        durable(
            &base.join("outbox").join(format!("{name}.json")),
            &json!({"path":p}),
        )?;
        let link = roots.join(name);
        match symlink(p, &link) {
            Ok(()) => (),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure!(
                    fs::read_link(link)? == Path::new(p),
                    "conflicting outbox root"
                );
            }
            Err(e) => return Err(e.into()),
        }
    }
    syncdir(roots)?;
    failpoint("outbox-before-register");
    Ok(Value::Null)
}
fn collect(request: &Value) -> Result<Value> {
    let fd = CONNECTION_GATE.load(Ordering::Relaxed);
    ensure!(fd >= 0, "missing native connection lock");
    // This callback runs synchronously in the native GC request's main thread.
    // The request itself must not prevent the coordinator from draining clients.
    ensure!(
        unsafe { libc::flock(fd, libc::LOCK_UN) } == 0,
        "release GC request lock"
    );
    let result = (|| -> Result<Value> {
        let mut socket = UnixStream::connect("/run/distributed-nix/control.sock")?;
        serde_json::to_writer(&mut socket, request)?;
        socket.shutdown(Shutdown::Write)?;
        let result: Value = serde_json::from_reader(BufReader::new(&mut socket))?;
        if let Some(e) = result.get("error") {
            bail!("cluster GC: {e}");
        }
        Ok(result)
    })();
    ensure!(
        unsafe { libc::flock(fd, libc::LOCK_SH) } == 0,
        "restore connection lock"
    );
    // Preserve the coordinator's diagnostic (e.g. a busy mount) rather than
    // replacing it with a generic paused-state error during failure recovery.
    let result = result?;
    ensure!(
        !Path::new("/run/distributed-nix/gc-active.json").exists(),
        "GC remains paused"
    );
    Ok(result)
}

#[repr(C)]
pub struct CallbackBuffer {
    data: *mut u8,
    len: usize,
}
/// malloc-owned output, caught panics and errors; C++ frees with the matching ABI.
/// # Safety
/// Input must reference `len` readable bytes and output a writable empty buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn distributed_nix_runtime_v1(
    op: u32,
    input: *const u8,
    len: usize,
    output: *mut CallbackBuffer,
) -> i32 {
    if input.is_null() || output.is_null() || len > isize::MAX as usize {
        return 2;
    }
    let result = std::panic::catch_unwind(|| -> Result<Value> {
        let request: Value =
            serde_json::from_slice(unsafe { std::slice::from_raw_parts(input, len) })?;
        match op {
            1 => enqueue(&serde_json::from_value::<Vec<String>>(request)?),
            2 => collect(&request),
            3 => {
                failpoint("outbox-after-register");
                Ok(Value::Null)
            }
            4 => {
                enqueue_realisation(Path::new("/run/distributed-nix"), Path::new("/"), &request)?;
                Ok(Value::Null)
            }
            _ => bail!("unknown runtime callback"),
        }
    })
    .unwrap_or_else(|_| Err(anyhow::anyhow!("Rust runtime callback panicked")));
    let (status, bytes) = match result {
        Ok(v) => (0, v.to_string().into_bytes()),
        Err(e) => {
            eprintln!("distributed-nix runtime: {e:#}");
            (1, format!("{e:#}").into_bytes())
        }
    };
    unsafe {
        *output = CallbackBuffer {
            data: std::ptr::null_mut(),
            len: 0,
        };
    }
    let data = unsafe { libc::malloc(bytes.len()) }.cast::<u8>();
    if data.is_null() {
        return 2;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data, bytes.len());
        *output = CallbackBuffer {
            data,
            len: bytes.len(),
        };
    }
    status
}

pub fn native_connection(root: &str, gate: i32, trusted: bool, runner: bool) -> Result<()> {
    CONNECTION_GATE.store(gate, Ordering::Relaxed);
    if runner {
        for file in ["/proc", "/etc/resolv.conf"] {
            run(Command::new("mount").args(["--bind", file, &format!("{root}{file}")]))?;
        }
    }
    crate::node::enter_root(root)?;
    crate::native::serve(trusted)
}

impl Node {
    pub fn ca_outbox(&self) -> Result<Value> {
        let pending = crate::node::journals(&self.base.join("ca-outbox"))?
            .into_iter()
            .map(|p| read_json(&p))
            .collect::<Result<Vec<_>>>()?;
        crate::native::dump_realisations(&self.root, &pending)
    }
    pub fn ca_acknowledge(&self, ids: &[String]) -> Result<Value> {
        for id in ids {
            let key = ca_key(id);
            for file in [
                self.base.join("ca-outbox").join(format!("{key}.json")),
                self.root
                    .join("nix/var/nix/gcroots/distributed-nix-ca-outbox")
                    .join(&key),
            ] {
                if file.symlink_metadata().is_ok() {
                    fs::remove_file(&file)?;
                    syncdir(file.parent().unwrap())?;
                }
            }
        }
        Ok(json!({"acknowledged":ids.len()}))
    }
    pub fn ca_backfill(&self) -> Result<Value> {
        let records = crate::native::scan_realisations(&self.root)?;
        for record in &records {
            enqueue_realisation(&self.base, &self.root, record)?;
        }
        Ok(json!({"enqueued":records.len()}))
    }
    pub fn connection(&self, trusted: bool, runner: bool) -> Result<Value> {
        let gate = Lock::acquire(&self.base.join("maintenance.lock"), true)?;
        ensure!(
            !self.gc_active().exists(),
            "cluster GC is paused; administrator must resume GC"
        );
        ensure!(
            self.base.join("ready").exists(),
            "store recovery incomplete"
        );
        let fd = gate.inherited_fd()?;
        let runtime = self.runtime()?;
        let path = runtime["runtime"]
            .as_array()
            .context("runtime")?
            .iter()
            .map(|p| format!("{}/bin", p.as_str().unwrap()))
            .collect::<Vec<_>>()
            .join(":");
        // The connection inherits its host or runner container's PID namespace.
        // Host GC runs only after runner pods have drained.
        Err(Command::new("unshare")
            .args(["--mount", "--propagation", "slave"])
            .arg(std::env::current_exe()?)
            .arg("native-daemon")
            .arg(&self.root)
            .arg(fd.to_string())
            .arg(if trusted { "trusted" } else { "untrusted" })
            .arg(if runner { "runner" } else { "host" })
            .env("PATH", path)
            .env("HOME", "/root")
            .exec()
            .into())
    }
    pub fn serve(&self, runner: bool) -> Result<Value> {
        let socket = if runner {
            Path::new("/run/distributed-nix-runner/socket").to_path_buf()
        } else {
            self.root.join("nix/var/nix/daemon-socket/socket")
        };
        let control = self.base.join("control.sock");
        let sockets = if runner {
            vec![&socket]
        } else {
            vec![&socket, &control]
        };
        for p in sockets {
            if p.exists() {
                fs::remove_file(p)?;
            }
        }
        let listener = UnixListener::bind(&socket)?;
        fs::set_permissions(
            &socket,
            fs::Permissions::from_mode(if runner { 0o666 } else { 0o600 }),
        )?;
        if !runner {
            durable(&self.base.join("native-daemon-v2.json"), &json!(true))?;
            let commands = UnixListener::bind(&control)?;
            fs::set_permissions(&control, fs::Permissions::from_mode(0o600))?;
            std::thread::spawn(move || {
                for stream in commands.incoming() {
                    let Ok(mut stream) = stream else { break };
                    std::thread::spawn(move || {
                        let result = (|| -> Result<Value> {
                            let mut request = Vec::new();
                            Read::by_ref(&mut stream)
                                .take(1024 * 1024)
                                .read_to_end(&mut request)?;
                            let mut cmd = Command::new(BIN);
                            cmd.arg("service-gc").current_dir(format!("{BASE}/cluster"));
                            let o = input(&mut cmd, &request)?;
                            Ok(serde_json::from_slice(&o.stdout)?)
                        })()
                        .unwrap_or_else(|e| json!({"error":format!("{e:#}")}));
                        let _ = serde_json::to_writer(&mut stream, &result);
                    });
                }
            });
        }
        for stream in listener.incoming() {
            let stream = stream?;
            let mut creds: libc::ucred = unsafe { std::mem::zeroed() };
            let mut len = std::mem::size_of_val(&creds) as libc::socklen_t;
            ensure!(
                unsafe {
                    libc::getsockopt(
                        stream.as_raw_fd(),
                        libc::SOL_SOCKET,
                        libc::SO_PEERCRED,
                        (&mut creds as *mut libc::ucred).cast(),
                        &mut len,
                    )
                } == 0,
                "read peer credentials"
            );
            let read: OwnedFd = stream.try_clone()?.into();
            let write: OwnedFd = stream.into();
            let mut child = Command::new(std::env::current_exe()?)
                .args([
                    "node",
                    "connection",
                    if creds.uid == 0 {
                        "trusted"
                    } else {
                        "untrusted"
                    },
                    if runner { "runner" } else { "host" },
                ])
                .stdin(Stdio::from(read))
                .stdout(Stdio::from(write))
                .spawn()?;
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Ok(Value::Null)
    }
    pub fn outbox(&self) -> Result<Value> {
        let dir = self.base.join("outbox");
        fs::create_dir_all(&dir)?;
        let paths = crate::node::journals(&dir)?
            .into_iter()
            .map(|p| -> Result<String> {
                Ok(read_json(&p)?["path"]
                    .as_str()
                    .context("outbox path")?
                    .into())
            })
            .collect::<Result<Vec<_>>>()?;
        crate::native::valid_paths(&self.root, &paths)
    }
    pub fn acknowledge(&self, paths: &[String]) -> Result<Value> {
        for path in paths {
            ensure!(valid_path(path), "invalid acknowledged path");
            let name = path.rsplit('/').next().unwrap();
            for file in [
                self.base.join("outbox").join(format!("{name}.json")),
                self.root
                    .join("nix/var/nix/gcroots/distributed-nix-outbox")
                    .join(name),
            ] {
                if file.symlink_metadata().is_ok() {
                    fs::remove_file(&file)?;
                    syncdir(file.parent().unwrap())?;
                }
            }
        }
        Ok(json!({"acknowledged":paths.len()}))
    }
    /// Called only while GC has drained registrations. Before that boundary an
    /// invalid path could still be between its outbox write and database commit.
    pub(crate) fn prune_uncommitted_outbox(&self) -> Result<()> {
        let valid: std::collections::BTreeSet<String> = serde_json::from_value(self.outbox()?)?;
        let mut invalid = Vec::new();
        for file in crate::node::journals(&self.base.join("outbox"))? {
            let path = read_json(&file)?["path"]
                .as_str()
                .context("outbox path")?
                .to_string();
            if !valid.contains(&path) {
                invalid.push(path);
            }
        }
        self.acknowledge(&invalid)?;
        let complete = self.ca_outbox()?;
        let ready: std::collections::BTreeSet<String> = if complete.is_null() {
            Default::default()
        } else {
            serde_json::from_value(complete["ready"].clone())?
        };
        let mut invalid = Vec::new();
        for file in crate::node::journals(&self.base.join("ca-outbox"))? {
            let value = read_json(&file)?;
            let id = value["id"].as_str().context("realisation ID")?;
            if !ready.contains(id) {
                invalid.push(id.to_string());
            }
        }
        self.ca_acknowledge(&invalid)?;
        Ok(())
    }
}
impl Cluster {
    pub fn publish_pending(&self) -> Result<Value> {
        let mut rows = Vec::new();
        for n in 0..3 {
            let result = (|| -> Result<Value> {
                let ca = self.call_json(n, &["ca-outbox".into()])?;
                if !ca.is_null() {
                    let m = Manifest::parse(ca["manifest"].clone())?;
                    let publication = self.publish_manifest(n, &m, &[0, 1, 2])?;
                    let ready = serde_json::from_value::<Vec<String>>(ca["ready"].clone())?;
                    for batch in ready.chunks(128) {
                        let mut args = vec!["ca-acknowledge".into()];
                        args.extend_from_slice(batch);
                        self.call(n, &args)?;
                    }
                    rows.push(publication);
                }
                let paths: Vec<String> =
                    serde_json::from_value(self.call_json(n, &["outbox".into()])?)?;
                if paths.is_empty() {
                    return Ok(Value::Null);
                }
                let batch: Vec<_> = paths.into_iter().take(128).collect();
                let publication = self.publish(n, &batch)?;
                let mut args = vec!["acknowledge".into()];
                args.extend(batch);
                self.call(n, &args)?;
                Ok(publication)
            })();
            rows.push(match result {
                Ok(v) => v,
                Err(e) => json!({"node":n,"error":format!("{e:#}")}),
            });
        }
        Ok(json!(rows))
    }
    pub fn publisher(&self) -> Result<Value> {
        loop {
            let report = self.publish_pending()?;
            durable(&self.repo.join("publisher-status.json"), &report)?;
            std::thread::sleep(Duration::from_millis(500));
        }
    }
    pub fn native_gc_request(&self, request: &Value) -> Result<Value> {
        let action = request["action"].as_u64().context("GC action")?;
        ensure!(action <= 3, "unknown GC action");
        ensure!(
            request["max_freed"].as_u64() == Some(u64::MAX),
            "bounded GC is not supported; use nix store gc without --max"
        );
        ensure!(
            action != 3,
            "explicit path deletion is not supported by shared GC; remove roots and run nix store gc"
        );
        let node: usize = fs::read_to_string(self.repo.join("node"))?.trim().parse()?;
        ensure!(node < 3, "invalid node");
        let r = self.gc(action < 2)?;
        let paths = if action == 0 {
            // Return only paths valid in the requesting node's local metadata.
            let keep: Vec<String> = serde_json::from_value(r["plan"]["keep"].clone())?;
            let mut args = vec!["valid-paths".into()];
            args.extend(keep);
            self.call_json(node, &args)?
        } else {
            r["plan"]["workers"][node].clone()
        };
        Ok(json!({"paths":paths,"bytes_freed":r["bytes_freed"].as_u64().unwrap_or(0)}))
    }
}
