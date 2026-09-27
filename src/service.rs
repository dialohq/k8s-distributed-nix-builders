//! Standard Nix socket, native worker protocol, and durable automatic publication.
use crate::{
    cluster::Cluster,
    manifest::{Manifest, realisation_path, valid_path},
    node::Node,
    util::*,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    os::{
        fd::{AsRawFd, OwnedFd},
        unix::{
            fs::{PermissionsExt, symlink},
            net::UnixListener,
            process::CommandExt,
        },
    },
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

fn ca_key(id: &str) -> String {
    format!("{:x}", Sha256::digest(id.as_bytes()))
}
fn enqueue_realisation(base: &Path, root: &Path, value: &Value) -> Result<()> {
    let _queue = Lock::acquire(&base.join("outbox.lock"), false)?;
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
    enqueue_paths(base, Path::new("/"), paths, || Ok(()))
}

fn enqueue_paths(
    base: &Path,
    root: &Path,
    paths: &[String],
    before_roots: impl Fn() -> Result<()>,
) -> Result<Value> {
    let _queue = Lock::acquire(&base.join("outbox.lock"), false)?;
    let roots = root.join("nix/var/nix/gcroots/distributed-nix-outbox");
    fs::create_dir_all(&roots)?;
    for p in paths {
        ensure!(valid_path(p), "invalid registration event");
        let name = p.rsplit('/').next().unwrap();
        durable(
            &base.join("outbox").join(format!("{name}.json")),
            &json!({"path":p}),
        )?;
        before_roots()?;
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
    syncdir(&roots)?;
    failpoint("outbox-before-register");
    Ok(Value::Null)
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
            3 => {
                failpoint("outbox-after-register");
                Ok(Value::Null)
            }
            4 => {
                enqueue_realisation(Path::new("/run/distributed-nix"), Path::new("/"), &request)?;
                Ok(Value::Null)
            }
            5 => crate::online::pin_runtime(&serde_json::from_value::<Vec<String>>(request)?),
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

pub fn native_connection(root: &str, trusted: bool) -> Result<()> {
    let group = crate::online::native_group(Path::new(root))?;
    group.activate()?;
    for file in ["/proc", "/etc/resolv.conf"] {
        crate::linux::bind(Path::new(file), Path::new(&format!("{root}{file}")))?;
    }
    crate::node::enter_root(root)?;
    crate::native::serve(trusted)
}

impl Node {
    pub fn ca_outbox(&self) -> Result<Value> {
        let _queue = Lock::acquire(&self.base.join("outbox.lock"), true)?;
        let pending = crate::node::journals(&self.base.join("ca-outbox"))?
            .into_iter()
            .map(|p| read_json(&p))
            .collect::<Result<Vec<_>>>()?;
        crate::native::dump_realisations(&self.root, &pending)
    }
    pub fn ca_acknowledge(&self, ids: &[String]) -> Result<Value> {
        let _queue = Lock::acquire(&self.base.join("outbox.lock"), false)?;
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
    pub fn connection(&self, trusted: bool) -> Result<Value> {
        let gate = Lock::acquire(&self.base.join("maintenance.lock"), true)?;
        ensure!(
            self.base.join("ready").exists(),
            "store recovery incomplete"
        );
        gate.inherit()?;
        let runtime = self.runtime()?;
        let path = runtime["runtime"]
            .as_array()
            .context("runtime")?
            .iter()
            .map(|p| format!("{}/bin", p.as_str().unwrap()))
            .collect::<Vec<_>>()
            .join(":");
        crate::linux::private_mount_namespace()?;
        Err(Command::new(std::env::current_exe()?)
            .arg("native-daemon")
            .arg(&self.root)
            .arg(if trusted { "trusted" } else { "untrusted" })
            .env("PATH", path)
            .env("HOME", "/root")
            .exec()
            .into())
    }
    pub fn serve_with_ready(&self, ready: impl FnOnce() -> Result<()>) -> Result<Value> {
        let _group = crate::online::native_group(&self.root)?;
        let socket = Path::new("/run/distributed-nix-runner/socket");
        if socket.exists() {
            fs::remove_file(socket)?;
        }
        let listener = UnixListener::bind(socket)?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o666))?;
        ready()?;
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
        let _queue = Lock::acquire(&self.base.join("outbox.lock"), true)?;
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
        let _queue = Lock::acquire(&self.base.join("outbox.lock"), false)?;
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
}
fn publish_queued(
    paths: Vec<String>,
    publish: impl FnOnce(&[String]) -> Result<Value>,
    mut acknowledge: impl FnMut(&[String]) -> Result<()>,
) -> Result<Value> {
    if paths.is_empty() {
        return Ok(Value::Null);
    }
    let publication = publish(&paths[..paths.len().min(128)])?;
    let published: std::collections::BTreeSet<String> =
        serde_json::from_value(publication["published_paths"].clone())?;
    // Only acknowledge the queued snapshot after the complete canonical closure
    // has committed and every participant has admitted it.
    let acknowledged: Vec<_> = paths
        .into_iter()
        .filter(|p| published.contains(p))
        .collect();
    for batch in acknowledged.chunks(128) {
        acknowledge(batch)?;
    }
    Ok(publication)
}

impl Cluster {
    fn publish_ca_pending(&self, node: usize) -> Result<Vec<Value>> {
        let ca = self.call_json(node, &["ca-outbox".into()])?;
        if ca.is_null() {
            return Ok(Vec::new());
        }
        let manifest = Manifest::parse(ca["manifest"].clone())?;
        let ready = serde_json::from_value::<Vec<String>>(ca["ready"].clone())?;
        ensure!(!ready.is_empty(), "CA manifest has no ready records");
        let reports = self.parallel(|target| {
            let file = self.receive(target, &manifest)?;
            self.call_json(target, &["realisation-conflicts".into(), file])
        })?;
        let mut conflicts = std::collections::BTreeSet::new();
        for report in &reports {
            for role in ["worker", "origin"] {
                conflicts.extend(serde_json::from_value::<Vec<String>>(report[role].clone())?);
            }
        }
        let blocked = manifest.blocked_realisations(conflicts)?;
        let mut rows = Vec::new();
        if !blocked.is_empty() {
            rows.push(json!({"node":node,"error":"conflicting realisations; queued with roots retained", "blocked":blocked, "conflicts":reports}));
        }
        let ready: Vec<_> = ready
            .into_iter()
            .filter(|id| !blocked.contains(id))
            .collect();
        if !ready.is_empty() {
            let subset = manifest.realisation_closure(&ready)?;
            let publication =
                self.publish_manifest(node, &subset, &(0..self.len()).collect::<Vec<_>>())?;
            for batch in ready.chunks(128) {
                let mut args = vec!["ca-acknowledge".into()];
                args.extend_from_slice(batch);
                self.call_json(node, &args)?;
            }
            rows.push(publication);
        }
        Ok(rows)
    }

    fn publish_paths_pending(&self) -> Result<Vec<Value>> {
        let mut rows = Vec::new();
        for n in 0..self.len() {
            let result = (|| -> Result<Value> {
                let paths: Vec<String> =
                    serde_json::from_value(self.call_json(n, &["outbox".into()])?)?;
                publish_queued(
                    paths,
                    |batch| self.publish(n, batch),
                    |batch| {
                        let mut args = vec!["acknowledge".into()];
                        args.extend_from_slice(batch);
                        self.call_json(n, &args)?;
                        Ok(())
                    },
                )
            })();
            rows.push(match result {
                Ok(v) => v,
                Err(e) => json!({"node":n,"error":format!("{e:#}")}),
            });
        }
        Ok(rows)
    }

    pub fn publish_pending(&self) -> Result<Value> {
        let mut rows = self.publish_paths_pending()?;
        for node in 0..self.len() {
            match self.publish_ca_pending(node) {
                Ok(publications) => rows.extend(publications),
                Err(error) => rows.push(json!({"node":node,"error":format!("{error:#}")})),
            }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_acknowledges_queued_dependencies_only_after_success() -> Result<()> {
        let queued: Vec<_> = (0..300).map(|i| format!("path-{i}")).collect();
        let mut acknowledged = Vec::new();
        publish_queued(
            queued.clone(),
            |roots| {
                ensure!(roots.len() == 128);
                let mut closure = queued[..250].to_vec();
                closure.push("not-in-queue-snapshot".into());
                Ok(json!({"published_paths":closure}))
            },
            |batch| {
                acknowledged.extend_from_slice(batch);
                Ok(())
            },
        )?;
        ensure!(acknowledged == queued[..250]);
        let mut called = false;
        let failed = publish_queued(
            queued.clone(),
            |_| bail!("one participant has not admitted the publication"),
            |_| {
                called = true;
                Ok(())
            },
        );
        ensure!(failed.is_err() && !called);
        let malformed = publish_queued(
            queued,
            |_| Ok(json!({})),
            |_| {
                called = true;
                Ok(())
            },
        );
        ensure!(malformed.is_err() && !called);
        Ok(())
    }

    #[test]
    fn acknowledgement_cannot_interleave_entry_and_root_creation() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let node = Node {
            base: directory.path().join("state"),
            root: directory.path().join("root"),
            ..Node::default()
        };
        let path = "/nix/store/11111111111111111111111111111111-output".to_owned();
        std::thread::scope(|scope| -> Result<()> {
            let (reached, at_entry) = std::sync::mpsc::channel();
            let (resume, resumed) = std::sync::mpsc::channel();
            let producer_node = node.clone();
            let producer_path = path.clone();
            let producer = scope.spawn(move || {
                enqueue_paths(
                    &producer_node.base,
                    &producer_node.root,
                    &[producer_path],
                    || {
                        reached.send(())?;
                        resumed.recv()?;
                        Ok(())
                    },
                )
            });
            at_entry.recv()?;
            // Probe exactly while an entry exists but its root has not been made.
            let lock = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(node.base.join("outbox.lock"))?;
            let blocked = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            let error = std::io::Error::last_os_error();
            // Always release the producer, including when the assertion fails.
            resume.send(())?;
            producer.join().unwrap()?;
            ensure!(
                blocked == -1 && error.kind() == std::io::ErrorKind::WouldBlock,
                "queue mutation was visible without its lock"
            );
            node.acknowledge(&[path.clone()])?;
            ensure!(crate::node::journals(&node.base.join("outbox"))?.is_empty());
            ensure!(
                fs::read_dir(node.root.join("nix/var/nix/gcroots/distributed-nix-outbox"))?
                    .next()
                    .is_none()
            );
            Ok(())
        })
    }
}
