//! Lock owners run in dedicated processes: parallel tests cannot accidentally
//! retain their flock file descriptions between fork and exec.
use anyhow::{Result, ensure};
use distributed_nix::{node::Node, online::group, util::Lock};
use std::{
    collections::BTreeSet,
    fs,
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    process::{Child, Command, Stdio},
};
const LIVE: &str = "/nix/store/22222222222222222222222222222222-live";

struct Owner {
    child: Child,
    socket: UnixStream,
    directory: tempfile::TempDir,
}
impl Owner {
    fn start(kind: &str) -> Result<Self> {
        let directory = tempfile::tempdir()?;
        let listener = UnixListener::bind(directory.path().join("control"))?;
        let child = Command::new(std::env::current_exe()?)
            .args(["--exact", "lease_owner", "--nocapture"])
            .env("DISTRIBUTED_NIX_TEST_OWNER", kind)
            .env("DISTRIBUTED_NIX_TEST_DIRECTORY", directory.path())
            .stdin(Stdio::null())
            .spawn()?;
        let (mut socket, _) = listener.accept()?;
        let mut ready = [0];
        socket.read_exact(&mut ready)?;
        ensure!(ready == *b"R");
        Ok(Self {
            child,
            socket,
            directory,
        })
    }
    fn release(&mut self) -> Result<()> {
        self.socket.write_all(b"X")?;
        let mut released = [0];
        self.socket.read_exact(&mut released)?;
        ensure!(released == *b"D");
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.socket.write_all(b"Q")?;
        ensure!(self.child.wait()?.success());
        Ok(())
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn node(path: &std::path::Path) -> Node {
    Node {
        base: path.into(),
        root: path.join("root"),
        origin: path.join("origin"),
        lower: path.join("lower"),
    }
}

#[test]
fn lease_owner() -> Result<()> {
    let Ok(kind) = std::env::var("DISTRIBUTED_NIX_TEST_OWNER") else {
        return Ok(());
    };
    let directory =
        std::path::PathBuf::from(std::env::var_os("DISTRIBUTED_NIX_TEST_DIRECTORY").unwrap());
    let mut socket = UnixStream::connect(directory.join("control"))?;
    let held: Box<dyn std::any::Any> = match kind.as_str() {
        "lock" => Box::new(Lock::acquire(&directory.join("lock"), false)?),
        "group" => {
            let node = node(&directory);
            let group = group(&node.base, &node.root, Some("pod-uid"))?;
            // The root's target may be absent; liveness is a metadata contract.
            std::os::unix::fs::symlink(
                LIVE,
                node.root
                    .join("nix/var/nix/gcroots/distributed-nix-clients/pod-pod-uid/live"),
            )?;
            Box::new(group)
        }
        _ => anyhow::bail!("unknown owner mode"),
    };
    socket.write_all(b"R")?;
    let mut release = [0];
    socket.read_exact(&mut release)?;
    ensure!(release == *b"X");
    drop(held);
    socket.write_all(b"D")?;
    socket.read_exact(&mut release)?;
    ensure!(release == *b"Q");
    Ok(())
}

#[test]
fn native_flock_release_follows_explicit_drop_acknowledgement() -> Result<()> {
    let mut owner = Owner::start("lock")?;
    let path = owner.directory.path().join("lock");
    let probe = || {
        Command::new("flock")
            .arg("-n")
            .arg(&path)
            .arg("true")
            .status()
    };
    ensure!(probe()?.code() == Some(1));
    owner.release()?;
    ensure!(probe()?.success());
    owner.finish()?;
    let first = Lock::acquire(&path, true)?;
    let second = Lock::acquire(&path, true)?;
    drop((first, second));
    Ok(())
}

#[test]
fn pod_roots_require_both_pod_retirement_and_lease_release() -> Result<()> {
    let mut owner = Owner::start("group")?;
    let node = node(owner.directory.path());
    node.prune_client_roots(&BTreeSet::new())?;
    ensure!(node.client_roots()?.contains(LIVE));
    owner.release()?;
    node.prune_client_roots(&BTreeSet::from(["pod-uid".into()]))?;
    ensure!(node.client_roots()?.contains(LIVE));
    node.prune_client_roots(&BTreeSet::new())?;
    ensure!(node.client_roots()?.is_empty());
    owner.finish()?;
    ensure!(
        fs::read_dir(node.base.join("client-roots"))?
            .next()
            .is_none()
    );
    Ok(())
}
