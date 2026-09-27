//! Kubernetes pod lifecycle. No shell interpreter or generated scripts.
//! Native Nix operations stay in cancellable child processes; mount/configuration
//! operations use the OS directly. Tini is only the PID 1 orphan reaper.
use crate::{
    node::{BASE, ORIGIN, ROOT},
    online_rpc::Config,
};
use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;
use std::{
    fs::{self, File, OpenOptions},
    os::fd::AsRawFd,
    path::Path,
    process::{ExitStatus, Stdio},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    process::{ChildStdout, Command},
    signal::unix::{SignalKind, signal},
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Store,
    Builder,
}
impl Role {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "store" => Ok(Self::Store),
            "builder" => Ok(Self::Builder),
            _ => bail!("invalid pod role: {value}"),
        }
    }
    fn participant(self, hostname: &str) -> Result<usize> {
        if self == Self::Store {
            return Ok(0);
        }
        let ordinal: usize = hostname
            .rsplit_once('-')
            .context("builder hostname requires an ordinal")?
            .1
            .parse()?;
        ordinal.checked_add(1).context("builder ordinal overflow")
    }
}

#[derive(Deserialize)]
struct Runtime {
    seed: Vec<String>,
}

#[derive(Deserialize)]
struct Participants {
    nodes: Vec<String>,
    token_file: std::path::PathBuf,
}
impl Participants {
    fn assign(self, role: Role, hostname: &str, pod_uid: Option<String>) -> Result<Config> {
        let config = Config {
            nodes: self.nodes,
            token_file: self.token_file,
            index: role.participant(hostname)?,
            pod_uid,
        };
        config.validate()?;
        Ok(config)
    }
}

fn mount(source: &str, target: &str, kind: Option<&str>, flags: libc::c_ulong) -> Result<()> {
    fs::create_dir_all(target)?;
    crate::linux::mount(
        Some(Path::new(source)),
        Path::new(target),
        kind,
        flags,
        None,
    )
}

fn claim(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    // SAFETY: live descriptor. Children inherit ownership until they have exited,
    // including after an abrupt supervisor crash; this is not a lease with expiry.
    ensure!(
        unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
        "persistent volume already has an owner"
    );
    ensure!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETFD, 0) } == 0,
        "inherit PVC ownership"
    );
    Ok(file)
}

#[derive(Default)]
struct KernelNfs {
    mounted: bool,
}
impl KernelNfs {
    fn prepare(&mut self) -> Result<()> {
        mount("nfsd", "/proc/fs/nfsd", Some("nfsd"), 0)?;
        self.mounted = true;
        // A container restart retains the pod network namespace. Stop any kernel
        // threads left by the old container, after acquiring exclusive PVC ownership.
        self.stop()?;
        mount("sunrpc", "/run/rpc_pipefs", Some("rpc_pipefs"), 0)
    }
    fn stop(&self) -> Result<()> {
        if self.mounted && fs::read_to_string("/proc/fs/nfsd/threads")?.trim() != "0" {
            fs::write("/proc/fs/nfsd/threads", "0\n")?;
        }
        Ok(())
    }
}
impl Drop for KernelNfs {
    fn drop(&mut self) {
        if let Err(error) = self.stop() {
            eprintln!("stopping kernel NFS: {error:#}");
        }
    }
}

/// Every child has exactly one owner, which waits for its exit even on cancellation.
struct Processes {
    stop: watch::Sender<bool>,
    tasks: JoinSet<()>,
    failures: mpsc::UnboundedSender<anyhow::Error>,
}
impl Processes {
    fn new(failures: mpsc::UnboundedSender<anyhow::Error>) -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            stop,
            tasks: JoinSet::new(),
            failures,
        }
    }
    fn spawn(
        &mut self,
        name: &str,
        command: &mut Command,
        critical: bool,
    ) -> Result<(oneshot::Receiver<Result<ExitStatus>>, Option<ChildStdout>)> {
        while self.tasks.try_join_next().is_some() {}
        let mut child = command
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .process_group(0)
            .spawn()
            .with_context(|| format!("start {name}"))?;
        let output = child.stdout.take();
        let pid = child.id().context("child PID")? as i32;
        let name = name.to_owned();
        let mut stop = self.stop.subscribe();
        let failures = self.failures.clone();
        let (done, result) = oneshot::channel();
        self.tasks.spawn(async move {
            let outcome = async {
                tokio::select! {
                    biased;
                    status = child.wait() => Ok(status?),
                    _ = stop.changed() => {
                        // The unreaped child owns this process-group ID. Signal the
                        // group so native daemon connections and build children stop too.
                        unsafe { libc::kill(-pid, libc::SIGTERM); }
                        match tokio::time::timeout(Duration::from_secs(20), child.wait()).await {
                            Ok(status) => Ok(status?),
                            Err(_) => {
                                unsafe { libc::kill(-pid, libc::SIGKILL); }
                                Ok(child.wait().await?)
                            }
                        }
                    }
                }
            }
            .await;
            if critical && !*stop.borrow() {
                let _ = failures.send(anyhow::anyhow!(
                    "required process {name} exited: {outcome:?}"
                ));
            }
            let _ = done.send(outcome);
        });
        Ok((result, output))
    }
    async fn status(&mut self, name: &str, command: &mut Command) -> Result<ExitStatus> {
        self.spawn(name, command, false)?
            .0
            .await
            .context("child owner disappeared")?
    }
    async fn run(&mut self, name: &str, command: &mut Command) -> Result<()> {
        let status = self.status(name, command).await?;
        ensure!(status.success(), "{name} failed: {status}");
        Ok(())
    }
    fn daemon(&mut self, name: &str, command: &mut Command) -> Result<()> {
        self.spawn(name, command, true)?;
        Ok(())
    }
    async fn shutdown(&mut self) {
        self.stop.send_replace(true);
        while let Some(result) = self.tasks.join_next().await {
            if let Err(error) = result {
                eprintln!("child owner failed: {error}");
            }
        }
    }
}

fn controller(args: &[&str]) -> Result<Command> {
    let mut command = Command::new(std::env::current_exe()?);
    command.args(args);
    Ok(command)
}

pub fn seed_worker(destination: &str) -> Result<()> {
    ensure!(
        matches!(destination, ROOT | ORIGIN),
        "invalid seed destination"
    );
    let runtime: Runtime = serde_json::from_slice(&fs::read("/etc/distributed-nix/runtime.json")?)?;
    crate::native::copy(Path::new("local"), destination, &runtime.seed)?;
    Ok(())
}

pub fn mount_nfs_worker() -> Result<()> {
    use std::net::ToSocketAddrs;
    let host = std::env::var("DISTRIBUTED_NIX_STORE_HOST")?;
    let address = (host.as_str(), 2049)
        .to_socket_addrs()?
        .next()
        .context("NFS Service has no address")?
        .ip();
    let options = format!(
        "vers=4.1,proto=tcp,port=2049,addr={address},hard,timeo=10,retrans=2,lookupcache=positive,actimeo=600,nosharecache"
    );
    crate::linux::mount(
        Some(Path::new(&format!("{host}:/"))),
        Path::new(&format!("{BASE}/lower")),
        Some("nfs4"),
        libc::MS_RDONLY,
        Some(&options),
    )
}

async fn seed(processes: &mut Processes, destination: &str) -> Result<()> {
    let mut command = controller(&["pod-seed", destination])?;
    command.env(
        "NIX_CONFIG",
        format!(
            "{}\nbuild-users-group =\n",
            std::env::var("NIX_CONFIG").unwrap_or_default()
        ),
    );
    processes
        .run("seed runtime via native Nix", &mut command)
        .await
}

async fn start(role: Role, processes: &mut Processes, nfs: &mut KernelNfs) -> Result<()> {
    for directory in [
        "/data/state",
        "/data/worker",
        "/data/origin",
        "/data/nfs/clients",
        "/run/distributed-nix-runner",
        "/etc/nix",
        "/var/lib/nfs",
    ] {
        fs::create_dir_all(directory)?;
    }
    for (source, target) in [
        ("/data/state", BASE),
        ("/data/worker", ROOT),
        ("/data/origin", ORIGIN),
    ] {
        mount(source, target, None, libc::MS_BIND)?;
    }
    fs::create_dir_all(format!("{BASE}/lower"))?;
    let participants: Participants =
        serde_json::from_slice(&fs::read("/etc/distributed-nix/participants.json")?)?;
    let pod_uid = if role == Role::Store {
        None
    } else {
        Some(std::env::var("DISTRIBUTED_NIX_POD_UID")?)
    };
    let config = participants.assign(role, &std::env::var("HOSTNAME")?, pod_uid)?;
    fs::write(
        "/run/distributed-nix-config.json",
        serde_json::to_vec(&config)?,
    )?;
    let runtime: Runtime = serde_json::from_slice(&fs::read("/etc/distributed-nix/runtime.json")?)?;
    ensure!(!runtime.seed.is_empty(), "empty runtime closure");
    if role == Role::Store {
        seed(processes, ORIGIN).await?;
        processes
            .run(
                "prepare origin",
                &mut controller(&["node", "prepare-origin"])?,
            )
            .await?;
        fs::create_dir_all(format!("{ORIGIN}/.distributed-nix-publications"))?;
        mount(ORIGIN, &format!("{BASE}/lower"), None, libc::MS_BIND)?;
        mount(
            ORIGIN,
            &format!("{BASE}/lower"),
            None,
            libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
        )?;
        nfs.prepare()?;
        processes.daemon(
            "NFS client recovery",
            Command::new("nfsdcld").args([
                "-F",
                "-p",
                "/run/rpc_pipefs",
                "-s",
                "/data/nfs/clients",
            ]),
        )?;
        fs::write(
            "/etc/exports",
            format!("{ORIGIN} *(ro,sync,root_squash,no_subtree_check,fsid=0)\n"),
        )?;
        processes
            .run("export NFS", Command::new("exportfs").arg("-r"))
            .await?;
        processes.daemon(
            "NFS authorization",
            Command::new("rpc.mountd").args([
                "--foreground",
                "--no-nfs-version",
                "2",
                "--no-nfs-version",
                "3",
            ]),
        )?;
        // Linux's documented NFSD control filesystem; no rpc.nfsd wrapper.
        for (file, value) in [
            ("versions", "-3 +4\n"),
            ("nfsv4leasetime", "10\n"),
            ("nfsv4gracetime", "10\n"),
            ("portlist", "tcp 2049\n"),
            ("threads", "8\n"),
        ] {
            fs::write(format!("/proc/fs/nfsd/{file}"), value)
                .with_context(|| format!("configure NFSD {file}"))?;
        }
        ensure!(
            fs::read_to_string("/proc/fs/nfsd/threads")?
                .trim()
                .parse::<u32>()?
                > 0,
            "kernel NFSD did not start"
        );
    } else {
        while !processes
            .status(
                "mount shared collection",
                &mut controller(&["pod-mount-nfs"])?,
            )
            .await?
            .success()
        {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    seed(processes, ROOT).await?;
    processes
        .run(
            "recover private store",
            &mut controller(&["node", "recover"])?,
        )
        .await?;
    mount(
        &format!("{ROOT}/nix/store"),
        "/nix/store",
        None,
        libc::MS_BIND | libc::MS_REC,
    )?;
    mount(
        "/run/distributed-nix-runner",
        &format!("{ROOT}/nix/var/nix/daemon-socket"),
        None,
        libc::MS_BIND,
    )?;
    mount(
        &format!("{ROOT}/nix/var/nix"),
        "/nix/var/nix",
        None,
        libc::MS_BIND | libc::MS_REC,
    )?;
    for path in ["work", "root"] {
        mount(
            &format!("{ROOT}/{path}"),
            &format!("/{path}"),
            None,
            libc::MS_BIND,
        )?;
    }
    for path in ["nix/nix.conf", "passwd", "group"] {
        fs::copy(format!("{ROOT}/etc/{path}"), format!("/etc/{path}"))?;
    }
    if role == Role::Builder {
        let mut command = controller(&["node", "runner-daemon", "--notify-ready"])?;
        command.stdout(Stdio::piped());
        let (_, output) = processes.spawn("native Nix daemon", &mut command, true)?;
        let mut output = output.context("readiness pipe")?;
        let mut ready = [0];
        output
            .read_exact(&mut ready)
            .await
            .context("native daemon readiness acknowledgement")?;
        ensure!(ready == *b"R", "invalid daemon readiness acknowledgement");
    }
    processes.daemon("gRPC server", &mut controller(&["serve"])?)?;
    if role == Role::Store {
        processes.daemon("publisher", &mut controller(&["publisher"])?)?;
    } else {
        while !processes
            .status("bootstrap metadata", &mut controller(&["bootstrap"])?)
            .await?
            .success()
        {
            // Retry only an external RPC failure; elapsed time never grants readiness.
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
    fs::write("/run/distributed-nix-ready", b"")?;
    Ok(())
}

pub fn run() -> Result<()> {
    let role = Role::parse(&std::env::var("DISTRIBUTED_NIX_ROLE")?)?;
    fs::create_dir_all("/data")?;
    let _owner = claim(Path::new("/data/instance.lock"))?;
    // Set before starting any threads; children read this configuration as well.
    unsafe {
        std::env::set_var(
            "DISTRIBUTED_NIX_ONLINE_CONFIG",
            "/run/distributed-nix-config.json",
        );
    }
    tokio::runtime::Runtime::new()?.block_on(async {
        let mut term = signal(SignalKind::terminate())?;
        let mut interrupt = signal(SignalKind::interrupt())?;
        let (failures, mut failure) = mpsc::unbounded_channel();
        let mut processes = Processes::new(failures);
        let mut nfs = KernelNfs::default();
        let operation = async {
            start(role, &mut processes, &mut nfs).await?;
            if role == Role::Store {
                let interval = std::env::var("DISTRIBUTED_NIX_GC_INTERVAL_SECONDS")
                    .unwrap_or_else(|_| "3600".into())
                    .parse::<u64>()?;
                ensure!(interval > 0, "GC interval must be positive");
                loop {
                    let result = processes
                        .status("online GC", &mut controller(&["gc", "--if-needed"])?)
                        .await?;
                    tokio::time::sleep(Duration::from_secs(if result.success() {
                        interval
                    } else {
                        5
                    }))
                    .await;
                }
            }
            std::future::pending::<Result<()>>().await
        };
        let result = tokio::select! {
            result = operation => result,
            error = failure.recv() => Err(error.unwrap_or_else(|| anyhow::anyhow!("process supervisor closed"))),
            _ = term.recv() => Ok(()),
            _ = interrupt.recv() => Ok(()),
        };
        let _ = fs::remove_file("/run/distributed-nix-ready");
        // Keep standard NFS helpers alive until the kernel server has stopped.
        let stopped = nfs.stop();
        processes.shutdown().await;
        result.and(stopped)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn process_fixture() -> Result<()> {
        use std::io::{Read, Write};
        let Ok(path) = std::env::var("DISTRIBUTED_NIX_PROCESS_FIXTURE") else {
            return Ok(());
        };
        let mut socket = std::os::unix::net::UnixStream::connect(path)?;
        socket.write_all(b"R")?;
        let mut release = [0];
        socket.read_exact(&mut release)?;
        ensure!(release == *b"X");
        Ok(())
    }

    fn fixture(path: &Path) -> Result<Command> {
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args(["--exact", "pod::tests::process_fixture", "--nocapture"])
            .env("DISTRIBUTED_NIX_PROCESS_FIXTURE", path);
        Ok(command)
    }

    #[tokio::test]
    async fn required_process_exit_is_reported() -> Result<()> {
        use tokio::{io::AsyncWriteExt, net::UnixListener};
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("ready");
        let listener = UnixListener::bind(&path)?;
        let (events, mut event) = mpsc::unbounded_channel();
        let mut processes = Processes::new(events);
        let (status, _) = processes.spawn("fixture", &mut fixture(&path)?, true)?;
        let (mut socket, _) = listener.accept().await?;
        let mut ready = [0];
        socket.read_exact(&mut ready).await?;
        ensure!(ready == *b"R");
        socket.write_all(b"X").await?;
        assert!(
            event
                .recv()
                .await
                .context("missing failure event")?
                .to_string()
                .contains("fixture")
        );
        assert!(status.await??.success());
        processes.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_reaps_children_after_startup_failure() -> Result<()> {
        use std::os::unix::process::ExitStatusExt;
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("ready");
        let listener = tokio::net::UnixListener::bind(&path)?;
        let (events, _event) = mpsc::unbounded_channel();
        let mut processes = Processes::new(events);
        let (status, _) = processes.spawn("fixture", &mut fixture(&path)?, true)?;
        let (mut socket, _) = listener.accept().await?;
        let mut ready = [0];
        socket.read_exact(&mut ready).await?;
        ensure!(ready == *b"R");
        assert!(
            processes
                .daemon(
                    "missing",
                    &mut Command::new(directory.path().join("absent"))
                )
                .is_err()
        );
        processes.shutdown().await;
        assert_eq!(status.await??.signal(), Some(libc::SIGTERM));
        // EOF acknowledges closure by the child; no elapsed-time assertion.
        assert_eq!(socket.read(&mut ready).await?, 0);
        Ok(())
    }

    #[test]
    fn participant_identity_is_checked() -> Result<()> {
        let participants: Participants = serde_json::from_str(
            r#"{"nodes":["store:9840","builder:9840"],"token_file":"/token"}"#,
        )?;
        let config =
            participants.assign(Role::Builder, "pool-builder-0", Some("pod-uid".into()))?;
        assert_eq!(config.index, 1);
        assert_eq!(config.pod_uid.as_deref(), Some("pod-uid"));
        assert_eq!(Role::Builder.participant("pool-builder-2")?, 3);
        assert_eq!(Role::Store.participant("pool-store-0")?, 0);
        assert!(Role::parse("worker").is_err());
        assert!(Role::Builder.participant("builder").is_err());
        assert!(Role::Builder.participant("builder-x").is_err());
        Ok(())
    }
}
