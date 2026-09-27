use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Read, Write},
    os::{
        fd::{AsRawFd, IntoRawFd},
        unix::{ffi::OsStrExt, fs::PermissionsExt},
    },
    path::Path,
    process::{Command, Output, Stdio},
};

pub fn refresh_metadata(path: &Path) -> Result<()> {
    let name = CString::new(path.as_os_str().as_bytes())?;
    // NFS positive dentries need both parent and item attributes refreshed.
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    let result = unsafe {
        libc::statx(
            libc::AT_FDCWD,
            name.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW | libc::AT_STATX_FORCE_SYNC,
            libc::STATX_BASIC_STATS,
            &mut stat,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("refresh metadata for {}", path.display()));
    }
    ensure!(stat.stx_nlink > 0, "unlinked source: {}", path.display());
    Ok(())
}

pub fn output(c: &mut Command) -> Result<Output> {
    let o = c.output().with_context(|| format!("execute {c:?}"))?;
    ensure!(
        o.status.success(),
        "{c:?}: {}\n{}",
        o.status,
        String::from_utf8_lossy(&o.stderr)
    );
    Ok(o)
}
pub fn run(c: &mut Command) -> Result<()> {
    output(c).map(|_| ())
}
pub fn capture(c: &mut Command) -> Result<String> {
    Ok(String::from_utf8(output(c)?.stdout)?)
}
pub fn json(c: &mut Command) -> Result<Value> {
    Ok(serde_json::from_slice(&output(c)?.stdout)?)
}
pub fn sh(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}
pub fn join(args: &[String]) -> String {
    args.iter().map(|s| sh(s)).collect::<Vec<_>>().join(" ")
}
pub fn read_json(p: &Path) -> Result<Value> {
    let bytes = fs::read(p).with_context(|| format!("read {}", p.display()))?;
    serde_json::from_slice(&bytes).context("parse JSON")
}
pub fn syncdir(p: &Path) -> Result<()> {
    File::open(p)?
        .sync_all()
        .with_context(|| format!("sync {}", p.display()))
}
pub fn durable(p: &Path, v: &impl serde::Serialize) -> Result<()> {
    let parent = p.parent().context("missing parent")?;
    fs::create_dir_all(parent)?;
    let mut temp = tempfile::Builder::new()
        .prefix(".tmp-")
        .tempfile_in(parent)?;
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o644))?;
    {
        let mut buffered = BufWriter::new(&mut temp);
        serde_json::to_writer(&mut buffered, v)?;
        buffered.write_all(b"\n")?;
        buffered.flush()?;
    }
    temp.as_file().sync_all()?;
    temp.persist(p).map_err(|error| error.error)?;
    syncdir(parent)?;
    if let Some(grandparent) = parent.parent() {
        syncdir(grandparent)?;
    }
    Ok(())
}
pub struct Lock(File);
impl Lock {
    pub fn inherited_fd(self) -> Result<i32> {
        let fd = self.0.as_raw_fd();
        // SAFETY: this live descriptor intentionally outlives exec in a worker.
        ensure!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, 0) } == 0,
            "inherit lock"
        );
        Ok(self.0.into_raw_fd())
    }
    pub fn acquire(p: &Path, shared: bool) -> Result<Self> {
        fs::create_dir_all(p.parent().context("lock parent")?)?;
        let f = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(p)?;
        loop {
            // SAFETY: live file descriptor; flock does not retain Rust memory.
            if unsafe {
                libc::flock(
                    f.as_raw_fd(),
                    if shared { libc::LOCK_SH } else { libc::LOCK_EX },
                )
            } == 0
            {
                break;
            }
            let e = std::io::Error::last_os_error();
            if e.kind() != std::io::ErrorKind::Interrupted {
                return Err(e.into());
            }
        }
        Ok(Self(f))
    }
    pub fn stale(&self) -> Result<bool> {
        Ok(self.0.metadata()?.len() != 0)
    }
    pub fn inherit(self) -> Result<()> {
        // SAFETY: descriptor is valid; clearing CLOEXEC deliberately holds the lease through exec.
        ensure!(
            unsafe { libc::fcntl(self.0.as_raw_fd(), libc::F_SETFD, 0) } == 0,
            "inherit lease: {}",
            std::io::Error::last_os_error()
        );
        let _ = self.0.into_raw_fd();
        Ok(())
    }
}
pub fn input(c: &mut Command, data: &[u8]) -> Result<Output> {
    let mut child = c
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let mut stdin = child.stdin.take().context("child stdin")?;
    // Drain output while writing so a failing verbose peer cannot deadlock the pipe.
    let o = std::thread::scope(|s| -> Result<Output> {
        let writer = s.spawn(move || stdin.write_all(data));
        let o = child.wait_with_output()?;
        let written = writer
            .join()
            .map_err(|_| anyhow::anyhow!("stdin thread panicked"))?;
        ensure!(
            o.status.success(),
            "{c:?}: {} {}",
            o.status,
            String::from_utf8_lossy(&o.stderr)
        );
        written?;
        Ok(o)
    })?;
    Ok(o)
}
pub fn read_stdin() -> Result<Vec<u8>> {
    let mut b = Vec::new();
    std::io::stdin().read_to_end(&mut b)?;
    Ok(b)
}
pub fn failpoint(name: &str) {
    if std::env::var("DISTRIBUTED_NIX_FAILPOINT").as_deref() == Ok(name) {
        // Real abrupt termination: no destructors, no journal cleanup.
        unsafe {
            libc::kill(libc::getpid(), libc::SIGKILL);
        }
    }
}
pub fn arg(args: &[String], i: usize) -> Result<&str> {
    args.get(i)
        .map(String::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing argument {i}"))
}
pub fn no_extra(args: &[String], n: usize) -> Result<()> {
    if args.len() != n {
        bail!("expected {n} arguments, got {}", args.len());
    }
    Ok(())
}
