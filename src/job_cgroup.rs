//! Job processes stay below the current container's cgroup, without resource limits.
use anyhow::{Context, Result, ensure};
use std::{
    fs,
    io::{Read, Seek},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
};

pub fn parent(mountpoint: &Path) -> Result<PathBuf> {
    fs::create_dir_all(mountpoint)?;
    crate::linux::mount(None, mountpoint, Some("cgroup2"), 0, None)?;
    let membership = fs::read_to_string("/proc/self/cgroup")?;
    let relative = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .context("ARC requires cgroup v2")?;
    ensure!(
        relative.starts_with('/') && !relative.split('/').any(|s| s == ".."),
        "invalid cgroup path"
    );
    let parent = mountpoint.join(relative.trim_start_matches('/'));
    ensure!(
        parent.join("cgroup.procs").is_file(),
        "container cgroup unavailable"
    );
    Ok(parent)
}

pub struct Group {
    path: PathBuf,
}
impl Group {
    pub fn new(parent: &Path, id: &str) -> Result<Self> {
        ensure!(
            !id.is_empty()
                && id.len() <= 100
                && id.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'),
            "invalid job ID"
        );
        let path = parent.join(format!("arc-{id}"));
        fs::create_dir(&path)?;
        if !path.join("cgroup.kill").is_file() {
            fs::remove_dir(&path)?;
            anyhow::bail!("ARC requires kernel cgroup.kill support");
        }
        Ok(Self { path })
    }

    pub fn attach(&self, command: &mut tokio::process::Command, unprivileged: bool) -> Result<()> {
        let membership = fs::OpenOptions::new()
            .write(true)
            .open(self.path.join("cgroup.procs"))?;
        // Only async-signal-safe syscalls run between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::write(membership.as_raw_fd(), b"0".as_ptr().cast(), 1) != 1 {
                    return Err(std::io::Error::last_os_error());
                }
                if unprivileged
                    && (libc::setgroups(0, std::ptr::null()) != 0
                        || libc::setgid(1001) != 0
                        || libc::setuid(1001) != 0)
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        fs::write(self.path.join("cgroup.kill"), "1")?;
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut events = fs::File::open(path.join("cgroup.events"))?;
            loop {
                events.rewind()?;
                let mut state = String::new();
                events.read_to_string(&mut state)?;
                if state.lines().any(|line| line == "populated 0") {
                    break;
                }
                let mut descriptor = libc::pollfd {
                    fd: events.as_raw_fd(),
                    events: libc::POLLPRI | libc::POLLERR,
                    revents: 0,
                };
                if unsafe { libc::poll(&mut descriptor, 1, -1) } < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::Interrupted {
                        return Err(error.into());
                    }
                }
            }
            fn remove(path: &Path) -> Result<()> {
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        remove(&entry.path())?;
                    }
                }
                fs::remove_dir(path)?;
                Ok(())
            }
            remove(&path)
        })
        .await??;
        Ok(())
    }
}
impl Drop for Group {
    fn drop(&mut self) {
        let _ = fs::write(self.path.join("cgroup.kill"), "1");
    }
}
