//! Linux mount operations, without command-line utilities.
use anyhow::{Context, Result, ensure};
use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

pub fn mount(
    source: Option<&Path>,
    target: &Path,
    kind: Option<&str>,
    flags: libc::c_ulong,
    data: Option<&str>,
) -> Result<()> {
    let source = source
        .map(|p| CString::new(p.as_os_str().as_bytes()))
        .transpose()?;
    let target_c = CString::new(target.as_os_str().as_bytes())?;
    let kind = kind.map(CString::new).transpose()?;
    let data = data.map(CString::new).transpose()?;
    // SAFETY: all pointers remain valid for the synchronous syscall.
    let result = unsafe {
        libc::mount(
            source.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()),
            target_c.as_ptr(),
            kind.as_ref().map_or(std::ptr::null(), |v| v.as_ptr()),
            flags,
            data.as_ref()
                .map_or(std::ptr::null(), |v| v.as_ptr().cast()),
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("mount {}", target.display()));
    }
    Ok(())
}
pub fn bind(source: &Path, target: &Path) -> Result<()> {
    mount(Some(source), target, None, libc::MS_BIND, None)
}
pub fn unmount(target: &Path) -> Result<()> {
    let name = CString::new(target.as_os_str().as_bytes())?;
    // SAFETY: live NUL-terminated path; no lazy unmount hides busy users.
    if unsafe { libc::umount2(name.as_ptr(), 0) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("unmount {}", target.display()));
    }
    Ok(())
}
fn mount_id(path: &Path) -> Result<u64> {
    let name = CString::new(path.as_os_str().as_bytes())?;
    let mut stat: libc::statx = unsafe { std::mem::zeroed() };
    // SAFETY: stat is writable and the pathname is valid for the call.
    if unsafe {
        libc::statx(
            libc::AT_FDCWD,
            name.as_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
            libc::STATX_MNT_ID,
            &mut stat,
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("statx {}", path.display()));
    }
    ensure!(
        stat.stx_mask & libc::STATX_MNT_ID != 0,
        "kernel does not support mount IDs"
    );
    Ok(stat.stx_mnt_id)
}
pub fn mountpoint(path: &Path) -> Result<bool> {
    if !path.try_exists()? {
        return Ok(false);
    }
    let path = std::fs::canonicalize(path)?;
    let Some(parent) = path.parent() else {
        return Ok(true);
    };
    Ok(mount_id(&path)? != mount_id(parent)?)
}

pub fn sync_filesystem(path: &Path) -> Result<()> {
    use std::os::fd::AsRawFd;
    let file = std::fs::File::open(path)?;
    // SAFETY: the filesystem descriptor remains open throughout syncfs.
    if unsafe { libc::syncfs(file.as_raw_fd()) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("sync filesystem {}", path.display()));
    }
    Ok(())
}

pub fn private_mount_namespace() -> Result<()> {
    // Called in the dedicated connection process, before starting native Nix.
    if unsafe { libc::unshare(libc::CLONE_NEWNS) } != 0 {
        return Err(std::io::Error::last_os_error()).context("unshare mount namespace");
    }
    mount(
        None,
        Path::new("/"),
        None,
        libc::MS_REC | libc::MS_SLAVE,
        None,
    )
}
