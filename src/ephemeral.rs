use crate::{
    manifest::{Manifest, valid_path},
    native,
    node::{Node, physical},
    util::{durable, refresh_metadata},
};
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::{
    ffi::CString,
    fs,
    os::unix::{ffi::OsStrExt, fs::symlink},
    path::Path,
    time::Instant,
};

fn readonly_bind(source: &Path, target: &Path) -> Result<()> {
    let source = CString::new(source.as_os_str().as_bytes())?;
    let target = CString::new(target.as_os_str().as_bytes())?;
    for flags in [
        libc::MS_BIND,
        libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY,
    ] {
        let result = unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                std::ptr::null(),
                flags,
                std::ptr::null(),
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error()).context("mount ephemeral store input");
        }
    }
    Ok(())
}

pub fn prepare(node: &Node, catalog: &Path) -> Result<()> {
    let started = Instant::now();
    ensure!(
        !node.root.join("nix/var/nix/db/db.sqlite").exists(),
        "ephemeral builder requires a fresh private database"
    );
    let manifest = if catalog.is_dir() {
        None
    } else {
        Some(Manifest::read(catalog)?)
    };
    let paths: Vec<String> = if let Some(manifest) = &manifest {
        manifest.paths.keys().cloned().collect()
    } else {
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(catalog.join("snapshot.json"))?)?;
        ensure!(metadata["version"] == 1, "unsupported snapshot version");
        serde_json::from_value(metadata["paths"].clone())?
    };
    ensure!(paths.iter().all(|p| valid_path(p)), "invalid snapshot path");
    let parsed = started.elapsed();
    node.prepare_ephemeral()?;
    refresh_metadata(&node.lower.join("nix/store"))?;
    for path in &paths {
        let source = physical(&node.lower, path);
        let target = physical(&node.root, path);
        ensure!(!target.try_exists()?, "store view must start empty: {path}");
        refresh_metadata(&source)?;
        let metadata = source.symlink_metadata()?;
        if metadata.file_type().is_symlink() {
            symlink(fs::read_link(&source)?, &target)?;
        } else if metadata.is_dir() {
            fs::create_dir(&target)?;
            readonly_bind(&source, &target)?;
        } else if metadata.is_file() {
            fs::File::create(&target)?;
            readonly_bind(&source, &target)?;
        } else {
            anyhow::bail!("unsupported shared path type: {path}");
        }
    }
    let mounted = started.elapsed();
    if let Some(manifest) = &manifest {
        native::register(&node.root, manifest)?;
    } else {
        let db = node.root.join("nix/var/nix/db");
        fs::create_dir_all(&db)?;
        for name in ["db.sqlite", "schema"] {
            fs::copy(catalog.join("nix/var/nix/db").join(name), db.join(name))?;
        }
    }
    durable(&node.base.join("ready"), &json!({"ephemeral":true}))?;
    durable(
        &node.base.join("startup.json"),
        &json!({
            "paths":paths.len(),
            "snapshot":manifest.is_none(),
            "catalog_seconds":parsed.as_secs_f64(),
            "mount_seconds":(mounted-parsed).as_secs_f64(),
            "register_seconds":(started.elapsed()-mounted).as_secs_f64(),
            "total_seconds":started.elapsed().as_secs_f64(),
        }),
    )?;
    Ok(())
}

pub fn cleanup(node: &Node) -> Result<()> {
    let store = node.root.join("nix/store");
    let mut mounts: Vec<std::path::PathBuf> = fs::read_to_string("/proc/self/mountinfo")?
        .lines()
        .filter_map(|line| line.split_whitespace().nth(4))
        .map(std::path::PathBuf::from)
        .filter(|path| path != &store && path.starts_with(&store))
        .collect();
    mounts.sort_by(|a, b| b.cmp(a));
    for path in mounts {
        let name = CString::new(path.as_os_str().as_bytes())?;
        if unsafe { libc::umount2(name.as_ptr(), libc::MNT_DETACH) } != 0 {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("unmount {}", path.display()));
        }
    }
    Ok(())
}
