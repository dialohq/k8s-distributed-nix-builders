//! Safe Rust facade over the linked, versioned Nix C ABI.
//! JSON stays in memory; no helper process, pipes, or temporary manifest file.
use crate::manifest::{Manifest, valid_path};
use anyhow::{Context, Result, bail, ensure};
use serde_json::Value;
use std::{
    ffi::{CString, c_char},
    os::unix::ffi::OsStrExt,
    path::Path,
};

const DUMP: u32 = 1;
const CHECK: u32 = 2;
const REGISTER: u32 = 3;

#[repr(C)]
struct Buffer {
    data: *mut u8,
    len: usize,
}
unsafe extern "C" {
    fn distributed_nix_call_v1(
        operation: u32,
        store: *const c_char,
        input: *const u8,
        input_len: usize,
        result: *mut Buffer,
    ) -> i32;
    fn distributed_nix_buffer_free_v1(buffer: *mut Buffer);
    fn distributed_nix_serve_store_v1(
        store: *const c_char,
        trusted: i32,
        result: *mut Buffer,
    ) -> i32;
    fn distributed_nix_serve_v1(trusted: i32, result: *mut Buffer) -> i32;
}
impl Drop for Buffer {
    fn drop(&mut self) {
        // SAFETY: the ABI alone allocated this buffer; this unique owner frees
        // it exactly once, using the allocating library's matching function.
        unsafe { distributed_nix_buffer_free_v1(self) };
    }
}
fn call(operation: u32, root: &Path, input: &[u8]) -> Result<Value> {
    let store = CString::new(root.as_os_str().as_bytes()).context("NUL in store path")?;
    let mut output = Buffer {
        data: std::ptr::null_mut(),
        len: 0,
    };
    // SAFETY: arguments are valid for the synchronous call; output is empty and
    // uniquely borrowed. C++ catches exceptions; no unwinding crosses this ABI.
    let status = unsafe {
        distributed_nix_call_v1(
            operation,
            store.as_ptr(),
            input.as_ptr(),
            input.len(),
            &mut output,
        )
    };
    ensure!(output.len <= isize::MAX as usize, "oversized native reply");
    let bytes = if output.len == 0 {
        &[]
    } else {
        ensure!(!output.data.is_null(), "null native reply");
        // SAFETY: successful allocation is owned by output, holds len initialized
        // bytes, and cannot be freed until this borrow is no longer used.
        unsafe { std::slice::from_raw_parts(output.data, output.len) }
    };
    if status != 0 {
        bail!(
            "native Nix (status {status}): {}",
            if bytes.is_empty() {
                "bridge allocation/argument failure".into()
            } else {
                String::from_utf8_lossy(bytes)
            }
        );
    }
    serde_json::from_slice(bytes).context("decode native Nix reply")
}
pub fn dump(root: &Path, paths: &[String]) -> Result<Manifest> {
    ensure!(
        !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
        "invalid dump paths"
    );
    Manifest::parse(call(DUMP, root, &serde_json::to_vec(paths)?)?)
}
pub fn check(root: &Path, manifest: &Manifest) -> Result<Value> {
    manifest.validate()?;
    call(CHECK, root, &serde_json::to_vec(manifest)?)
}
pub fn register(root: &Path, manifest: &Manifest) -> Result<Value> {
    manifest.validate()?;
    call(REGISTER, root, &serde_json::to_vec(manifest)?)
}
pub fn canonical_manifest(root: &Path, manifest: &Manifest) -> Result<Manifest> {
    manifest.validate()?;
    Manifest::parse(call(9, root, &serde_json::to_vec(manifest)?)?)
}
pub fn admit_local(
    root: &Path,
    manifest: &Manifest,
    local: &[String],
    register: bool,
) -> Result<Value> {
    manifest.validate()?;
    ensure!(
        local.iter().all(|p| manifest.paths.contains_key(p)),
        "local path outside manifest"
    );
    let mut request = serde_json::to_value(manifest)?;
    request["_preserveLocal"] = serde_json::to_value(local)?;
    call(
        if register { REGISTER } else { CHECK },
        root,
        &serde_json::to_vec(&request)?,
    )
}

/// Native liveness plus a conservative reference/deriver graph for cluster GC.
pub fn gc_snapshot(root: &Path) -> Result<Value> {
    call(4, root, b"null")
}
/// Exact candidate deletion with native liveness checks still enabled.
pub fn gc_delete(root: &Path, paths: &std::collections::BTreeSet<String>) -> Result<Value> {
    ensure!(paths.iter().all(|p| valid_path(p)), "invalid GC path");
    call(5, root, &serde_json::to_vec(paths)?)
}

pub fn valid_paths(root: &Path, paths: &[String]) -> Result<Value> {
    ensure!(paths.iter().all(|p| valid_path(p)), "invalid store path");
    call(6, root, &serde_json::to_vec(paths)?)
}
pub fn dump_realisations(root: &Path, pending: &[Value]) -> Result<Value> {
    call(7, root, &serde_json::to_vec(pending)?)
}

pub fn realisation_conflicts(root: &Path, manifest: &Manifest) -> Result<Value> {
    manifest.validate()?;
    call(13, root, &serde_json::to_vec(&manifest.realisations)?)
}
pub fn scan_realisations(root: &Path) -> Result<Vec<Value>> {
    Ok(serde_json::from_value(call(8, root, b"null")?)?)
}
pub fn catalog(root: &Path) -> Result<Manifest> {
    Manifest::parse(call(10, root, b"null")?)
}
pub fn serve(trusted: bool) -> Result<()> {
    let mut result = Buffer {
        data: std::ptr::null_mut(),
        len: 0,
    };
    // SAFETY: dedicated worker process; stdin/out are the client connection.
    let status = unsafe { distributed_nix_serve_v1(i32::from(trusted), &mut result) };
    if status != 0 {
        let bytes = if result.len == 0 {
            &[]
        } else {
            ensure!(
                !result.data.is_null() && result.len <= isize::MAX as usize,
                "invalid native error"
            );
            unsafe { std::slice::from_raw_parts(result.data, result.len) }
        };
        bail!("native daemon: {}", String::from_utf8_lossy(bytes));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cpp_errors_return_to_rust_repeatedly() {
        // Neither JSON parse exceptions nor invalid operation exceptions escape.
        for _ in 0..64 {
            assert!(
                call(DUMP, Path::new("/unused"), b"{")
                    .unwrap_err()
                    .to_string()
                    .contains("parse_error")
            );
            assert!(
                call(99, Path::new("/unused"), b"[]")
                    .unwrap_err()
                    .to_string()
                    .contains("unknown Nix bridge operation")
            );
        }
    }
    #[test]
    fn rejects_embedded_nul_and_handles_empty_input() {
        assert!(call(DUMP, Path::new("/bad\0root"), b"[]").is_err());
        assert!(call(DUMP, Path::new("/unused"), b"").is_err());
    }
    #[test]
    fn c_abi_null_arguments_and_buffer_release() {
        let mut output = Buffer {
            data: std::ptr::null_mut(),
            len: 0,
        };
        // SAFETY: deliberate null arguments are allowed by the documented error
        // contract; output is valid. No invalid non-null pointers are supplied.
        unsafe {
            assert_eq!(
                distributed_nix_call_v1(DUMP, std::ptr::null(), std::ptr::null(), 0, &mut output),
                2
            );
            assert!(!output.data.is_null());
            distributed_nix_buffer_free_v1(&mut output);
            assert!(output.data.is_null() && output.len == 0);
            distributed_nix_buffer_free_v1(std::ptr::null_mut());
            assert_eq!(
                distributed_nix_call_v1(
                    DUMP,
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    std::ptr::null_mut()
                ),
                2
            );
        }
    }
}

pub fn snapshot(root: &Path, destination: &Path) -> Result<Value> {
    call(11, root, &serde_json::to_vec(destination)?)
}

pub fn copy(root: &Path, target: &str, paths: &[String]) -> Result<Value> {
    ensure!(
        !paths.is_empty() && paths.iter().all(|p| valid_path(p)),
        "invalid copy roots"
    );
    call(
        12,
        root,
        &serde_json::to_vec(&serde_json::json!({"target":target,"paths":paths}))?,
    )
}

pub fn serve_store(root: &Path) -> Result<()> {
    let uri = CString::new(root.as_os_str().as_bytes())?;
    let mut result = Buffer {
        data: std::ptr::null_mut(),
        len: 0,
    };
    let status = unsafe { distributed_nix_serve_store_v1(uri.as_ptr(), 1, &mut result) };
    ensure!(
        result.len <= isize::MAX as usize,
        "oversized native server reply"
    );
    if status != 0 {
        let detail = if result.len == 0 {
            String::new()
        } else {
            ensure!(!result.data.is_null(), "null native server reply");
            String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(result.data, result.len) })
                .into_owned()
        };
        bail!("native collection daemon: {detail}");
    }
    Ok(())
}
