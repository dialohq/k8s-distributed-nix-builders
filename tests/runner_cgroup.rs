use anyhow::{Context, Result, ensure};
use distributed_nix::job_cgroup::Group;
use std::{io::Write, path::Path, process::Stdio};
use tokio::io::AsyncReadExt;

#[test]
fn detached_fixture() -> Result<()> {
    let Some(socket) = std::env::var_os("DISTRIBUTED_NIX_CGROUP_FIXTURE") else {
        return Ok(());
    };
    unsafe {
        match libc::fork() {
            -1 => return Err(std::io::Error::last_os_error().into()),
            0 => {
                ensure!(libc::setsid() >= 0);
            }
            _ => loop {
                libc::pause();
            },
        }
    }
    let mut connection = std::os::unix::net::UnixStream::connect(socket)?;
    connection.write_all(b"R")?;
    loop {
        unsafe {
            libc::pause();
        }
    }
}

#[tokio::test]
#[ignore = "requires writable cgroup v2 and root"]
async fn detached_descendant_is_dead_before_group_is_reusable() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let socket = temp.path().join("ready");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let parent = distributed_nix::job_cgroup::parent(&temp.path().join("cgroups"))?;
    let id = format!("test-{}", std::process::id());
    let group = Group::new(&parent, &id)?;
    let mut command = tokio::process::Command::new(std::env::current_exe()?);
    command
        .args(["--exact", "detached_fixture", "--nocapture"])
        .env("DISTRIBUTED_NIX_CGROUP_FIXTURE", &socket)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    group.attach(&mut command, false)?;
    let mut child = command.spawn()?;
    drop(command);
    let (mut connection, _) = listener.accept().await?;
    let mut byte = [0];
    connection
        .read_exact(&mut byte)
        .await
        .context("fixture readiness")?;
    ensure!(byte == *b"R");
    group.stop().await?;
    ensure!(!child.wait().await?.success());
    ensure!(
        connection.read(&mut byte).await? == 0,
        "detached child kept socket alive"
    );
    let next = Group::new(&parent, &id)?;
    next.stop().await?;
    distributed_nix::linux::unmount(Path::new(&temp.path().join("cgroups")))?;
    Ok(())
}
