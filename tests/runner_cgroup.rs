use anyhow::{Context, Result, ensure};
use distributed_nix::job_cgroup::Group;
use std::{io::Write, path::Path, process::Stdio};
use tokio::io::AsyncReadExt;

static CGROUP_MOUNT: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
    let _mount = CGROUP_MOUNT.lock().unwrap();
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

#[tokio::test]
#[ignore = "requires writable cgroup v2 and root"]
async fn cleaning_one_job_preserves_concurrent_job_processes() -> Result<()> {
    let _mount = CGROUP_MOUNT.lock().unwrap();
    let temp = tempfile::tempdir()?;
    let parent = distributed_nix::job_cgroup::parent(&temp.path().join("cgroups"))?;
    let mut jobs = Vec::new();
    for i in 0..2 {
        let socket = temp.path().join(format!("ready-{i}"));
        let listener = tokio::net::UnixListener::bind(&socket)?;
        let id = format!("parallel-{}-{i}", std::process::id());
        let group = Group::new(&parent, &id)?;
        let mut command = tokio::process::Command::new(std::env::current_exe()?);
        command
            .args(["--exact", "detached_fixture", "--nocapture"])
            .env("DISTRIBUTED_NIX_CGROUP_FIXTURE", &socket)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        group.attach(&mut command, false)?;
        let child = command.spawn()?;
        drop(command);
        let (mut connection, _) = listener.accept().await?;
        let mut byte = [0];
        connection.read_exact(&mut byte).await?;
        ensure!(byte == *b"R");
        jobs.push((group, child, connection, id));
    }
    let (first, mut first_child, mut first_socket, _) = jobs.remove(0);
    first.stop().await?;
    ensure!(!first_child.wait().await?.success());
    ensure!(first_socket.read(&mut [0]).await? == 0);

    let (second, mut second_child, mut second_socket, id) = jobs.remove(0);
    ensure!(second_child.try_wait()?.is_none());
    let events = std::fs::read_to_string(parent.join(format!("arc-{id}/cgroup.events")))?;
    ensure!(events.lines().any(|line| line == "populated 1"));
    second.stop().await?;
    ensure!(!second_child.wait().await?.success());
    ensure!(second_socket.read(&mut [0]).await? == 0);
    distributed_nix::linux::unmount(&temp.path().join("cgroups"))?;
    Ok(())
}
