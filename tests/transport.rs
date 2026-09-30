use anyhow::{Result, ensure};
use distributed_nix::{
    native,
    node::Node,
    online_rpc::{
        Config,
        wire::{
            StoreRequest, store_request::Operation, store_transport_client::StoreTransportClient,
        },
    },
    transport::{Client, server},
    util::{durable, output},
};
use serde_json::json;
use std::{fs, path::PathBuf, process::Command};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_stream_publication_is_authenticated_retryable_and_cannot_collect() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let base = directory.path();
    let node = Node {
        base: base.join("state"),
        root: base.join("source"),
        origin: base.join("origin"),
        lower: base.join("origin"),
    };
    fs::create_dir_all(&node.base)?;
    fs::create_dir_all(&node.root)?;
    durable(&node.base.join("ready"), &json!(true))?;
    native::valid_paths(&node.origin, &[])?;
    native::valid_paths(&node.root, &[])?;
    let reader = rusqlite::Connection::open(node.root.join("nix/var/nix/db/db.sqlite"))?;
    reader.execute_batch(
        "PRAGMA wal_checkpoint(TRUNCATE); BEGIN; SELECT count(*) FROM ValidPaths;",
    )?;
    fs::write(base.join("input"), vec![b'x'; 1024 * 1024])?;
    let source = base.join("source");
    let added = output(
        Command::new("nix-store")
            .args(["--store", source.to_str().unwrap(), "--add"])
            .arg(base.join("input")),
    )?;
    let path = String::from_utf8(added.stdout)?.trim().to_owned();
    let manifest = native::dump(&source, &[path.clone()])?;
    let token = "native-transfer-test-012345678901234567890";
    fs::write(base.join("token"), token)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = Config {
        nodes: vec![address.to_string()],
        index: 0,
        token_file: base.join("token"),
        pod_uid: None,
    };
    let (stop, stopping) = tokio::sync::oneshot::channel();
    let service = server(
        node.clone(),
        config,
        PathBuf::from(env!("CARGO_BIN_EXE_distributed-nix")),
    )?;
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = stopping.await;
            })
            .await
    });
    let mut unauthenticated = StoreTransportClient::connect(format!("http://{address}")).await?;
    ensure!(
        unauthenticated
            .operate(StoreRequest::default())
            .await
            .unwrap_err()
            .code()
            == tonic::Code::Unauthenticated
    );
    let client = Client::new(&address.to_string(), token, true)?;
    let lease = client.lease().await?;
    let mut receive = StoreRequest::default();
    receive.operation = Operation::Receive as i32;
    receive.manifest = serde_json::to_vec(&manifest)?;
    let receipt = client.operate(receive).await?;
    let id = receipt["batch"].as_str().unwrap().to_owned();
    let request = |operation| StoreRequest {
        operation: operation as i32,
        batch: id.clone(),
        ..Default::default()
    };
    ensure!(client.operate(request(Operation::Commit)).await.is_err());
    client.operate(request(Operation::Reserve)).await?;
    let exporter = Client::new(&address.to_string(), token, false)?;
    let source_socket = base.join("source.sock");
    let source_proxy = exporter.proxy(&source_socket)?;
    let source = PathBuf::from(format!("unix://{}", source_socket.display()));
    let socket = base.join("native.sock");
    let proxy = client.proxy(&socket)?;
    let destination = format!("unix://{}", socket.display());
    for _ in 0..2 {
        let source = source.clone();
        let path = path.clone();
        let destination = destination.clone();
        tokio::task::spawn_blocking(move || native::copy(&source, &destination, &[path])).await??;
        client.operate(request(Operation::Commit)).await?;
    }
    ensure!(
        fs::read(node.origin.join(path.trim_start_matches('/')))? == fs::read(base.join("input"))?
    );
    ensure!(
        node.origin
            .join(".distributed-nix-publications")
            .join(format!("{id}.json"))
            .exists()
    );
    client.operate(request(Operation::Release)).await?;
    client.operate(request(Operation::Release)).await?;
    ensure!(
        !node
            .base
            .join("incoming")
            .join(format!("{id}.json"))
            .exists()
    );
    ensure!(
        node.origin
            .join(".distributed-nix-publications")
            .join(format!("{id}.json"))
            .exists()
    );
    let gc_uri = destination.clone();
    let forbidden = tokio::task::spawn_blocking(move || {
        Command::new("nix-store")
            .args(["--store", &gc_uri, "--gc"])
            .output()
    })
    .await??;
    ensure!(!forbidden.status.success());
    ensure!(String::from_utf8_lossy(&forbidden.stderr).contains("administrator coordinator"));
    fs::write(base.join("upload"), "worker exports must reject uploads")?;
    let added = output(
        Command::new("nix-store")
            .arg("--store")
            .arg(&node.origin)
            .arg("--add")
            .arg(base.join("upload")),
    )?;
    let upload = String::from_utf8(added.stdout)?.trim().to_owned();
    let origin = node.origin.clone();
    let export_uri = source.to_str().unwrap().to_owned();
    let paths = vec![upload.clone()];
    let rejected = tokio::task::spawn_blocking(move || native::copy(&origin, &export_uri, &paths))
        .await?
        .unwrap_err();
    ensure!(rejected.to_string().contains("export-only"));
    ensure!(native::valid_paths(&node.root, &[upload])? == json!([]));
    let bad = StoreRequest {
        operation: Operation::Publication as i32,
        batch: "../../etc/passwd".into(),
        ..Default::default()
    };
    ensure!(client.operate(bad).await.is_err());
    use std::os::fd::AsRawFd;
    let lock = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(node.base.join("publication.lock"))?;
    ensure!(unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0);
    drop(source_proxy);
    drop(exporter);
    drop(proxy);
    drop(lease);
    drop(client);
    drop(unauthenticated);
    drop(lock);
    // Acquiring the actual exclusive gate is the acknowledgement of cancellation.
    // No delay is evidence that the server has released its publication leases.
    let base = node.base.clone();
    let released = tokio::task::spawn_blocking(move || {
        distributed_nix::util::Lock::acquire(&base.join("publication.lock"), false)
    })
    .await??;
    drop(released);
    stop.send(()).unwrap();
    task.await??;
    Ok(())
}

#[test]
fn membership_accepts_dns_and_arbitrary_pool_size_but_rejects_reuse() -> Result<()> {
    let temp = tempfile::tempdir()?;
    fs::write(
        temp.path().join("token"),
        "0123456789012345678901234567890123456789",
    )?;
    for size in [2, 5] {
        let node = Node {
            base: temp.path().join(format!("state-{size}")),
            root: temp.path().join("worker"),
            origin: temp.path().join("origin"),
            lower: temp.path().join("lower"),
        };
        let mut config = Config {
            nodes: (0..size)
                .map(|i| format!("builder-{i}.pool.test.svc.cluster.local:9840"))
                .collect(),
            index: 1,
            token_file: temp.path().join("token"),
            pod_uid: Some("current-pod".into()),
        };
        distributed_nix::online_rpc::Service::new(node.clone(), config.clone())?;
        config.pod_uid = Some("replacement-pod".into());
        distributed_nix::online_rpc::Service::new(node.clone(), config.clone())?;
        config.nodes.pop();
        ensure!(distributed_nix::online_rpc::Service::new(node, config).is_err());
    }
    Ok(())
}

#[test]
fn concurrent_membership_initialization_has_one_winner() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let node = Node {
        base: directory.path().join("state"),
        ..Node::default()
    };
    let token = directory.path().join("token");
    fs::write(&token, "concurrent-membership-012345678901234567890")?;
    let start = std::sync::Barrier::new(2);
    let outcomes = std::thread::scope(|scope| {
        let jobs: Vec<_> = ["first:9840", "second:9840"]
            .into_iter()
            .map(|endpoint| {
                let node = node.clone();
                let token = token.clone();
                let start = &start;
                scope.spawn(move || {
                    let config = Config {
                        nodes: vec![endpoint.into()],
                        index: 0,
                        token_file: token,
                        pod_uid: None,
                    };
                    start.wait();
                    (
                        endpoint,
                        distributed_nix::online_rpc::Service::new(node, config).is_ok(),
                    )
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    let winners: Vec<_> = outcomes
        .into_iter()
        .filter(|(_, success)| *success)
        .collect();
    ensure!(winners.len() == 1);
    let membership: serde_json::Value =
        serde_json::from_slice(&fs::read(node.base.join("membership.json"))?)?;
    ensure!(membership["nodes"] == json!([winners[0].0]));
    Ok(())
}
