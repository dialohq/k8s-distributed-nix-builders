use anyhow::{Result, ensure};
use distributed_nix::{native, rpc, util::output};
use serde_json::json;
use std::{fs, path::Path, process::Command, time::Duration};

const TOKEN: &str = "test-collection-token-with-at-least-32-bytes";

fn add(root: &Path, input: &Path) -> Result<String> {
    let added = output(
        Command::new("nix-store")
            .args(["--option", "build-users-group", "", "--store"])
            .arg(root)
            .arg("--add")
            .arg(input),
    )?;
    Ok(String::from_utf8(added.stdout)?.trim().into())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publication_survives_builder_and_server_replacement() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(45), scenario()).await?
}

async fn scenario() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let origin = temp.path().join("origin");
    let builder = temp.path().join("builder");
    let fixture = temp.path().join("fixture");
    fs::write(&fixture, vec![b'x'; 2 * 1024 * 1024])?;
    let path = add(&builder, &fixture)?;
    let mut manifest = native::dump(&builder, &[path.clone()])?;
    let ca_id = format!("sha256:{}!out", "1".repeat(64));
    manifest.realisations.insert(
        ca_id.clone(),
        json!({
            "id": ca_id, "outPath": path.trim_start_matches("/nix/store/"),
            "signatures": [], "dependentRealisations": {},
        }),
    );
    native::register(&builder, &manifest)?;
    let service = rpc::Service::new(
        origin.clone(),
        TOKEN.into(),
        env!("CARGO_BIN_EXE_distributed-nix").into(),
    )?;
    ensure!(
        rpc::Service::new(
            origin.clone(),
            TOKEN.into(),
            env!("CARGO_BIN_EXE_distributed-nix").into()
        )
        .is_err(),
        "two collection coordinators acquired the same store"
    );
    let weak_service = std::sync::Arc::downgrade(&service);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(rpc::serve(listener, service, std::future::pending()));
    let client = rpc::Client::connect(&endpoint, TOKEN, "builder-1").await?;
    let invalid = rpc::Client::connect(&endpoint, "wrong-token", "builder-1").await?;
    ensure!(invalid.bootstrap().await.is_err(), "invalid token accepted");
    ensure!(
        client.prepare(&manifest).await.is_err(),
        "unregistered builder admitted"
    );
    let first = client.bootstrap().await?;
    ensure!(first.paths == 0);
    ensure!(
        first == client.bootstrap().await?,
        "bootstrap retry changed generation"
    );
    let observer = rpc::Client::connect(&endpoint, TOKEN, "old-generation-reader").await?;
    ensure!(observer.bootstrap().await? == first);
    let canonical = client.prepare(&manifest).await?;
    ensure!(
        client.commit(&canonical).await.is_err(),
        "missing output committed"
    );
    let socket = temp.path().join("store.sock");
    let proxy = client.proxy(&socket)?;
    let gc_target = format!("unix://{}", socket.display());
    let gc = tokio::task::spawn_blocking(move || {
        Command::new("nix-store")
            .args(["--store", &gc_target, "--gc"])
            .output()
    })
    .await??;
    ensure!(!gc.status.success(), "uncoordinated collection GC accepted");
    ensure!(String::from_utf8_lossy(&gc.stderr).contains("drained collection"));
    let source = builder.clone();
    let paths = vec![path.clone()];
    let target = format!("unix://{}", socket.display());
    tokio::task::spawn_blocking(move || native::copy(&source, &target, &paths)).await??;
    client.commit(&canonical).await?;
    client.commit(&canonical).await?;
    ensure!(native::dump(&origin, &[path.clone()])?.paths == canonical.paths);
    let ca = native::dump_realisations(&origin, &[canonical.realisations[&ca_id].clone()])?;
    ensure!(ca["manifest"]["realisations"] == serde_json::to_value(&canonical.realisations)?);
    drop(proxy);
    let node = distributed_nix::node::Node {
        root: builder.clone(),
        base: temp.path().join("builder-state"),
        origin: origin.clone(),
        lower: origin.clone(),
    };
    distributed_nix::util::durable(
        &node
            .base
            .join("outbox")
            .join(format!("{}.json", path.rsplit('/').next().unwrap())),
        &json!({"path":path}),
    )?;
    use sha2::{Digest, Sha256};
    let key = format!("{:x}", Sha256::digest(ca_id.as_bytes()));
    distributed_nix::util::durable(
        &node.base.join("ca-outbox").join(format!("{key}.json")),
        &canonical.realisations[&ca_id],
    )?;
    ensure!(invalid.flush(&node).await.is_err());
    ensure!(
        node.outbox()? == json!([path]),
        "failed publication discarded outbox"
    );
    ensure!(
        !node.ca_outbox()?.is_null(),
        "failed publication discarded CA outbox"
    );
    ensure!(client.flush(&node).await? == 2);
    ensure!(node.outbox()? == json!([]));
    ensure!(node.ca_outbox()?.is_null());
    for _ in 0..100 {
        if client.release().await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    client.release().await?;
    let old_snapshot = origin
        .join(".distributed-nix-catalogs")
        .join(&first.generation);
    ensure!(
        old_snapshot.is_dir(),
        "removed a generation with an active member"
    );
    observer.release().await?;
    ensure!(!old_snapshot.exists(), "unused generation was not retired");
    fs::remove_dir_all(&builder)?;
    drop(client);
    drop(invalid);
    drop(observer);
    server.abort();
    let _ = server.await;
    for _ in 0..100 {
        if weak_service.upgrade().is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let service = rpc::Service::new(
        origin.clone(),
        TOKEN.into(),
        env!("CARGO_BIN_EXE_distributed-nix").into(),
    )?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(rpc::serve(listener, service, std::future::pending()));
    let fresh = rpc::Client::connect(&endpoint, TOKEN, "builder-2").await?;
    let snapshot = fresh.bootstrap().await?;
    ensure!(snapshot.paths == 1 && snapshot.generation != first.generation);
    let restored = origin
        .join(".distributed-nix-catalogs")
        .join(snapshot.generation);
    fs::copy(
        origin.join(path.trim_start_matches('/')),
        restored.join(path.trim_start_matches('/')),
    )?;
    let ca = native::dump_realisations(&restored, &[canonical.realisations[&ca_id].clone()])?;
    ensure!(ca["manifest"]["realisations"] == serde_json::to_value(&canonical.realisations)?);
    output(
        Command::new("nix-store")
            .arg("--store")
            .arg(&restored)
            .args(["--verify", "--check-contents"]),
    )?;
    fresh.release().await?;
    server.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interrupted_streams_release_their_sessions() -> Result<()> {
    tokio::time::timeout(Duration::from_secs(15), async {
        use rpc::wire::{Frame, Member, collection_client::CollectionClient};
        use tokio_stream::wrappers::ReceiverStream;
        let temp = tempfile::tempdir()?;
        let service = rpc::Service::new(
            temp.path().join("origin"),
            TOKEN.into(),
            env!("CARGO_BIN_EXE_distributed-nix").into(),
        )?;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let server = tokio::spawn(rpc::serve(listener, service, std::future::pending()));
        let client = rpc::Client::connect(&endpoint, TOKEN, "cancel-test").await?;
        client.bootstrap().await?;
        let mut wire = CollectionClient::connect(endpoint).await?;
        let authorize = |request: &mut tonic::metadata::MetadataMap| {
            request.insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
            request.insert("x-member", "cancel-test".parse().unwrap());
        };
        let mut mismatch = tonic::Request::new(Member {
            id: "someone-else".into(),
        });
        authorize(mismatch.metadata_mut());
        ensure!(wire.release(mismatch).await.unwrap_err().code() == tonic::Code::InvalidArgument);
        let (sender, receiver) = tokio::sync::mpsc::channel::<Frame>(1);
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        authorize(request.metadata_mut());
        let response = wire.store(request).await?;
        ensure!(
            client.release().await.is_err(),
            "released member with active stream"
        );
        drop(response);
        drop(sender);
        for _ in 0..100 {
            if client.release().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        client.release().await?;
        client.bootstrap().await?;
        let (sender, receiver) = tokio::sync::mpsc::channel::<Frame>(1);
        let mut request = tonic::Request::new(ReceiverStream::new(receiver));
        authorize(request.metadata_mut());
        let mut response = wire.store(request).await?.into_inner();
        sender
            .send(Frame {
                data: vec![0; 65537],
            })
            .await?;
        ensure!(
            response.message().await.is_err(),
            "oversized frame accepted"
        );
        drop(response);
        drop(sender);
        for _ in 0..100 {
            if client.release().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        client.release().await?;
        server.abort();
        Ok::<(), anyhow::Error>(())
    })
    .await?
}
