use anyhow::{Result, ensure};
use distributed_nix::{
    node::Node,
    online_rpc::wire::{GcRequest, online_gc_client::OnlineGcClient},
    online_rpc::{Config, Service, serve},
    util::durable,
};
use serde_json::json;
use std::{fs, path::PathBuf};
use tonic::{Code, Request};

#[tokio::test]
async fn authenticated_rpc_rejects_unknown_pods_and_conflicting_epochs() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = temp.path();
    let node = Node {
        base: base.into(),
        root: base.join("root"),
        origin: base.join("origin"),
        lower: base.join("lower"),
    };
    fs::create_dir_all(&node.root)?;
    durable(&base.join("ready"), &json!(true))?;
    let token = "online-gc-test-012345678901234567890123456789";
    fs::write(base.join("token"), token)?;
    let cri = base.join("cri.json");
    fs::write(&cri, r#"{"items":[]}"#)?;
    let cat: PathBuf = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|p| p.join("cat"))
        .find(|p| p.is_file())
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let config = Config {
        nodes: vec![address, "127.0.0.1:1".parse()?, "127.0.0.1:2".parse()?],
        index: 0,
        token_file: base.join("token"),
        cri_command: vec![cat.to_str().unwrap().into(), cri.to_str().unwrap().into()],
        namespace: "arc-runners".into(),
    };
    let service = Service::new(node.clone(), config.clone())?;
    let (stop, stopping) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(serve(listener, service, async {
        let _ = stopping.await;
    }));
    let mut client = OnlineGcClient::connect(format!("http://{address}")).await?;
    let id = "00000000000000000000000000000001";
    let request = || GcRequest {
        epoch: id.into(),
        origin: false,
        plan_json: vec![],
    };
    ensure!(client.preflight(request()).await.unwrap_err().code() == Code::Unauthenticated);
    let authorized = |mut r: GcRequest| {
        if r.epoch.is_empty() {
            r.epoch = id.into();
        }
        let mut request = Request::new(r);
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
    };
    client.preflight(authorized(request())).await?;
    fs::write(
        &cri,
        r#"{"items":[{"metadata":{"namespace":"arc-runners","uid":"unknown-pod"}}]}"#,
    )?;
    ensure!(
        client
            .preflight(authorized(request()))
            .await
            .unwrap_err()
            .code()
            == Code::FailedPrecondition
    );
    let _group = distributed_nix::online::group(&node.base, &node.root, Some("unknown-pod"))?;
    client.preflight(authorized(request())).await?;
    let plan = distributed_nix::gc::Plan {
        id: id.into(),
        keep: Default::default(),
        workers: vec![Default::default(); 3],
        origin: Default::default(),
    };
    let prepared = || GcRequest {
        epoch: id.into(),
        origin: false,
        plan_json: serde_json::to_vec(&plan).unwrap(),
    };
    client.prepare(authorized(prepared())).await?;
    client.prepare(authorized(prepared())).await?;
    let mut conflicting = request();
    conflicting.epoch = "00000000000000000000000000000002".into();
    ensure!(
        client
            .preflight(authorized(conflicting))
            .await
            .unwrap_err()
            .code()
            == Code::FailedPrecondition
    );
    client.plan(authorized(prepared())).await?;
    let mut origin = request();
    origin.origin = true;
    ensure!(client.sweep(authorized(origin)).await.unwrap_err().code() == Code::FailedPrecondition);
    ensure!(
        client
            .finish(authorized(request()))
            .await
            .unwrap_err()
            .code()
            == Code::FailedPrecondition
    );
    // Failed endpoint connection does not clear durable retirement fences.
    ensure!(
        distributed_nix::online_rpc::collect(&node, &config, false, None)
            .await
            .is_err()
    );
    ensure!(base.join("online-gc.json").exists() && base.join("retiring").exists());
    stop.send(()).unwrap();
    server.await??;
    Ok(())
}

#[tokio::test]
async fn coordinator_rejects_duplicate_worker_identity_before_marking() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let base = temp.path();
    let node = Node {
        base: base.into(),
        root: base.join("root"),
        origin: base.join("origin"),
        lower: base.join("lower"),
    };
    fs::create_dir_all(&node.root)?;
    durable(&base.join("ready"), &json!(true))?;
    fs::write(
        base.join("token"),
        "online-gc-test-012345678901234567890123456789",
    )?;
    fs::write(base.join("cri.json"), r#"{"items":[]}"#)?;
    let cat = std::env::split_paths(&std::env::var_os("PATH").unwrap())
        .map(|p| p.join("cat"))
        .find(|p| p.is_file())
        .unwrap();
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await?);
    }
    let config = Config {
        nodes: listeners.iter().map(|l| l.local_addr().unwrap()).collect(),
        index: 0,
        token_file: base.join("token"),
        cri_command: vec![
            cat.to_str().unwrap().into(),
            base.join("cri.json").to_str().unwrap().into(),
        ],
        namespace: "arc-runners".into(),
    };
    let mut tasks = Vec::new();
    for listener in listeners {
        let (stop, stopping) = tokio::sync::oneshot::channel();
        tasks.push((
            stop,
            tokio::spawn(serve(
                listener,
                Service::new(node.clone(), config.clone())?,
                async {
                    let _ = stopping.await;
                },
            )),
        ));
    }
    let error = distributed_nix::online_rpc::collect(&node, &config, false, None)
        .await
        .unwrap_err();
    ensure!(error.to_string().contains("wrong node identity"));
    ensure!(!base.join("online-master.json").exists() && !base.join("retiring").exists());
    for (stop, task) in tasks {
        stop.send(()).unwrap();
        task.await??;
    }
    Ok(())
}
