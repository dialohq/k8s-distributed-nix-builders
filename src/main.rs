use anyhow::{Result, bail};
use distributed_nix::{cluster::Cluster, node::Node, util::arg};
fn main() {
    if let Err(e) = execute() {
        eprintln!("distributed-nix: {e:#}");
        std::process::exit(1);
    }
}
fn execute() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("pod") {
        return distributed_nix::pod::run();
    }
    if args.first().map(String::as_str) == Some("pod-seed") {
        return distributed_nix::pod::seed_worker(arg(&args, 1)?);
    }
    if args.first().map(String::as_str) == Some("pod-mount-nfs") {
        return distributed_nix::pod::mount_nfs_worker();
    }
    if args.is_empty() || args[0] == "--help" {
        println!(
            "distributed-nix: native Nix, private metadata, shared packages\n\nConfiguration: DISTRIBUTED_NIX_ONLINE_CONFIG (default /etc/distributed-nix/online-gc.json)\nCommands: serve, publisher, status, bootstrap, publish PARTICIPANT PATH..., reconcile ID, gc [--dry-run | --if-needed]\nInternal: node OP ARGS..., pivot ROOT COMMAND ARGS..."
        );
        return Ok(());
    }
    if matches!(args[0].as_str(), "gc" | "serve") {
        let file = std::env::var("DISTRIBUTED_NIX_ONLINE_CONFIG")
            .unwrap_or_else(|_| "/etc/distributed-nix/online-gc.json".into());
        let config: distributed_nix::online_rpc::Config =
            serde_json::from_slice(&std::fs::read(file)?)?;
        return tokio::runtime::Runtime::new()?.block_on(async {
            if args[0] == "serve" {
                let service =
                    distributed_nix::online_rpc::Service::new(Node::default(), config.clone())?;
                let listener = tokio::net::TcpListener::bind("0.0.0.0:9840").await?;
                distributed_nix::online_rpc::serve(listener, service, async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
            } else {
                anyhow::ensure!(
                    args.len() == 1
                        || (args.len() == 2
                            && matches!(args[1].as_str(), "--dry-run" | "--if-needed")),
                    "usage: gc [--dry-run | --if-needed]"
                );
                let threshold = if args.get(1).is_some_and(|s| s == "--if-needed") {
                    Some(
                        std::env::var("DISTRIBUTED_NIX_GC_MIN_FREE_PERCENT")
                            .unwrap_or_else(|_| "20".into())
                            .parse()?,
                    )
                } else {
                    None
                };
                let result = distributed_nix::online_rpc::collect(
                    &Node::default(),
                    &config,
                    args.get(1).is_some_and(|s| s == "--dry-run"),
                    threshold,
                )
                .await?;
                println!("{}", serde_json::to_string_pretty(&result)?);
                Ok(())
            }
        });
    }
    if args[0] == "native-transfer" {
        return distributed_nix::native::serve_transfer(std::path::Path::new(arg(&args, 1)?));
    }
    if args[0] == "native-gc" {
        distributed_nix::node::enter_chroot(arg(&args, 1)?)?;
        let root = std::path::Path::new("local?path-info-cache-size=0");
        let v = match arg(&args, 2)? {
            "snapshot" => distributed_nix::native::gc_snapshot(root)?,
            "online-snapshot" => distributed_nix::native::online_snapshot(root)?,
            "delete" => distributed_nix::native::gc_delete(
                root,
                &serde_json::from_slice(&distributed_nix::util::read_stdin()?)?,
            )?,
            _ => bail!("unknown native GC operation"),
        };
        println!("{}", serde_json::to_string(&v)?);
        return Ok(());
    }
    if args[0] == "native-daemon" {
        return distributed_nix::service::native_connection(
            arg(&args, 1)?,
            arg(&args, 2)? == "trusted",
        );
    }
    if args[0] == "pivot" {
        return distributed_nix::node::pivot(&args[1..]);
    }
    let result = if args[0] == "node" {
        Node::default().dispatch(&args[1..])?
    } else {
        let c = Cluster::new()?;
        match args[0].as_str() {
            "publisher" => c.publisher()?,
            "status" => c.status()?,
            "bootstrap" => c.bootstrap(&Node::default())?,
            "publish" => c.publish(arg(&args, 1)?.parse()?, &args[2..])?,
            "reconcile" => c.reconcile(arg(&args, 1)?)?,
            _ => bail!("unknown command; use --help"),
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
