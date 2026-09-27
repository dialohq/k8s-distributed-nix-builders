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
    if args.is_empty() || args[0] == "--help" {
        println!(
            "distributed-nix: native Nix + node-local metadata + shared packages\n\nConfiguration: /etc/distributed-nix/cluster.json (override: DISTRIBUTED_NIX_CONFIG)\nCommands: exec NODE COMMAND, build NODE ARGS..., publish NODE PATH..., reconcile ID, recover NODE, gc [--dry-run], gc-maintenance [--dry-run]\nInternal: node OP ARGS..., pivot ROOT COMMAND ARGS...\nExperimental: catalog STORE OUTPUT, snapshot STORE DIRECTORY, ephemeral-builder CATALOG_OR_SNAPSHOT, ephemeral-cleanup"
        );
        return Ok(());
    }
    if (matches!(args[0].as_str(), "online-gc" | "online-gc-server")
        || (args[0] == "gc"
            && std::path::Path::new("/etc/distributed-nix/online-gc.json").exists()))
    {
        let file = std::env::var("DISTRIBUTED_NIX_ONLINE_CONFIG")
            .unwrap_or_else(|_| "/etc/distributed-nix/online-gc.json".into());
        let config: distributed_nix::online_rpc::Config =
            serde_json::from_slice(&std::fs::read(file)?)?;
        return tokio::runtime::Runtime::new()?.block_on(async {
            if args[0] == "online-gc-server" {
                let service =
                    distributed_nix::online_rpc::Service::new(Node::default(), config.clone())?;
                let listener = tokio::net::TcpListener::bind(config.nodes[config.index]).await?;
                distributed_nix::online_rpc::serve(listener, service, async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await
            } else {
                anyhow::ensure!(
                    args.len() == 1
                        || (args.len() == 2
                            && matches!(args[1].as_str(), "--dry-run" | "--if-needed")),
                    "usage: online-gc [--dry-run | --if-needed]"
                );
                let threshold = if args.get(1).is_some_and(|s| s == "--if-needed") {
                    Some(
                        std::env::var("CIBOX_GC_MIN_FREE_PERCENT")
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
    if args[0] == "snapshot" {
        let destination = std::path::Path::new(arg(&args, 2)?);
        let metadata =
            distributed_nix::native::snapshot(std::path::Path::new(arg(&args, 1)?), destination)?;
        distributed_nix::util::durable(&destination.join("snapshot.json"), &metadata)?;
        println!(
            "{}",
            serde_json::json!({"paths":metadata["paths"].as_array().unwrap().len()})
        );
        return Ok(());
    }
    if args[0] == "catalog" {
        let manifest = distributed_nix::native::catalog(std::path::Path::new(arg(&args, 1)?))?;
        distributed_nix::util::durable(std::path::Path::new(arg(&args, 2)?), &manifest)?;
        println!(
            "{}",
            serde_json::json!({"paths":manifest.paths.len(),"realisations":manifest.realisations.len()})
        );
        return Ok(());
    }
    if args[0] == "ephemeral-cleanup" {
        return distributed_nix::ephemeral::cleanup(&Node::default());
    }
    if args[0] == "ephemeral-builder" {
        let node = Node::default();
        distributed_nix::ephemeral::prepare(&node, std::path::Path::new(arg(&args, 1)?))?;
        node.serve(true)?;
        return Ok(());
    }
    if args[0] == "collection-server" {
        let config: distributed_nix::rpc::Config =
            serde_json::from_slice(&std::fs::read(arg(&args, 1)?)?)?;
        let token = std::fs::read_to_string(&config.token_file)?;
        let service = distributed_nix::rpc::Service::new(
            config.root,
            token.trim().as_bytes().to_vec(),
            std::env::current_exe()?,
        )?;
        return tokio::runtime::Runtime::new()?.block_on(async {
            let listener = tokio::net::TcpListener::bind(config.listen).await?;
            distributed_nix::rpc::serve(listener, service, async {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => (),
                    _ = term.recv() => (),
                }
            })
            .await
        });
    }
    if args[0] == "collection-client" {
        let config: distributed_nix::rpc::ClientConfig =
            serde_json::from_slice(&std::fs::read(arg(&args, 1)?)?)?;
        return tokio::runtime::Runtime::new()?.block_on(async {
            let client = config.connect().await?;
            let result = match arg(&args, 2)? {
                "bootstrap" => {
                    let snapshot = client.bootstrap().await?;
                    serde_json::json!({"generation":snapshot.generation,"paths":snapshot.paths})
                }
                "publish" => {
                    let root = std::path::PathBuf::from(arg(&args, 3)?);
                    anyhow::ensure!(args.len() > 4, "publish requires output paths");
                    let manifest = distributed_nix::native::dump(&root, &args[4..])?;
                    serde_json::json!({"id":client.publish(&root, &manifest).await?})
                }
                "flush" => serde_json::json!({"published":client.flush(&Node::default()).await?}),
                "release" => {
                    client.release().await?;
                    serde_json::json!({"released":true})
                }
                _ => bail!("unknown collection client operation"),
            };
            println!("{result}");
            Ok(())
        });
    }
    if args[0] == "native-collection" {
        return distributed_nix::native::serve_store(std::path::Path::new(arg(&args, 1)?));
    }
    if args[0] == "native-daemon" {
        return distributed_nix::service::native_connection(
            arg(&args, 1)?,
            arg(&args, 2)?.parse()?,
            arg(&args, 3)? == "trusted",
            args.get(4).is_some_and(|a| a == "runner"),
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
            "service-gc" => c.native_gc_request(&serde_json::from_slice(
                &distributed_nix::util::read_stdin()?,
            )?)?,
            "gc" | "gc-maintenance" => {
                anyhow::ensure!(
                    args.len() == 1 || (args.len() == 2 && args[1] == "--dry-run"),
                    "usage: gc [--dry-run]"
                );
                if args[0] == "gc-maintenance" {
                    c.gc_maintenance(args.len() == 2)?
                } else {
                    c.gc(args.len() == 2)?
                }
            }
            "exec" => {
                let o = c.exec(arg(&args, 1)?.parse()?, arg(&args, 2)?)?;
                print!("{}", String::from_utf8_lossy(&o.stdout));
                eprint!("{}", String::from_utf8_lossy(&o.stderr));
                return Ok(());
            }
            "build" => c.build(arg(&args, 1)?.parse()?, &args[2..])?,
            "publish" => c.publish(arg(&args, 1)?.parse()?, &args[2..])?,
            "reconcile" => c.reconcile(arg(&args, 1)?)?,
            "recover" => c.call_json(arg(&args, 1)?.parse()?, &["recover".into()])?,
            _ => bail!("unknown command; use --help"),
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
