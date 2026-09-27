//! Exercise the user-facing executable, including argv transport and build+publish.
use anyhow::{Result, ensure};
use distributed_nix::{cluster::Cluster, node::ROOT, util::*};
use serde_json::{Value, json};
use std::{
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

#[test]
#[ignore = "requires initialized three-VM Rust lab"]
fn public_build_publishes_and_exec_reads_on_every_node() -> Result<()> {
    let c = Cluster::new()?;
    let runtime = read_json(&c.repo.join("manifest.json"))?;
    let tag = format!(
        "rust-cli-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let expected = "literal 'quotes' and $dollars survive transport";
    let body = format!("printf %s {} > \"$out\"", sh(expected));
    let expr = format!(
        "builtins.derivation {{ name={}; system=\"x86_64-linux\"; builder=(builtins.storePath {})+\"/bin/bash\"; args=[\"-c\" {}]; }}",
        json!(tag),
        runtime["bash"],
        json!(body)
    );
    c.put(0, &format!("{ROOT}/work/{tag}.nix"), expr.as_bytes())?;
    let binary = env!("CARGO_BIN_EXE_distributed-nix");
    let built =
        output(Command::new(binary).args(["build", "0", "--file", &format!("/work/{tag}.nix")]))?;
    let publication: Value = serde_json::from_slice(&built.stdout)?;
    let path = publication["paths"][0].as_str().unwrap();
    ensure!(
        publication["admissions"].as_array().unwrap().len() == 3,
        "missing acknowledgements"
    );
    for n in 0..3 {
        let read =
            output(Command::new(binary).args(["exec", &n.to_string(), &format!("cat {path}")]))?;
        ensure!(
            read.stdout == expected.as_bytes(),
            "wrong output on node {n}"
        );
    }
    let id = publication["batch"].as_str().unwrap();
    let retry = json(Command::new(binary).args(["reconcile", id]))?;
    ensure!(retry["batch"] == id, "reconcile changed identity");
    ensure!(
        !Command::new(binary)
            .args(["exec", "3", "true"])
            .output()?
            .status
            .success(),
        "invalid node accepted"
    );
    durable(
        &c.repo.join("results/distributed-nix/public-cli.json"),
        &json!({"complete":true,"publication":publication,"all_three_nodes_read_expected_bytes":true,"reconcile_idempotent":true,"invalid_node_rejected":true}),
    )
}
