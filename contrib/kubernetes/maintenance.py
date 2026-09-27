#!/usr/bin/env python3
"""Drain ARC before distributed-nix GC; retain the drain after interrupted collection."""
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import time

BASE = Path("/var/lib/distributed-nix")
STATE = BASE / "arc-maintenance.json"
NAMESPACE = "arc-runners"


def run(args, data=None):
    return subprocess.run(args, input=data, text=True, check=True, capture_output=True).stdout


def kube(*args, data=None):
    return run(["kubectl", *args], data)


def apply(value):
    kube("apply", "--server-side", "--field-manager=distributed-nix-maintenance", "-f", "-", data=json.dumps(value))


def durable(value):
    temporary = STATE.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(STATE)
    fd = os.open(BASE, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def remote(host, command):
    config = json.loads(Path("/etc/distributed-nix/cluster.json").read_text())
    return run(["ssh", "-i", config["identity_file"], "-o", "BatchMode=yes",
                "-o", "StrictHostKeyChecking=yes", "-o", f'UserKnownHostsFile={config["known_hosts_file"]}',
                "-o", "ConnectTimeout=5", "-o", "ServerAliveInterval=2", "-o", "ServerAliveCountMax=3",
                f"root@{host}", command])


def wait_for(predicate, seconds=900):
    deadline = time.monotonic() + seconds
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError("Drain incomplete; admission remains blocked. Rerun maintenance.")
        time.sleep(2)


def drain(automatic=False, seconds=900):
    if STATE.exists():
        state = json.loads(STATE.read_text())
    else:
        sets = json.loads(kube("get", "autoscalingrunnersets", "-n", NAMESPACE, "-o", "json"))["items"]
        if not sets:
            raise RuntimeError("No ARC scale sets found")
        state = {"sets": {s["metadata"]["name"]: {
            "minRunners": s["spec"]["minRunners"], "maxRunners": s["spec"]["maxRunners"],
        } for s in sets}, "phase": "draining", "automatic": automatic}
        durable(state)
    apply({"apiVersion": "v1", "kind": "ResourceQuota",
           "metadata": {"name": "distributed-nix-maintenance", "namespace": NAMESPACE},
           "spec": {"hard": {"pods": "0"}}})
    wait_for(lambda: json.loads(kube("get", "resourcequota", "distributed-nix-maintenance", "-n", NAMESPACE,
                                    "-o", "json")).get("status", {}).get("hard", {}).get("pods") == "0")
    for name in state["sets"]:
        kube("patch", "autoscalingrunnerset", name, "-n", NAMESPACE, "--type=merge",
             "-p", json.dumps({"spec": {"minRunners": 0, "maxRunners": 0}}))
    print("Waiting for existing runner jobs to finish", flush=True)
    wait_for(lambda: not json.loads(kube("get", "pods", "-n", NAMESPACE, "-o", "json"))["items"], seconds=seconds)
    config = json.loads(Path("/etc/distributed-nix/cluster.json").read_text())
    for host in config["nodes"]:
        containers = json.loads(remote(host, "k3s crictl ps -o json"))["containers"]
        if any(c.get("labels", {}).get("io.kubernetes.pod.namespace") == NAMESPACE for c in containers):
            raise RuntimeError(f"Runner containers remain on {host}")
        remote(host, "test -f /var/lib/distributed-nix/gc-maintenance-only")
    state["phase"] = "drained"
    durable(state)
    return state


def resume(state):
    config = json.loads(Path("/etc/distributed-nix/cluster.json").read_text())
    for host in config["nodes"]:
        remote(host, "test ! -e /var/lib/distributed-nix/gc-active.json")
    for name, limits in state["sets"].items():
        kube("patch", "autoscalingrunnerset", name, "-n", NAMESPACE, "--type=merge",
             "-p", json.dumps({"spec": limits}))
    kube("delete", "resourcequota", "distributed-nix-maintenance", "-n", NAMESPACE, "--ignore-not-found")
    STATE.rename(BASE / "arc-maintenance-completed.json")
    fd = os.open(BASE, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    print("ARC scheduling restored", flush=True)


def automatic_gc():
    if STATE.exists():
        state = json.loads(STATE.read_text())
        if not state.get("automatic", False):
            print("Manual maintenance is active; automatic GC skipped", flush=True)
            return
    else:
        threshold = int(os.environ.get("CIBOX_GC_MIN_FREE_PERCENT", "20"))
        if not 1 <= threshold <= 100:
            raise ValueError("CIBOX_GC_MIN_FREE_PERCENT must be between 1 and 100")
        config = json.loads(Path("/etc/distributed-nix/cluster.json").read_text())
        pressure = False
        for host in config["nodes"]:
            size, available = map(int, remote(host, "df -B1 --output=size,avail /srv").splitlines()[-1].split())
            pressure |= available * 100 < size * threshold
        if not pressure:
            print("No shared-store disk pressure; automatic GC skipped", flush=True)
            return
    try:
        state = drain(automatic=True, seconds=10)
    except TimeoutError:
        # No collection has started. Resume still checks every node for a GC epoch.
        resume(json.loads(STATE.read_text()))
        print("Runner arrived during drain; automatic GC deferred", flush=True)
        return
    state["phase"] = "collecting"
    durable(state)
    print(run(["distributed-nix", "gc-maintenance"]), flush=True)
    gc_git_caches()
    resume(state)


def gc_git_caches():
    config = json.loads(Path("/etc/distributed-nix/cluster.json").read_text())
    for host in config["nodes"]:
        remote(host, "cibox-git-cache gc")


def main():
    if os.geteuid() != 0:
        raise PermissionError("Run on cibox-0 as root")
    os.environ["KUBECONFIG"] = "/etc/rancher/k3s/k3s.yaml"
    command = sys.argv[1:]
    if command not in (["drain"], ["resume"], ["gc"], ["gc", "--dry-run"], ["auto-gc"]):
        raise ValueError("Usage: cibox-maintenance drain | resume | gc [--dry-run] | auto-gc")
    with (BASE / "arc-maintenance.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if command == ["auto-gc"]:
            automatic_gc()
            return
        if command == ["resume"]:
            resume(json.loads(STATE.read_text()))
            return
        state = drain()
        if command[0] == "gc":
            result = run(["distributed-nix", "gc-maintenance", *command[1:]])
            print(result, flush=True)
            if "--dry-run" not in command:
                gc_git_caches()
            resume(state)


if __name__ == "__main__":
    main()
