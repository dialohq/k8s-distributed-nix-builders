#!/usr/bin/env python3
"""Destructive integration test for a disposable Helm installation (stdlib only)."""
import argparse
import concurrent.futures
import json
import subprocess
import time
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--namespace', default='nix-builders')
parser.add_argument('--release', default='nix-builders')
args = parser.parse_args()
base = args.release + '-nix'
store = base + '-store-0'
builders = [base + '-builder-' + str(i) for i in range(3)]
report = {}


def passed(name):
    report[name] = "passed"
    print(name + ": passed", flush=True)


def kubectl(*words, check=True, timeout=300):
    result = subprocess.run(['kubectl', '-n', args.namespace, *words], text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f'{words}: {result.stderr}\n{result.stdout}')
    return result


def execute(pod, *words, **kw):
    return kubectl('exec', pod, '--', *words, **kw)


def eventually(fn, timeout=180):
    deadline = time.monotonic() + timeout
    error = None
    while time.monotonic() < deadline:
        try:
            return fn()
        except (AssertionError, RuntimeError) as exc:
            error = exc
            time.sleep(2)
    raise RuntimeError(f'timed out: {error}')


def ready():
    pods = json.loads(kubectl('get', 'pods', '-l', f'app.kubernetes.io/instance={args.release}', '-o', 'json').stdout)
    running = [p for p in pods['items'] if p['metadata']['name'] in [store, *builders]]
    assert len(running) == 4
    for pod in running:
        assert any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', []))
        spec = pod['spec']
        assert not spec.get('hostPID') and not spec.get('hostNetwork')
        assert all('hostPath' not in v for v in spec['volumes'])
    status = json.loads(execute(store, "distributed-nix", "status").stdout)
    assert len(status) == 4
    return pods


def replace(pod):
    old = json.loads(kubectl('get', 'pod', pod, '-o', 'json').stdout)['metadata']['uid']
    kubectl('delete', 'pod', pod, '--wait=true', '--timeout=180s')
    def renewed():
        current = json.loads(kubectl('get', 'pod', pod, '-o', 'json').stdout)
        assert current['metadata']['uid'] != old
        return ready()
    eventually(renewed, timeout=300)


def build(pod, expression, no_build=False):
    flags = ['--max-jobs', '0'] if no_build else []
    return execute(pod, 'nix', 'build', '--impure', '--no-link', '--print-out-paths',
                   '--option', 'substituters', '', *flags, '--expr', expression).stdout.strip()


def expression(name, ca=False, delay=0):
    return '''let r = builtins.fromJSON (builtins.readFile /etc/distributed-nix/runtime.json); in
    derivation { name = "%s"; system = builtins.currentSystem;
      builder = "${builtins.storePath r.bash}/bin/bash"; PATH = "${builtins.storePath r.coreutils}/bin";
      args = [ "-ec" "sleep %s; mkdir -p $out; echo %s > $out/result" ];
      %s
    }''' % (name, delay, name, ' __contentAddressed = true; outputHashMode = "recursive"; outputHashAlgo = "sha256";' if ca else '')


started = time.monotonic()
eventually(ready, timeout=600)
status = json.loads(execute(store, 'distributed-nix', 'status').stdout)
assert len(status) == 4
name = 'helm-e2e-' + uuid.uuid4().hex[:12]
plain = expression(name)
output = build(builders[0], plain)
for pod in builders[1:]:
    assert eventually(lambda pod=pod: build(pod, plain, no_build=True)) == output
    assert execute(pod, 'cat', output + '/result').stdout.strip() == name
    filesystem = execute(pod, 'findmnt', '-n', '-T', output, '-o', 'FSTYPE').stdout.strip()
    assert filesystem == 'nfs4', filesystem
passed('native_build_and_shared_reuse')
ca = expression(name + '-ca', ca=True)
ca_output = build(builders[0], ca)
for pod in builders[1:]:
    assert eventually(lambda pod=pod: build(pod, ca, no_build=True)) == ca_output
passed('content_addressed_reuse')
replace(builders[2])
assert build(builders[2], plain, no_build=True) == output
passed('builder_replacement')
replace(store)
assert execute(builders[1], 'cat', output + '/result').stdout.strip() == name
assert build(builders[1], ca, no_build=True) == ca_output
passed('store_replacement_and_nfs_reconnect')

# A missing member must stop collection before deleting shared bytes.
kubectl('scale', 'statefulset', base + '-builder', '--replicas=2')
try:
    kubectl('wait', '--for=delete', 'pod/' + builders[2], '--timeout=180s')
    failed = execute(store, 'distributed-nix', 'gc', check=False)
    assert failed.returncode != 0, failed.stdout
    assert execute(builders[1], 'cat', output + '/result').stdout.strip() == name
finally:
    kubectl('scale', 'statefulset', base + '-builder', '--replicas=3')
eventually(ready, timeout=300)
passed('missing_peer_fails_closed')

failed = execute(store, 'env', 'DISTRIBUTED_NIX_FAILPOINT=online-after-barriers',
                 'distributed-nix', 'gc', check=False)
assert failed.returncode == 137, (failed.returncode, failed.stderr)
execute(store, "test", "-f", "/var/lib/distributed-nix/online-master.json")
execute(store, 'distributed-nix', 'gc')
passed('coordinator_crash_resume')

with concurrent.futures.ThreadPoolExecutor() as pool:
    future = pool.submit(build, builders[1], expression(name + '-active', delay=15))
    time.sleep(3)
    execute(store, 'distributed-nix', 'gc')
    live = future.result(timeout=180)
    assert execute(builders[1], 'cat', live + '/result').stdout.strip() == name + '-active'
passed('build_during_gc')

# All pods which touched the first output are retired, then GC can reclaim it.
for pod in builders:
    replace(pod)
execute(store, 'distributed-nix', 'gc')
assert execute(store, 'test', '-e', '/srv/distributed-nix/origin' + output, check=False).returncode != 0
passed('unrooted_output_collected')
report['elapsed_seconds'] = round(time.monotonic() - started, 2)
print(json.dumps(report, indent=2), flush=True)
