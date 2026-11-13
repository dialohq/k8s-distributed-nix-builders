"""Run inside deploy/native-build-vm.nix's disposable libvirt guest, as root.

Uses stock Nix/SSH, independent stores, the shipped forced-command config, and
the real publication/online-GC coordinator. No Kubernetes or host writes.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import statistics
import subprocess
import time
import uuid


WORKER = Path('/srv/distributed-nix/worker')
ORIGIN = Path('/srv/distributed-nix/origin')
BASE = Path('/var/lib/distributed-nix')
CLIENT = 'local?root=/var/lib/proof-client'
REMOTE = 'ssh-ng://root@127.0.0.1:2222?ssh-key=/run/proof/id_ed25519&max-connections=1'
SSHOPTS = ('-oUserKnownHostsFile=/run/proof/known_hosts '
           '-oStrictHostKeyChecking=yes -oIdentitiesOnly=yes -oBatchMode=yes')
os.environ['NIX_SSHOPTS'] = SSHOPTS
os.environ['NIX_CONFIG'] = 'experimental-features = nix-command flakes ca-derivations\n'
RUNTIME = json.loads(Path('/etc/distributed-nix/runtime.json').read_text())
RESULTS = []


def run(*args, check=True, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if check and result.returncode:
        raise RuntimeError(f'{shlex.join(args)}: {result.returncode}\n{result.stdout}\n{result.stderr}')
    return result


def wait_for(predicate, description, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.02)
    raise AssertionError('timed out: ' + description)


def record(case, **facts):
    row = {'case': case, **facts}
    RESULTS.append(row)
    print(json.dumps(row), flush=True)
    Path('/exchange/results.json').write_text(json.dumps(RESULTS, indent=2) + '\n')


def native_roots(path):
    return list((WORKER / 'nix/var/nix/gcroots/distributed-nix-clients').glob('client-*/' + Path(path).name))


def valid(root, path):
    return run('nix', 'path-info', '--store', 'local?root=' + str(root), path, check=False).returncode == 0


def gc():
    report = json.loads(run('distributed-nix', 'gc').stdout)
    Path('/exchange/gc-' + report['id'] + '.json').write_text(json.dumps(report, indent=2))
    return {'id': report['id'], 'phase': report['phase'], 'bytes_freed': report['bytes_freed']}


def publish(path):
    # Both path and realisation outboxes independently pin outputs. Exercise the
    # production publisher rather than manually dropping either safety root.
    with open('/exchange/publisher.log', 'a') as log:
        child = subprocess.Popen(['distributed-nix', 'publisher'], stdout=log, stderr=log)
        try:
            wait_for(lambda: json.loads(run('distributed-nix', 'node', 'outbox').stdout) == [] and
                     json.loads(run('distributed-nix', 'node', 'ca-outbox').stdout) is None,
                     'publication and both acknowledgements')
        finally:
            child.terminate()
            child.wait(timeout=30)
    assert valid(ORIGIN, path)


def leases_released(roots):
    for root in roots:
        lease = BASE / 'client-roots' / root.parent.name / 'lease'
        if not lease.exists():
            continue
        with lease.open('r') as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
    return True


def expression(script, name='proof'):
    return ('builtins.derivation { name = ' + json.dumps(name + '-' + uuid.uuid4().hex) + '; '
            'system = "x86_64-linux"; builder = (builtins.storePath ' + json.dumps(RUNTIME['bash']) + ') + "/bin/bash"; '
            'args = ["-euc" ' + json.dumps(script) + ']; '
            'PATH = (builtins.storePath ' + json.dumps(RUNTIME['coreutils']) + ') + "/bin"; }')


def build_command(expr):
    return ['nix', 'build', '--impure', '--store', CLIENT, '--expr', expr, '--no-link', '--print-out-paths',
            '--max-jobs', '0', '--builders', REMOTE + ' x86_64-linux - 2 1',
            '--option', 'substituters', '']


def build(expr):
    start = time.monotonic()
    result = run(*build_command(expr))
    path = result.stdout.strip()
    assert valid(WORKER, path), result.stderr
    assert Path('/var/lib/proof-client' + path).exists()
    return path, time.monotonic() - start, result.stderr


def processes(pid):
    tree = {}
    for line in run('ps', '-eo', 'pid=,ppid=').stdout.splitlines():
        child, parent = map(int, line.split())
        tree.setdefault(parent, []).append(child)
    def descendants(parent):
        return [child for child in tree.get(parent, []) for child in [child, *descendants(child)]]
    return [pid, *descendants(pid)]


def copy_while_gc(path, suspend=False):
    destination = '/var/lib/proof-copy-' + uuid.uuid4().hex
    env = dict(os.environ, NIX_SSHOPTS=SSHOPTS + ' -l 8192')
    old = set(native_roots(path))
    with open('/exchange/copy.log', 'w') as log:
        child = subprocess.Popen(['nix', 'copy', '--from', REMOTE, '--to', 'local?root=' + destination,
                                  '--no-check-sigs', path], stdout=log, stderr=log, env=env)
        frozen = []
        try:
            # Wait for this new protocol connection to pin the source, then
            # backpressure Nix and all its SSH children, not a mock transport.
            roots = wait_for(lambda: set(native_roots(path)) - old, 'copy root acquired')
            partial = wait_for(lambda: next((p.stat().st_size for p in Path(destination).rglob('payload')
                                            if 0 < p.stat().st_size < 16 * 1024 * 1024), None),
                               'partial NAR received')
            frozen = processes(child.pid)
            for pid in frozen:
                os.kill(pid, signal.SIGSTOP)
            report = gc()
            assert all(root.is_symlink() for root in roots)
            assert valid(WORKER, path) and valid(ORIGIN, path)
            if suspend:
                start = time.monotonic()
                # While SSH clients cannot answer keepalives, the server must
                # close the dead connection and release the native lease.
                wait_for(lambda: leases_released(roots),
                         'server retired suspended connection', timeout=100)
                gc()
                assert not any(root.is_symlink() for root in roots)
                record('suspension releases roots', seconds=time.monotonic() - start)
            for pid in reversed(frozen):
                try:
                    os.kill(pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            frozen = []
            code = child.wait(timeout=90)
            if suspend:
                assert code != 0, 'suspended copy unexpectedly survived server disconnect'
            else:
                assert code == 0, Path('/exchange/copy.log').read_text()
                copied = Path(destination + path + '/payload')
                original = WORKER / path.lstrip('/') / 'payload'
                assert hashlib.sha256(copied.read_bytes()).digest() == hashlib.sha256(original.read_bytes()).digest()
                record('copy survives online GC', bytes=copied.stat().st_size,
                       bytes_received_before_gc=partial, gc=report)
            wait_for(lambda: leases_released(roots), 'copy worker exited')
        finally:
            for pid in frozen:
                try:
                    os.kill(pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            if child.poll() is None:
                child.kill()
                child.wait()


def held_build(disconnect=False):
    barrier = '/proof-hold/' + uuid.uuid4().hex
    hold = WORKER / ('work' + barrier)
    hold.mkdir(mode=0o777, exist_ok=True)
    hold.chmod(0o777)
    os.mkfifo(hold / 'release', 0o666)
    (hold / 'release').chmod(0o666)
    expr = expression(f'echo ready > {barrier}/ready; read -r gate < {barrier}/release; '
                      'mkdir "$out"; echo "$gate" > "$out/marker"', 'held')
    logpath = '/exchange/held-disconnect.log' if disconnect else '/exchange/held.log'
    with open(logpath, 'w+') as log:
        child = subprocess.Popen(build_command(expr), stdout=log, stderr=log)
        frozen = []
        try:
            wait_for(lambda: (hold / 'ready').exists() or child.poll() is not None, 'build FIFO barrier')
            assert child.poll() is None, Path(logpath).read_text()
            client_pids = set(processes(child.pid))
            build_pids = [int(line.split()[0]) for line in run('ps', '-eo', 'pid=,args=').stdout.splitlines()
                          if barrier in line and int(line.split()[0]) not in client_pids]
            assert build_pids
            roots = list((WORKER / 'nix/var/nix/gcroots/distributed-nix-clients').glob('client-*/*held*.drv'))
            assert roots
            report = gc()
            assert child.poll() is None
            if disconnect:
                frozen = processes(child.pid)
                for pid in frozen:
                    os.kill(pid, signal.SIGSTOP)
                start = time.monotonic()
                wait_for(lambda: leases_released(roots), 'disconnected build lease release', timeout=100)
                assert all(not Path('/proc/' + str(pid)).exists() for pid in build_pids)
                gc()
                assert not any(root.is_symlink() for root in roots)
                for pid in reversed(frozen):
                    try:
                        os.kill(pid, signal.SIGCONT)
                    except ProcessLookupError:
                        pass
                frozen = []
                assert child.wait(timeout=30) != 0
                record('suspended build cancelled and children reaped', seconds=time.monotonic() - start)
            else:
                (hold / 'release').write_text('released-after-gc\n')
                assert child.wait(timeout=60) == 0, Path(logpath).read_text()
                path = Path(logpath).read_text().splitlines()[-1]
                assert Path('/var/lib/proof-client' + path + '/marker').read_text() == 'released-after-gc\n'
                record('build survives online GC', path=path, gc=report)
        finally:
            for pid in frozen:
                try:
                    os.kill(pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            if child.poll() is None:
                child.kill()
                child.wait()


def incremental_build_after_gc():
    a = expression('mkdir "$out"; printf incremental-a > "$out/marker"', 'incremental-a')
    a_path, _, _ = build(a)
    info = json.loads(run('nix', 'path-info', '--store', CLIENT, '--json', a_path).stdout)[a_path]
    assert any(sig.startswith('native-proof-1:') for sig in info['signatures'])
    run('nix', 'store', 'verify', '--store', CLIENT, '--no-contents', '--sigs-needed', '1',
        '--option', 'trusted-public-keys', Path('/run/proof-signing.pub').read_text().strip(), a_path)
    root = '/var/lib/proof-incremental-a-' + uuid.uuid4().hex
    run('nix-store', '--store', CLIENT, '--add-root', root, '--realise', a_path)
    assert Path(root).is_symlink()

    def b():
        return 'let a = (' + a + '); in ' + expression(
            'mkdir "$out"; cat ${a}/marker > "$out/marker"', 'incremental-b')

    before, _, _ = build(b())
    assert Path('/var/lib/proof-client' + before + '/marker').read_text() == 'incremental-a'
    after = b()
    drv = json.loads(run('nix', 'derivation', 'show', '--impure', '--store', CLIENT, '--expr', after).stdout)
    assert any('incremental-a' in p for d in drv['derivations'].values() for p in d['inputs']['drvs'])
    Path('/exchange/incremental-b-derivation.json').write_text(json.dumps(drv, indent=2))
    publish(a_path)
    report = gc()
    assert not valid(WORKER, a_path) and not valid(ORIGIN, a_path)
    assert valid(Path('/var/lib/proof-client'), a_path) and Path(root).is_symlink()
    start = time.monotonic()
    result = run(*build_command(after), check=False, timeout=45)
    Path('/exchange/incremental-build-after-gc.log').write_text(result.stdout + result.stderr)
    record('ordinary derivation-context incremental build after remote GC', a=a_path,
           client_root=root, pre_gc_b_succeeded=True, gc=report,
           post_gc_b_status=result.returncode, seconds=time.monotonic() - start)
    assert result.returncode == 0, result.stderr
    path = result.stdout.strip()
    assert Path('/var/lib/proof-client' + path + '/marker').read_text() == 'incremental-a'


def cache_substitution():
    probe = RUNTIME['cacheProbe']
    run('nix', 'copy', '--from', 'https://cache.nixos.org', '--to', CLIENT, probe, timeout=120)
    closure = run('nix', 'path-info', '--store', CLIENT, '--recursive', probe).stdout.splitlines()
    registration = run('nix-store', '--store', CLIENT, '--dump-db', *closure).stdout
    run('nix-store', '--store', CLIENT, '--load-db', input=registration)
    info = json.loads(run('nix', 'path-info', '--store', CLIENT, '--json', probe).stdout)[probe]
    assert not info['signatures']
    if valid(WORKER, probe):
        publish(probe)
        gc()
    assert not valid(WORKER, probe)
    expr = expression('mkdir "$out"; ${builtins.storePath ' + probe + '}/bin/hello > "$out/marker"', 'cached-input')
    denied = run(*build_command(expr)[:-3], '--option', 'builders-use-substitutes', 'false', check=False, timeout=45)
    assert denied.returncode and 'lacks a signature by a trusted key' in denied.stderr
    start = time.monotonic()
    result = run(*build_command(expr)[:-3], '--option', 'builders-use-substitutes', 'true', timeout=120)
    path = result.stdout.strip()
    assert Path('/var/lib/proof-client' + path + '/marker').read_text() == 'Hello, world!\n'
    info = json.loads(run('nix', 'path-info', '--store', REMOTE, '--json', probe).stdout)[probe]
    assert any(sig.startswith('cache.nixos.org-1:') for sig in info['signatures'])
    Path('/exchange/cache-substitution.log').write_text(result.stderr)
    record('stock QueryValidPaths substitutes unsigned image input', input=probe,
           client_signatures=0, without_substitution_status=denied.returncode,
           with_substitution_status=result.returncode, seconds=time.monotonic() - start)


def main():
    run('systemctl', 'is-active', 'native-proof-rpc', 'native-proof-ssh')
    # closureInfo's disposable source database has no cache signatures.
    run('nix', 'copy', '--no-check-sigs', '--to', CLIENT, RUNTIME['bash'], RUNTIME['coreutils'])
    ping = json.loads(run('nix', 'store', 'info', '--store', REMOTE, '--json').stdout)
    assert ping['trusted'] is False, ping
    wrong = REMOTE.replace('/id_ed25519', '/wrong_key')
    denied = run('nix', 'store', 'info', '--store', wrong, check=False)
    assert denied.returncode and 'Permission denied' in denied.stderr
    shell = run('ssh', '-p', '2222', '-i', '/run/proof/id_ed25519', *shlex.split(SSHOPTS),
                'root@127.0.0.1', 'touch /run/escaped', input='', check=False)
    assert shell.returncode and not Path('/run/escaped').exists()
    forbidden = run('nix', 'store', 'gc', '--store', REMOTE, check=False)
    assert forbidden.returncode and 'shared store GC requires the administrator coordinator' in forbidden.stderr
    build(expression('test ! -e /run/secrets/nix-signing/key; mkdir "$out"; echo sandboxed > "$out/marker"'))
    record('authentication and confinement', nix=run('nix', '--version').stdout.strip(),
           trusted=ping['trusted'], wrong_key_status=denied.returncode,
           shell_status=shell.returncode, gc_status=forbidden.returncode)

    source_file = Path('/var/lib/proof-source-' + uuid.uuid4().hex)
    source_file.write_text('native source input\n')
    source = run('nix', 'store', 'add-file', '--store', CLIENT, str(source_file)).stdout.strip()
    path, _, _ = build(expression('mkdir "$out"; cat ${builtins.storePath ' + source + '} > "$out/marker"'))
    assert Path('/var/lib/proof-client' + path + '/marker').read_text() == source_file.read_text()
    source_file.unlink()
    record('content-addressed input uploaded', source=source, path=path)

    cold, warm = [], []
    for _ in range(3):
        expr = expression('mkdir "$out"; printf remote > "$out/marker"')
        path, seconds, stderr = build(expr)
        cold.append(seconds)
        assert Path('/var/lib/proof-client' + path + '/marker').read_text() == 'remote'
        assert "on 'ssh-ng://" in stderr, stderr
        warm.append(build(expr)[1])
    record('ordinary nix build and copy-back', cold_seconds=cold, cold_median=statistics.median(cold),
           local_reuse_seconds=warm, local_reuse_median=statistics.median(warm))

    held_build()
    held_build(disconnect=True)
    path, _, _ = build(expression('mkdir "$out"; dd if=/dev/urandom of="$out/payload" bs=1M count=16 status=none', 'copy'))
    publish(path)
    copy_while_gc(path)
    gc()
    assert not valid(WORKER, path) and not valid(ORIGIN, path)
    record('disconnected roots reclaimed without pod replacement', path=path)

    local = run(*build_command(expression('mkdir "$out"; echo unsigned > "$out/marker"', 'unsigned')),
                '--max-jobs', '1', '--builders', '').stdout.strip()
    unsigned = run('nix', 'copy', '--no-check-sigs', '--from', CLIENT, '--to', REMOTE, local, check=False)
    assert unsigned.returncode and 'lacks a signature by a trusted key' in unsigned.stderr
    record('unsigned input-addressed import denied', status=unsigned.returncode)

    path, _, _ = build(expression('mkdir "$out"; dd if=/dev/urandom of="$out/payload" bs=1M count=16 status=none', 'suspend'))
    publish(path)
    copy_while_gc(path, suspend=True)
    gc()
    assert not valid(WORKER, path) and not valid(ORIGIN, path)
    cache_substitution()
    incremental_build_after_gc()
    record('all native-build cases passed')


if __name__ == '__main__':
    main()
