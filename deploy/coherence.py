"""Native admission must invalidate cached NFS names before binding them."""
import hashlib
import json
import shlex
import uuid


def check_recreated_path(execute, store, peer):
    fixture = '/.e2e-coherence-' + uuid.uuid4().hex
    source = '/srv/distributed-nix/origin' + fixture
    lower = '/var/lib/distributed-nix/lower' + fixture
    scratch = '/work' + fixture
    path = '/nix/store/' + '0' * 32 + '-coherence-probe'
    execute(store, 'mkdir', '-p', source + path, source + '/.distributed-nix-publications')
    execute(store, 'bash', '-ec', 'echo 0 > "$1/value"', 'fixture', source + path)
    try:
        for i in range(10):
            execute(store, 'bash', '-ec', 'mkdir "$1/next"; echo "$2" > "$1/next/value"',
                    'fixture', source, str(i + 1))
            nar_hash = execute(store, 'nix', 'hash', 'path', source + '/next').stdout.strip()
            nar_size = int(execute(store, 'bash', '-ec',
                                  'nix-store --dump "$1/next" | wc -c', 'fixture', source).stdout)
            manifest = {'version': 1, 'roots': [path], 'paths': {path: {
                'narHash': nar_hash, 'narSize': nar_size, 'references': [], 'registrationTime': 1,
                'ca': None, 'deriver': None, 'signatures': [], 'ultimate': False,
                'storeDir': '/nix/store', 'version': 1}}}
            encoded = json.dumps(manifest, sort_keys=True, separators=(',', ':'))
            batch = hashlib.sha256(encoded.encode()).hexdigest()
            # Prime the old positive lookup, then replace it on the server.
            assert execute(peer, 'cat', lower + path + '/value').stdout.strip() == str(i)
            execute(store, 'bash', '-ec', '''
                mv "$1$2" "$1/old"
                mv "$1/next" "$1$2"
                rm -rf "$1/old"
                printf %s "$3" > "$1/.distributed-nix-publications/$4.json"
            ''', 'fixture', source, path, encoded, batch)
            local = scratch + '/' + str(i)
            execute(peer, 'mkdir', '-p', local + '/base/lower', local + '/worker/nix/store',
                    local + '/worker/nix/var/nix')
            execute(peer, 'bash', '-ec', 'printf %s "$2" > "$1/manifest.json"',
                    'fixture', local, encoded)
            script = f'''
                mount --bind {shlex.quote(lower)} {shlex.quote(local + '/base/lower')}
                mount --rbind {shlex.quote(local + '/base')} /var/lib/distributed-nix
                mount --bind {shlex.quote(local + '/worker')} /srv/distributed-nix/worker
                export NIX_CONFIG='experimental-features = nix-command flakes ca-derivations
                build-users-group ='
                distributed-nix node admit {shlex.quote(local + '/manifest.json')}
                nix-store --store 'local?root=/srv/distributed-nix/worker' --verify-path {path}
                cat /srv/distributed-nix/worker{path}/value
            '''
            result = execute(peer, 'unshare', '--mount', '--propagation', 'private',
                             'bash', '-euc', script)
            assert result.stdout.splitlines()[-1] == str(i + 1), result.stdout
    finally:
        execute(store, 'rm', '-rf', source)
        execute(peer, 'rm', '-rf', scratch)
