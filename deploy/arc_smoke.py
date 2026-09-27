#!/usr/bin/env python3
"""Exercise the unprivileged ARC image and real runner failure cleanup on Kubernetes."""
import argparse
import json
import subprocess
import time
import uuid

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--namespace', default='nix-builders')
p.add_argument('--release', default='e2e')
a = p.parse_args()
base = a.release + '-nix'
name = 'arc-smoke-' + uuid.uuid4().hex[:12]

def kube(*args, value=None):
    r = subprocess.run(['kubectl', '-n', a.namespace, *args],
                       input=json.dumps(value) if value else None,
                       text=True, capture_output=True, check=True)
    return r.stdout

def apply(value):
    kube('apply', '-f', '-', value=value)

store = json.loads(kube('get', 'pod', base + '-store-0', '-o', 'json'))
secret = next(v['secret']['secretName'] for v in store['spec']['volumes'] if v['name'] == 'token')
image = store['spec']['containers'][0]['image']
builders = [base + '-builder-' + str(i) for i in range(3)]
config = {'builders': [f'{b}.{base}-builders.{a.namespace}.svc.cluster.local:9840' for b in builders],
          'token_file': '/run/secrets/controller/token'}
try:
    apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': name},
           'data': {'arc-client.json': json.dumps(config)}})
    apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'labels': {
        'app.kubernetes.io/name': 'distributed-nix', 'app.kubernetes.io/instance': a.release}},
        'spec': {'restartPolicy': 'Never', 'automountServiceAccountToken': False,
                 'securityContext': {'runAsUser': 1001, 'runAsGroup': 1001, 'fsGroup': 1001, 'runAsNonRoot': True},
                 'containers': [{'name': 'client', 'image': image, 'imagePullPolicy': 'IfNotPresent',
                    'command': ['/bin/distributed-nix', 'arc-client'],
                    'securityContext': {'readOnlyRootFilesystem': True, 'allowPrivilegeEscalation': False,
                                        'capabilities': {'drop': ['ALL']}},
                    'env': [{'name': 'HOME', 'value': '/run/distributed-nix-arc-client'},
                            {'name': 'DISTRIBUTED_NIX_POD_UID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.uid'}}},
                            {'name': 'ACTIONS_RUNNER_INPUT_JITCONFIG', 'value': 'e30='}],
                    'volumeMounts': [
                        {'name': 'config', 'mountPath': '/etc/distributed-nix/arc-client.json', 'subPath': 'arc-client.json'},
                        {'name': 'token', 'mountPath': '/run/secrets/controller', 'readOnly': True},
                        {'name': 'runtime', 'mountPath': '/run/distributed-nix-arc-client'}]}],
                 'volumes': [{'name': 'config', 'configMap': {'name': name}},
                             {'name': 'token', 'secret': {'secretName': secret, 'defaultMode': 288}},
                             {'name': 'runtime', 'emptyDir': {}}]}})
    deadline = time.monotonic() + 180
    while True:
        pod = json.loads(kube('get', 'pod', name, '-o', 'json'))
        if pod['status']['phase'] in ['Failed', 'Succeeded']:
            break
        if time.monotonic() > deadline:
            raise TimeoutError('ARC smoke pod did not finish')
        time.sleep(1)
    log = kube('logs', name)
    assert 'Attached to ' in log, log
    assert 'Not configured' in log, log
    assert pod['status']['containerStatuses'][0]['state']['terminated']['exitCode'] == 1, log
    uid = pod['metadata']['uid']
    owners = []
    for builder in builders:
        script = '''import sqlite3, pathlib, json, sys
id = sys.argv[1]
r = json.loads(pathlib.Path('/run/distributed-nix-arc.json').read_text())
n = sqlite3.connect(r['database']).execute('SELECT count(*) FROM starts WHERE id=?', (id,)).fetchone()[0]
if n:
 assert not (pathlib.Path(r['work']) / id).exists()
 assert not (pathlib.Path(r['cgroup']) / ('arc-' + id)).exists()
 assert not pathlib.Path(r['poison']).exists()
print(n)
'''
        count = int(kube('exec', builder, '--', 'python3', '-c', script, uid))
        if count:
            owners.append(builder)
    assert len(owners) == 1, owners
    print(json.dumps({'unprivileged_arc_attachment': 'passed', 'runner_failure_cleanup': 'passed',
                      'builder': owners[0], 'client_uid': uid, 'host_paths': 0}))
finally:
    kube('delete', 'pod,configmap', name, '--ignore-not-found', '--wait=true')
