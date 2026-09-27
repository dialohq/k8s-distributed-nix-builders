#!/bin/bash
set -euo pipefail
role=${DISTRIBUTED_NIX_ROLE:?}
mkdir -p /data/{state,worker,origin,nfs} /srv/distributed-nix /var/lib /run/distributed-nix-runner /etc/nix /work
mkdir -p /var/lib/distributed-nix /srv/distributed-nix/{worker,origin}
mount --bind /data/state /var/lib/distributed-nix
mount --bind /data/worker /srv/distributed-nix/worker
mount --bind /data/origin /srv/distributed-nix/origin
mkdir -p /var/lib/distributed-nix/lower
# All mounts remain in this pod's mount namespace.
exec 9>/data/instance.lock
flock -n 9 || { echo 'Persistent volume already has an owner' >&2; exit 1; }
python3 - <<'PY'
import json,os
value=json.load(open('/etc/distributed-nix/participants.json'))
value['index']=0 if os.environ['DISTRIBUTED_NIX_ROLE']=='store' else int(os.environ['HOSTNAME'].rsplit('-',1)[1])+1
value['pod_uid']=None if value['index']==0 else os.environ['DISTRIBUTED_NIX_POD_UID']
json.dump(value,open('/run/distributed-nix-config.json','w'))
PY
export DISTRIBUTED_NIX_ONLINE_CONFIG=/run/distributed-nix-config.json
mapfile -t seed < <(python3 -c 'import json; print("\n".join(json.load(open("/etc/distributed-nix/runtime.json"))["seed"]))')
pids=()
stop() { trap - EXIT TERM INT; kill "${pids[@]}" 2>/dev/null || true; wait || true; }
trap stop EXIT TERM INT
if [ "$role" = store ]; then
  nix --option build-users-group "" --store local copy --no-check-sigs --to /srv/distributed-nix/origin "${seed[@]}"
  distributed-nix node prepare-origin
  mkdir -p /srv/distributed-nix/origin/.distributed-nix-publications
  mount --bind /srv/distributed-nix/origin /var/lib/distributed-nix/lower
  mount -o remount,bind,ro /var/lib/distributed-nix/lower
  mkdir -p /var/run/ganesha /var/lib/nfs
  ln -sfn /data/nfs /var/lib/nfs/ganesha
  ganesha.nfsd -F -f /etc/ganesha/ganesha.conf -L /dev/stderr & pids+=("$!")
else
  mount -t nfs4 -o ro,vers=4.1,proto=tcp,port=2049,hard,timeo=10,retrans=2,lookupcache=positive,actimeo=1,nosharecache "${DISTRIBUTED_NIX_STORE_HOST:?}:/collection" /var/lib/distributed-nix/lower
fi
nix --option build-users-group "" --store local copy --no-check-sigs --to /srv/distributed-nix/worker "${seed[@]}"
distributed-nix node recover
mount --rbind /srv/distributed-nix/worker/nix/store /nix/store
mount --bind /run/distributed-nix-runner /srv/distributed-nix/worker/nix/var/nix/daemon-socket
mount --rbind /srv/distributed-nix/worker/nix/var/nix /nix/var/nix
mount --bind /srv/distributed-nix/worker/work /work
mount --bind /srv/distributed-nix/worker/root /root
cp /srv/distributed-nix/worker/etc/nix/nix.conf /etc/nix/nix.conf
cp /srv/distributed-nix/worker/etc/passwd /etc/passwd
cp /srv/distributed-nix/worker/etc/group /etc/group
if [ "$role" = builder ]; then
  distributed-nix node runner-daemon & daemon_pid=$!; pids+=("$daemon_pid")
  while ! test -S /run/distributed-nix-runner/socket; do
    kill -0 "$daemon_pid" || exit 1
    sleep .1
  done
fi
distributed-nix serve & pids+=("$!")
if [ "$role" = store ]; then
  distributed-nix publisher & pids+=("$!")
  (
    while true; do
      if distributed-nix gc --if-needed; then sleep "${DISTRIBUTED_NIX_GC_INTERVAL_SECONDS:-3600}"; else sleep 5; fi
    done
  ) & pids+=("$!")
fi
if [ "$role" = builder ]; then
  until distributed-nix bootstrap; do
    for pid in "${pids[@]}"; do kill -0 "$pid" || exit 1; done
    sleep 2
  done
fi
touch /run/distributed-nix-ready
wait -n "${pids[@]}"
exit 1
