#!/usr/bin/env bash
# CI-neutral build. Authenticate Azure CLI first; supply only approved wheels.
set -euo pipefail
if [[ $# != 4 ]]; then
  echo 'Usage: build.sh ACR_NAME RELEASE WHEEL_DIRECTORY OUTPUT_DIRECTORY' >&2
  exit 2
fi
registry_name=$1
release=$2
wheel_directory=$(realpath "$3")
output_directory=$(realpath -m "$4")
[[ $registry_name =~ ^[a-zA-Z0-9]{5,50}$ && $release =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || exit 2
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
mkdir -p "$output_directory"
# Keep the builder selected by setup-buildx-action while isolating registry
# credentials. Buildx otherwise falls back to the engine's default builder.
if [[ -z ${BUILDX_CONFIG:-} ]]; then
  stlc_original_docker_config=${DOCKER_CONFIG:-"$HOME/.docker"}
  export BUILDX_CONFIG="$stlc_original_docker_config/buildx"
fi
export DOCKER_CONFIG
DOCKER_CONFIG=$(mktemp -d)
trap 'rm -rf "$DOCKER_CONFIG"' EXIT
registry_server=$(az acr show --name "$registry_name" --query loginServer --output tsv --only-show-errors)
# Azure CLI writes its short-lived registry token directly to the private Docker
# configuration. It never enters a build argument, image layer or output file.
az acr login --name "$registry_name" --only-show-errors >/dev/null
for role in webhook worker; do
  target=webhook
  if [[ $role == worker ]]; then target=managed_runtime; fi
  image="$registry_server/stlc-$role:$release"
  docker buildx build --platform linux/amd64 --file "$repo_root/Dockerfile.azure" \
    --build-context "stlc_wheels=$wheel_directory" --target "$target" \
    --tag "$image" --load "$repo_root"
  if [[ $role == worker ]]; then
    # Real browser launch as the runtime UID before publishing the worker.
    docker run --rm --user 10001:10001 --entrypoint node "$image" -e \
      "(async()=>{const {chromium}=require('/opt/stlc-playwright/node_modules/playwright');const b=await chromium.launch({headless:true});const p=await b.newPage();await p.setContent('<title>STLC</title>');if(await p.title()!=='STLC')throw Error('browser smoke failed');await b.close()})().catch(()=>process.exit(1))"
  fi
  docker buildx build --platform linux/amd64 --file "$repo_root/Dockerfile.azure" \
    --build-context "stlc_wheels=$wheel_directory" --target "$target" \
    --tag "$image" --metadata-file "$output_directory/$role-build.json" \
    --provenance=true --push "$repo_root"
done
python3 - "$registry_server" "$release" "$output_directory" <<'PY'
import json,re,sys
from pathlib import Path
registry,release,directory=sys.argv[1:]
folder=Path(directory)
images={}
for role in ('webhook','worker'):
    digest=json.loads((folder/f'{role}-build.json').read_text())['containerimage.digest']
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',digest):
        raise SystemExit('Build did not produce an immutable image digest')
    images[role]=f'{registry}/stlc-{role}@{digest}'
(folder/'images.json').write_text(json.dumps({'release':release,'images':images},indent=2)+'\n')
print(json.dumps({'images':images}))
PY
