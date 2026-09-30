#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Reuse a working Linux environment; preserve incompatible host environments.
if [ -e .venv ] && ! .venv/bin/python -c 'import sys; assert sys.version_info >= (3, 11)' >/dev/null 2>&1; then
  backup=$(mktemp -d /tmp/spaceport-venv-backup.XXXXXX)
  mv .venv "$backup/venv"
  echo "Previous virtual environment moved to $backup/venv"
fi
if [ ! -e .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m grpc_tools.protoc -I proto --python_out=src/spaceport_hivemind/generated proto/bazaar.proto

if [ ! -d bazaar-protobuf-starter-linux ]; then
  tar -xzf artifacts/bazaar-starter-linux.tar.gz
fi
for executable in .devcontainer/start-practice-server bazaar-protobuf-starter-linux/spaceport-validate-linux-arm64 bazaar-protobuf-starter-linux/spaceport-validate-linux-x86_64; do
  if [ ! -x "$executable" ]; then
    chmod +x "$executable"
  fi
done

# Never overwrite a team's private credentials during rebuilds.
if [ ! -e my-credentials.json ]; then
  cp credentials.example.json my-credentials.json
  chmod 600 my-credentials.json
fi
printf '\nSetup complete. Edit the three placeholders in my-credentials.json.\n'
printf 'Then follow GETTING_STARTED.md to connect your assigned planet.\n'
