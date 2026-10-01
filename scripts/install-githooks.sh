#!/usr/bin/env bash
# Instala los git hooks versionados en .githooks/ (pre-commit, pre-push) para este repo.
# Uso: ./scripts/install-githooks.sh
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit .githooks/pre-push

echo "✓ core.hooksPath -> .githooks"
echo "✓ hooks instalados: $(ls .githooks)"
