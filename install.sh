#!/usr/bin/env bash
# openaws installer (Linux / macOS)
#
# Installs openaws from source (it is not published to PyPI). Prefers pipx,
# then uv, then a plain pip install. Re-run with OPENAWS_REF=<branch|tag> to
# pin a revision.
set -euo pipefail

REPO="git+https://github.com/cognis-digital/openaws.git"
REF="${OPENAWS_REF:-}"
SPEC="$REPO"
if [ -n "$REF" ]; then
  SPEC="${REPO}@${REF}"
fi

echo "openaws installer"
echo "source: $SPEC"

if command -v pipx >/dev/null 2>&1; then
  echo "==> installing with pipx"
  pipx install "$SPEC"
elif command -v uv >/dev/null 2>&1; then
  echo "==> installing with uv"
  uv tool install "$SPEC"
elif command -v pip3 >/dev/null 2>&1; then
  echo "==> installing with pip3"
  pip3 install "$SPEC"
elif command -v pip >/dev/null 2>&1; then
  echo "==> installing with pip"
  pip install "$SPEC"
else
  echo "error: none of pipx, uv, or pip were found on PATH." >&2
  echo "Install Python 3.10+ and pip, then re-run this script." >&2
  exit 1
fi

echo
echo "Installed. Try:"
echo "  openaws serve"
echo "  openaws --help"
