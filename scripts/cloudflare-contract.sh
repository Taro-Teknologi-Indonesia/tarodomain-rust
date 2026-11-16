#!/usr/bin/env bash
# Runs every contract suite on D1 itself, inside workerd (plan Step 10.8).
#
# Needs Node.js (for wrangler) and `worker-build` (cargo install worker-build).
# No Cloudflare account: `wrangler dev` uses a local D1 kept in a temp dir.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
port="${PORT:-8787}"
state="$(mktemp -d)"
log="$state/wrangler.log"
pid=""

cleanup() {
  [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  rm -rf "$state"
}
trap cleanup EXIT

cd "$root/harness/cloudflare-contract"
npx --yes wrangler dev --local --port "$port" --persist-to "$state/persist" >"$log" 2>&1 &
pid=$!

echo "waiting for the harness on :$port ..."
for _ in $(seq 1 240); do
  if curl -fsS "http://127.0.0.1:$port/suites" >/dev/null 2>&1; then ready=1; break; fi
  if ! kill -0 "$pid" 2>/dev/null; then echo "wrangler exited:"; cat "$log"; exit 1; fi
  sleep 1
done
[[ "${ready:-}" == 1 ]] || { echo "the harness never came up:"; cat "$log"; exit 1; }

failed=0
for suite in $(curl -fsS "http://127.0.0.1:$port/suites" | tr -d '[]"' | tr ',' ' '); do
  if out="$(curl -sS -X POST -w ' (%{http_code})' "http://127.0.0.1:$port/suites/$suite")" \
     && [[ "$out" == "ok (200)" ]]; then
    echo "ok    $suite"
  else
    echo "FAIL  $suite: $out"
    failed=1
  fi
done

if [[ "$failed" != 0 ]]; then
  echo "--- wrangler log ---"
  cat "$log"
  exit 1
fi
echo "every suite passed on D1"
