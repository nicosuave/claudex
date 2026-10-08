#!/usr/bin/env bash
set -euo pipefail

version=${1:?Usage: update-homebrew.sh VERSION}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]
tap=nicosuave/homebrew-tap
workflow=update-formula.yml

update_formula() {
  local previous candidate log run_id=""
  previous=$(gh run list --repo "$tap" --workflow "$workflow" --event repository_dispatch --limit 100 --json databaseId --jq '.[].databaseId')
  gh api --method POST "repos/$tap/dispatches" \
    -f event_type=update-formula \
    -f 'client_payload[formula]=claudex' \
    -f "client_payload[version]=$version" \
    -f 'client_payload[repo]=nicosuave/claudex'
  for ((attempt=0; attempt<60; attempt++)); do
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      if grep -Fqx "$candidate" <<<"$previous"; then continue; fi
      log=$(gh run view "$candidate" --repo "$tap" --log 2>/dev/null || true)
      if grep -Fq "Updating claudex to $version" <<<"$log"; then
        run_id=$candidate
        break
      fi
    done < <(gh run list --repo "$tap" --workflow "$workflow" --event repository_dispatch --limit 20 --json databaseId --jq '.[].databaseId')
    [[ -n "$run_id" ]] && break
    sleep 2
  done
  [[ -n "$run_id" ]] || { echo "Timed out finding the Claudex tap update" >&2; return 1; }
  gh run watch "$run_id" --repo "$tap" --exit-status
}

if ! update_formula; then
  echo "Retrying once after the release-asset CDN settles" >&2
  sleep 10
  update_formula
fi
