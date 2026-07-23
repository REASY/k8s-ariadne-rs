#!/usr/bin/env bash
set -euo pipefail

# Advisory-only source health report. These metrics are signals for review, not
# pass/fail thresholds: cohesive code is preferable to mechanically small files.
report_limit="${RUST_SOURCE_REPORT_LIMIT:-20}"
report_data="$(mktemp)"
trap 'rm -f "$report_data"' EXIT

find . -path './target' -prune -o -type f -name '*.rs' -print |
  sed 's#^\./##' |
  sort |
  while IFS= read -r source_file; do
    awk -v file="$source_file" '
      BEGIN {
        lines = 0
        functions = 0
        decisions = 0
      }
      {
        lines += 1
        if ($0 ~ /^[[:space:]]*(pub(\([^)]*\))?[[:space:]]+)?(async[[:space:]]+)?fn[[:space:]]+/) {
          functions += 1
        }
        if ($0 ~ /(^|[^[:alnum:]_])(if|match|for|while|loop)([^[:alnum:]_]|$)/ ||
            $0 ~ /&&|\|\|/) {
          decisions += 1
        }
      }
      END {
        printf "%d\t%d\t%d\t%s\n", lines, functions, decisions, file
      }
    ' "$source_file"
  done >"$report_data"

render_report() {
  printf '## Rust source health (advisory)\n\n'
  printf 'Largest %s Rust source files. “Decision lines” is a lightweight complexity proxy, not cyclomatic complexity.\n\n' "$report_limit"
  printf '| Lines | Functions | Decision lines | File |\n'
  printf '| ---: | ---: | ---: | :--- |\n'
  sort -t $'\t' -k1,1nr -k4,4 "$report_data" |
    head -n "$report_limit" |
    awk -F '\t' '{ printf "| %d | %d | %d | `%s` |\n", $1, $2, $3, $4 }'
}

render_report
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  render_report >>"$GITHUB_STEP_SUMMARY"
fi
