#!/usr/bin/env bash
# ci/fetch-otelcol.sh <dir> — fetch the OpenTelemetry Collector the OTLP e2e (`ferrod`'s
# `tests/otlp_it.rs`, M2-C4c-2) drives as its CONSUMER, and print the binary's path.
#
# ONE place pins the version and the checksum, shared by `.github/workflows/ci.yml` (the
# `integration` lane) and `ci/local-gate.sh --live`, so the two cannot drift. The checksum is the
# official release's own (`opentelemetry-collector-releases_otelcol_checksums.txt`). Cached: a
# binary already in <dir> is reused.
set -euo pipefail
dir="${1:?usage: fetch-otelcol.sh <dir>}"
version="0.115.0"
sha256="cf6268c459cfc92cc3c2bcab9a631ca1024d5e60bc727e760870f9f9d923dbb9"
url="https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v${version}/otelcol_${version}_linux_amd64.tar.gz"

mkdir -p "$dir"
if [ ! -x "$dir/otelcol" ]; then
  curl -sSfL -o "$dir/otelcol.tgz" "$url"
  echo "${sha256}  $dir/otelcol.tgz" | sha256sum -c - >&2
  tar xzf "$dir/otelcol.tgz" -C "$dir" otelcol
  rm -f "$dir/otelcol.tgz"
fi
echo "$dir/otelcol"
