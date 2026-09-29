#!/usr/bin/env bash
# Build the test-only tracing agent (same steps as test-trace/build.ps1 on the original
# workstation). Output: $1 (default agent/pg12382-test-agent.jar). Runs the executable
# self-check unless SKIP_SELFCHECK=1.
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${1:-$HERE/agent/pg12382-test-agent.jar}"
WORK="$(mktemp -d)"
LIB="$WORK/lib"; CLS="$WORK/classes"
mkdir -p "$LIB" "$CLS"
trap 'rm -rf "$WORK"' EXIT

MAVEN_CENTRAL="https://repo1.maven.org/maven2"
# name|path — versions match the ones the original local build resolved from ~/.m2.
DEPS=(
  "byte-buddy.jar|net/bytebuddy/byte-buddy/1.12.19/byte-buddy-1.12.19.jar"
  "connect-api.jar|org/apache/kafka/connect-api/3.2.0/connect-api-3.2.0.jar"
  "kafka-clients.jar|org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar"
  "slf4j-api.jar|org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar"
)
for dep in "${DEPS[@]}"; do
  name="${dep%%|*}"; path="${dep#*|}"
  curl -fsSL --retry 5 --retry-delay 3 -o "$LIB/$name" "$MAVEN_CENTRAL/$path"
  expected="$(curl -fsSL --retry 5 --retry-delay 3 "$MAVEN_CENTRAL/$path.sha1" | tr -d ' \n' | cut -c1-40)"
  actual="$(sha1sum "$LIB/$name" | cut -d' ' -f1)"
  if [ "$expected" != "$actual" ]; then echo "SHA-1 mismatch for $path" >&2; exit 1; fi
done

# Byte Buddy is bundled into the agent jar (as build.ps1 does with `jar xf`).
( cd "$CLS" && jar xf "$LIB/byte-buddy.jar" )
javac -nowarn -encoding UTF-8 -source 8 -target 8 \
  -cp "$LIB/byte-buddy.jar:$LIB/connect-api.jar" -d "$CLS" \
  "$HERE/agent/TraceAgent.java" "$HERE/agent/TraceProbe.java" \
  "$HERE/agent/TraceSelfCheck.java" "$HERE/agent/FlushGate.java" 2>&1 | grep -v '^Note:\|bootstrap class path\|^warning\|^[0-9]* warning' || true
test -f "$CLS/TraceAgent.class" || { echo "agent compilation failed" >&2; exit 1; }
printf 'Manifest-Version: 1.0\nPremain-Class: TraceAgent\n\n' > "$WORK/agent.mf"
mkdir -p "$(dirname "$OUT")"
jar cfm "$OUT" "$WORK/agent.mf" -C "$CLS" .

if [ "${SKIP_SELFCHECK:-0}" != "1" ]; then
  java "-javaagent:$OUT=selfcheck" \
    -cp "$CLS:$LIB/connect-api.jar:$LIB/kafka-clients.jar:$LIB/slf4j-api.jar" TraceSelfCheck
fi
sha256sum "$OUT"
