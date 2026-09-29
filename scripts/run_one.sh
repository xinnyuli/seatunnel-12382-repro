#!/usr/bin/env bash
# One measurement: patch the test (test-only), optionally load the tracing agent, run the focused
# IT once, classify the result. Never modifies production code; refuses to run if anything but
# the IT file differs from the pinned revision.
#
# Required env: TARGET  SHA  JAVA  IDX  SEATUNNEL_DIR
# Optional env: OUT_DIR (default ./out)  TRACE (true|false, default true)  MAVEN_TIMEOUT_MIN (45)
#               MVN_CMD (default ./mvnw; tests inject a stub)  PREBUILT_AGENT (skip build, tests only)
#               STRESS (true|false, default false): stress-ng CPU/memory/IO contention, started only once the
#               IT begins (not during the build); STRESS_CMD overrides the command (tests)
#               GITHUB_RUN_ID / GITHUB_RUN_ATTEMPT (used in the run id)
set -uo pipefail

HARNESS="$(cd "$(dirname "$0")/.." && pwd)"
: "${TARGET:?}" "${SHA:?}" "${JAVA:?}" "${IDX:?}" "${SEATUNNEL_DIR:?}"
OUT_DIR="${OUT_DIR:-$PWD/out}"
TRACE="${TRACE:-true}"
MAVEN_TIMEOUT_MIN="${MAVEN_TIMEOUT_MIN:-45}"
MVN_CMD="${MVN_CMD:-./mvnw}"
STRESS="${STRESS:-false}"
STRESS_CMD="${STRESS_CMD:-stress-ng --cpu $(nproc) --cpu-load 80 --vm 1 --vm-bytes 25% --hdd 1 --timeout 1800s}"
# Never start with "gate-": that prefix would switch on the (experimental) flush-gate in the agent.
RUN_ID="${TARGET}-j${JAVA}-n${IDX}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
TEST_PATH='seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-postgres-e2e/src/test/java/org/apache/seatunnel/connectors/seatunnel/cdc/postgres/PostgresCDCIT.java'
RES_DIR='seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources'
AGENT_NAME='pg12382-test-agent.jar'
PATCH="$HARNESS/patches/observation.patch"
IT_METHOD='PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes'

mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/mvn.log"
META=$(python3 - <<PY
import json
print(json.dumps({"target":"$TARGET","sha":"$SHA","java":"$JAVA","idx":int("$IDX"),"run_id":"$RUN_ID","trace_requested":"$TRACE"=="true","stress":"$STRESS"=="true"}))
PY
)

harness_error() {  # infrastructure problem in the harness itself: fail the job loudly
  echo "HARNESS ERROR: $*" >&2
  echo "$*" > "$OUT_DIR/harness_error.txt"
  exit 1
}

cd "$SEATUNNEL_DIR" || harness_error "SEATUNNEL_DIR missing"
head_sha="$(git rev-parse HEAD)"
[ "$head_sha" = "$SHA" ] || harness_error "checked-out revision $head_sha != expected $SHA"
[ -z "$(git status --porcelain)" ] || harness_error "working tree not clean before patching: $(git status --porcelain | head -3)"

# ---- test-only observation patch ------------------------------------------------------------
if ! git apply --check "$PATCH" 2>"$OUT_DIR/patch_error.txt"; then
  echo "observation patch does not apply at $SHA (the IT changed); needs review, not forced" >&2
  python3 - <<PY > "$OUT_DIR/result.json"
import json
d = json.loads('''$META''')
d.update(category="NO_TEST_RUN", reached_check=False, trace_valid=False, boundary_verdict="n/a",
         note="PATCH_DOES_NOT_APPLY", maven_exit=None)
print(json.dumps(d, indent=2))
PY
  exit 0
fi
PATCHED=0
trap 'rm -f "$SEATUNNEL_DIR/$RES_DIR/$AGENT_NAME"; [ "$PATCHED" = 1 ] && git -C "$SEATUNNEL_DIR" apply -R "$PATCH" 2>/dev/null; true' EXIT
git apply "$PATCH" || harness_error "patch application failed after successful --check"
PATCHED=1
changed="$(git diff HEAD --name-only)"
[ "$changed" = "$TEST_PATH" ] || harness_error "changes outside the IT after patching: $changed"

AGENT_SHA="not-loaded"
JVM_OPT=()
if [ "$TRACE" = "true" ]; then
  if [ -n "${PREBUILT_AGENT:-}" ]; then
    agent_jar="$PREBUILT_AGENT"
  else
    agent_jar="$OUT_DIR/$AGENT_NAME"
    bash "$HARNESS/scripts/build_agent.sh" "$agent_jar" >"$OUT_DIR/agent_build.log" 2>&1 || {
      tail -30 "$OUT_DIR/agent_build.log" >&2; harness_error "agent build/self-check failed"; }
  fi
  [ ! -e "$RES_DIR/$AGENT_NAME" ] || harness_error "diagnostic jar already exists in test resources"
  cp "$agent_jar" "$RES_DIR/$AGENT_NAME"
  AGENT_SHA="$(sha256sum "$RES_DIR/$AGENT_NAME" | cut -d' ' -f1)"
  JVM_OPT=("-Dseatunnel.e2e.seatunnel.server.jvm.option=-javaagent:/tmp/seatunnel/config/$AGENT_NAME=$RUN_ID")
fi
# only the IT file may differ from the pinned revision (the jar is untracked and removed on exit)
[ "$(git diff HEAD --name-only)" = "$TEST_PATH" ] || harness_error "unexpected tracked change before run"

# ---- run ----------------------------------------------------------------------------------------
# Same flags as upstream's all-connectors-it-N job, narrowed to this one test; zeta container only,
# like the failing scheduled run ("Test = zeta only").
export RUN_ALL_CONTAINER=false RUN_ZETA_CONTAINER=true
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx4096m}"
STRESS_PID=""
if [ "$STRESS" = "true" ]; then
  # Contention only while the IT runs, so the build is not slowed and the test sees the same load profile.
  ( until grep -q 'Running org.apache.seatunnel.connectors.seatunnel.cdc.postgres.PostgresCDCIT' "$LOG" 2>/dev/null; do sleep 2; done
    echo "[harness] stress started: $STRESS_CMD" >>"$LOG"
    exec $STRESS_CMD >>"$OUT_DIR/stress.log" 2>&1 ) &
  STRESS_PID=$!
fi
START=$(date +%s)
echo "[harness] run=$RUN_ID sha=$SHA java=$JAVA trace=$TRACE agentSha256=$AGENT_SHA" | tee "$LOG"
# shellcheck disable=SC2086
timeout -k 60 "${MAVEN_TIMEOUT_MIN}m" $MVN_CMD -B -T 1 -Pci verify \
  -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-postgres-e2e -am \
  -DskipUT=true -DskipIT=false -D"license.skipAddThirdParty"=true -D"skip.ui"=true -D"skip.spotless"=true \
  --no-snapshot-updates -D"failsafe.failIfNoSpecifiedTests"=false -DfailIfNoTests=false \
  -D"it.test"="$IT_METHOD" "${JVM_OPT[@]}" >>"$LOG" 2>&1
MVN_EXIT=$?
END=$(date +%s)
if [ -n "$STRESS_PID" ]; then pkill -P "$STRESS_PID" 2>/dev/null; kill "$STRESS_PID" 2>/dev/null; wait "$STRESS_PID" 2>/dev/null; fi

# ---- classify + package -------------------------------------------------------------------------
META=$(python3 - <<PY
import json
d = json.loads('''$META''')
d.update(agent_sha256="$AGENT_SHA", duration_sec=$((END-START)))
print(json.dumps(d))
PY
)
python3 "$HARNESS/scripts/analyze_run.py" --log "$LOG" --maven-exit "$MVN_EXIT" --meta "$META" --out "$OUT_DIR/result.json" || harness_error "analyzer crashed"
{
  echo "revision=$SHA"; echo "experimentalResume=false"; echo "run=$RUN_ID"; echo "agentSha256=$AGENT_SHA"
  echo "observationPatchSha256=$(sha256sum "$PATCH" | cut -d' ' -f1)"; echo "javaMatrix=$JAVA"; echo "mavenExit=$MVN_EXIT"
  echo "testDiff:"; git diff HEAD -- "$TEST_PATH"
  grep -E '\[PG12382\] |Tests run:|BUILD (SUCCESS|FAILURE)|^\[ERROR\]' "$LOG" | grep -v '^[+-]' 
} >"$OUT_DIR/summary.txt" 2>/dev/null
FS="seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-postgres-e2e/target/failsafe-reports"
[ -d "$FS" ] && mkdir -p "$OUT_DIR/failsafe-reports" && cp -r "$FS"/. "$OUT_DIR/failsafe-reports/" 2>/dev/null
gzip -f "$LOG"
python3 -c "import json;d=json.load(open('$OUT_DIR/result.json'));print('[harness] category=%s trace_valid=%s verdict=%s'%(d['category'],d['trace_valid'],d['boundary_verdict']))"
exit 0
