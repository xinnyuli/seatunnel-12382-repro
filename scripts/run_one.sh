#!/usr/bin/env bash
# One measurement: patch the test (test-only), optionally load the tracing agent, run the focused
# IT once, classify the result. Never modifies production code; refuses to run if anything but
# the IT file differs from the pinned revision.
#
# Required env: TARGET  SHA  JAVA  IDX  SEATUNNEL_DIR
# Optional env: OUT_DIR (default ./out)  MAVEN_TIMEOUT_MIN (45)
#               INSTRUMENTATION: full (IT log lines + agent, default) | logs-only (IT log lines, no agent)
#                                | none (untouched upstream test, no agent; the control for "does observing change it?")
#               TRACE=false is the legacy spelling of logs-only
#               MVN_CMD (default ./mvnw; tests inject a stub)  PREBUILT_AGENT (skip build, tests only)
#               STRESS (true|false, default false): stress-ng CPU/memory/IO contention, started only once the
#               IT begins (not during the build); STRESS_CMD overrides the command (tests)
#               DBZ_LOGS (true|false, default false): raise three Debezium classes that log the WAL resume
#               search (WalPositionLocator, AbstractMessageDecoder, PostgresStreamingChangeEventSource) from
#               WARN to INFO in the test container's log4j2.properties (a test resource). Config only.
#               TEST_FROM_SHA (optional, full SHA): take PostgresCDCIT.java from this apache/seatunnel commit
#               instead of SHA's own copy. Lets a before/after compare the SAME test against two
#               production revisions (e.g. PR #12454, which also edits this IT). Test file only.
#               WAL_RANGE (true|false, default false): test-only patch that logs pg_current_wal_insert_lsn()/
#               pg_current_wal_lsn() right before and after the id=15 insert. Needs INSTRUMENTATION=none.
#               GITHUB_RUN_ID / GITHUB_RUN_ATTEMPT (used in the run id)
set -uo pipefail

HARNESS="$(cd "$(dirname "$0")/.." && pwd)"
: "${TARGET:?}" "${SHA:?}" "${JAVA:?}" "${IDX:?}" "${SEATUNNEL_DIR:?}"
OUT_DIR="${OUT_DIR:-$PWD/out}"
TRACE="${TRACE:-true}"
if [ -z "${INSTRUMENTATION:-}" ]; then
  if [ "$TRACE" = "false" ]; then INSTRUMENTATION=logs-only; else INSTRUMENTATION=full; fi
fi
case "$INSTRUMENTATION" in full|logs-only|none) ;; *) echo "bad INSTRUMENTATION=$INSTRUMENTATION" >&2; exit 1;; esac
MAVEN_TIMEOUT_MIN="${MAVEN_TIMEOUT_MIN:-45}"
MVN_CMD="${MVN_CMD:-./mvnw}"
STRESS="${STRESS:-false}"
DBZ_LOGS="${DBZ_LOGS:-false}"
case "$DBZ_LOGS" in true|false) ;; *) echo "bad DBZ_LOGS=$DBZ_LOGS" >&2; exit 1;; esac
TEST_FROM_SHA="${TEST_FROM_SHA:-}"
case "$TEST_FROM_SHA" in ''|[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;; *) echo "bad TEST_FROM_SHA=$TEST_FROM_SHA" >&2; exit 1;; esac
[ -z "$TEST_FROM_SHA" ] || [ ${#TEST_FROM_SHA} -eq 40 ] || { echo "TEST_FROM_SHA must be a full 40-char SHA" >&2; exit 1; }
WAL_RANGE="${WAL_RANGE:-false}"
case "$WAL_RANGE" in true|false) ;; *) echo "bad WAL_RANGE=$WAL_RANGE" >&2; exit 1;; esac
if [ "$WAL_RANGE" = true ] && [ "$INSTRUMENTATION" != none ]; then echo "WAL_RANGE=true needs INSTRUMENTATION=none" >&2; exit 1; fi
STRESS_CMD="${STRESS_CMD:-stress-ng --cpu $(nproc) --cpu-load 80 --vm 1 --vm-bytes 25% --hdd 1 --timeout 1800s}"
# Never start with "gate-": that prefix would switch on the (experimental) flush-gate in the agent.
RUN_ID="${TARGET}-j${JAVA}-n${IDX}-${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
TEST_PATH='seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-postgres-e2e/src/test/java/org/apache/seatunnel/connectors/seatunnel/cdc/postgres/PostgresCDCIT.java'
RES_DIR='seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources'
AGENT_NAME='pg12382-test-agent.jar'
LOG4J="$RES_DIR/log4j2.properties"
PATCH="$HARNESS/patches/observation.patch"
WAL_PATCH="$HARNESS/patches/wal-range.patch"
IT_METHOD='PostgresCDCIT#testPostgresCdcSnapshotOnlyAndCommittedOffsetStartupModes'

mkdir -p "$OUT_DIR"
LOG="$OUT_DIR/mvn.log"
META=$(python3 - <<PY
import json
print(json.dumps({"target":"$TARGET","sha":"$SHA","java":"$JAVA","idx":int("$IDX"),"run_id":"$RUN_ID","instrumentation":"$INSTRUMENTATION","stress":"$STRESS"=="true","dbz_logs":"$DBZ_LOGS"=="true","test_from_sha":"$TEST_FROM_SHA" or None,"wal_range":"$WAL_RANGE"=="true"}))
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

# Restore every tracked file we may touch (test file, log4j) and drop the jar, whatever happens.
trap 'rm -f "$SEATUNNEL_DIR/$RES_DIR/$AGENT_NAME"; git -C "$SEATUNNEL_DIR" checkout HEAD -- "$TEST_PATH" "$LOG4J" 2>/dev/null; true' EXIT

# ---- optional: the same IT from another revision (test file only) -----------------------------
TEST_SWAPPED=0
if [ -n "$TEST_FROM_SHA" ]; then
  git cat-file -e "$TEST_FROM_SHA^{commit}" 2>/dev/null \
    || git fetch -q --depth 1 origin "$TEST_FROM_SHA" \
    || harness_error "cannot fetch TEST_FROM_SHA $TEST_FROM_SHA"
  git checkout "$TEST_FROM_SHA" -- "$TEST_PATH" || harness_error "cannot take $TEST_PATH from $TEST_FROM_SHA"
  git diff --quiet HEAD -- "$TEST_PATH" || TEST_SWAPPED=1
fi
TEST_SHA256="$(sha256sum "$TEST_PATH" | cut -d' ' -f1)"

# ---- test-only observation patch ------------------------------------------------------------
if [ "$INSTRUMENTATION" = "none" ] && [ "$WAL_RANGE" = true ]; then
  if ! git apply --check "$WAL_PATCH" 2>"$OUT_DIR/patch_error.txt"; then
    echo "wal-range patch does not apply to this IT; needs review, not forced" >&2
    python3 - <<PY > "$OUT_DIR/result.json"
import json
d = json.loads('''$META''')
d.update(category="NO_TEST_RUN", reached_check=False, trace_valid=False, boundary_verdict="n/a",
         note="WAL_PATCH_DOES_NOT_APPLY", maven_exit=None)
print(json.dumps(d, indent=2))
PY
    exit 0
  fi
elif [ "$INSTRUMENTATION" = "none" ]; then
  : # untouched upstream test: nothing to apply
elif ! git apply --check "$PATCH" 2>"$OUT_DIR/patch_error.txt"; then
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
EXPECT_CHANGED=""
[ "$TEST_SWAPPED" = 1 ] && EXPECT_CHANGED="$TEST_PATH"
if [ "$INSTRUMENTATION" = "none" ] && [ "$WAL_RANGE" = true ]; then
  git apply "$WAL_PATCH" || harness_error "wal-range patch failed after successful --check"
  EXPECT_CHANGED="$TEST_PATH"
elif [ "$INSTRUMENTATION" != "none" ]; then
  git apply "$PATCH" || harness_error "patch application failed after successful --check"
  EXPECT_CHANGED="$TEST_PATH"
fi
# ---- optional: surface Debezium's WAL resume decisions (test resource config only) ------------
if [ "$DBZ_LOGS" = "true" ]; then
  # Upstream sets io.debezium.connector to WARN here; more specific loggers override it for three classes.
  grep -q '^logger.debezium.name=io.debezium.connector$' "$LOG4J" || harness_error "unexpected $LOG4J layout; not editing it"
  cat >>"$LOG4J" <<'L4J'

# [PG12382] test-only (issue #12382): log Debezium's WAL resume search and replay filtering
logger.pg12382loc.name=io.debezium.connector.postgresql.connection.WalPositionLocator
logger.pg12382loc.level=INFO
logger.pg12382dec.name=io.debezium.connector.postgresql.connection.AbstractMessageDecoder
logger.pg12382dec.level=INFO
logger.pg12382src.name=io.debezium.connector.postgresql.PostgresStreamingChangeEventSource
logger.pg12382src.level=INFO
L4J
  EXPECT_CHANGED="$(printf '%s\n%s\n' "$EXPECT_CHANGED" "$LOG4J" | sed '/^$/d' | LC_ALL=C sort)"
fi
changed="$(git diff HEAD --name-only | LC_ALL=C sort)"
[ "$changed" = "$EXPECT_CHANGED" ] || harness_error "unexpected changes after patching: $changed"

AGENT_SHA="not-loaded"
JVM_OPT=()
if [ "$INSTRUMENTATION" = "full" ]; then
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
# only the IT file (and, with DBZ_LOGS, the test log4j2.properties) may differ from the pinned revision
# (the jar is untracked and removed on exit)
[ "$(git diff HEAD --name-only | LC_ALL=C sort)" = "$EXPECT_CHANGED" ] || harness_error "unexpected tracked change before run"

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
echo "[harness] run=$RUN_ID sha=$SHA java=$JAVA instrumentation=$INSTRUMENTATION dbzLogs=$DBZ_LOGS testFrom=${TEST_FROM_SHA:-same} walRange=$WAL_RANGE testSha256=$TEST_SHA256 agentSha256=$AGENT_SHA" | tee "$LOG"
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
d.update(agent_sha256="$AGENT_SHA", duration_sec=$((END-START)), test_sha256="$TEST_SHA256")
print(json.dumps(d))
PY
)
BARE=(); [ "$INSTRUMENTATION" = "none" ] && BARE=(--bare)
python3 "$HARNESS/scripts/analyze_run.py" --log "$LOG" --maven-exit "$MVN_EXIT" --meta "$META" "${BARE[@]}" --out "$OUT_DIR/result.json" || harness_error "analyzer crashed"
{
  echo "revision=$SHA"; echo "experimentalResume=false"; echo "run=$RUN_ID"; echo "agentSha256=$AGENT_SHA"
  echo "instrumentation=$INSTRUMENTATION"; echo "dbzLogs=$DBZ_LOGS"; echo "observationPatchSha256=$(sha256sum "$PATCH" | cut -d' ' -f1)"; echo "javaMatrix=$JAVA"; echo "mavenExit=$MVN_EXIT"
  echo "testFromSha=${TEST_FROM_SHA:-same}"; echo "walRange=$WAL_RANGE"; echo "walRangePatchSha256=$(sha256sum "$WAL_PATCH" | cut -d' ' -f1)"; echo "testSha256=$TEST_SHA256"
  echo "testDiff:"; git diff HEAD -- "$TEST_PATH"
  echo "log4jDiff:"; git diff HEAD -- "$LOG4J"
  grep -E '\[PG12382\] |WalPositionLocator - |AbstractMessageDecoder - |PostgresStreamingChangeEventSource - |Tests run:|BUILD (SUCCESS|FAILURE)|^\[ERROR\]' "$LOG" | grep -v '^[+-]' 
} >"$OUT_DIR/summary.txt" 2>/dev/null
FS="seatunnel-e2e/seatunnel-connector-v2-e2e/connector-cdc-postgres-e2e/target/failsafe-reports"
[ -d "$FS" ] && mkdir -p "$OUT_DIR/failsafe-reports" && cp -r "$FS"/. "$OUT_DIR/failsafe-reports/" 2>/dev/null
gzip -f "$LOG"
python3 -c "import json;d=json.load(open('$OUT_DIR/result.json'));print('[harness] category=%s trace_valid=%s verdict=%s dbz_false_match=%s boundary_hit=%s'%(d['category'],d['trace_valid'],d['boundary_verdict'],d.get('dbz_false_match'),d.get('dbz_boundary_hit')))"
exit 0
