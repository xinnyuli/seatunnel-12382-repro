import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Observes only the single-row experiment. Never changes records, offsets, or return values. */
public final class TraceProbe {
    private static final AtomicLong SEQUENCE = new AtomicLong();
    private static final ThreadLocal<String> ROW = new ThreadLocal<>();
    private static final ThreadLocal<Long> CHECKPOINT = new ThreadLocal<>();
    // Diagnostic lifetime is one focused IT, including its restores. Preserve failed batch history.
    private static final Map<Object, String> TARGETS = Collections.synchronizedMap(new IdentityHashMap<>());

    public static Object call(Object obj, String name, Object... args) throws Exception {
        for (Method method : obj.getClass().getMethods()) {
            if (method.getName().equals(name) && method.getParameterCount() == args.length) {
                boolean matches = true;
                Class<?>[] types = method.getParameterTypes();
                for (int i = 0; i < args.length; i++) {
                    if (args[i] != null && !types[i].isInstance(args[i])
                            && !(types[i] == int.class && args[i] instanceof Integer)) matches = false;
                }
                if (!matches) continue;
                method.setAccessible(true);
                return method.invoke(obj, args);
            }
        }
        throw new NoSuchMethodException(obj.getClass().getName() + "." + name);
    }
    private static Object field(Object obj, String name) throws Exception {
        for (Class<?> type = obj.getClass(); type != null; type = type.getSuperclass()) {
            try {
                Field field = type.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(obj);
            } catch (NoSuchFieldException ignored) { }
        }
        throw new NoSuchFieldException(name);
    }
    private static Object struct(Object value, String name) throws Exception {
        if (value == null) return null;
        Object schema = call(value, "schema");
        if (call(schema, "field", name) == null) return null;
        return call(value, "get", name);
    }
    private static String offset(Object value) throws Exception {
        if (value == null) return "null";
        if (!(value instanceof Map)) value = call(value, "getOffset");
        Map<?, ?> map = (Map<?, ?>) value;
        StringBuilder text = new StringBuilder();
        for (String key : new String[]{"lsn", "lsn_proc", "lsn_commit", "last_commit_lsn", "txId", "messageType"}) {
            if (map.containsKey(key)) text.append(key).append('=').append(map.get(key)).append(',');
        }
        return text.toString();
    }
    private static String source(Object record) throws Exception {
        if (record == null) return null;
        Object value = call(record, "value");
        return source(call(record, "key"), value, call(record, "sourceOffset"));
    }
    private static String source(Object key, Object value, Object position) throws Exception {
        Object operation = struct(value, "op");
        if (!"c".equals(operation)) return null;
        Object id = struct(key, "id");
        if (!(id instanceof Number) || ((Number) id).longValue() != 15) return null;
        Object info = struct(value, "source");
        Object table = struct(info, "table");
        if (!"postgres_cdc_table_1".equals(table) || !"inventory".equals(struct(info, "schema"))) return null;
        return "id=15 op=c schema=" + struct(info, "schema") + " table=" + table
            + " eventLsn=" + struct(info, "lsn") + " tx=" + struct(info, "txId")
            + " offset={" + offset(position) + "}";
    }
    private static boolean row(Object record) throws Exception {
        if (record == null || !record.getClass().getSimpleName().equals("SeaTunnelRow")) return false;
        // This focused IT has id as its first column. Do not reuse for arbitrary schemas.
        Object id = call(record, "getField", 0);
        return id instanceof Number && ((Number) id).longValue() == 15
            && "INSERT".equals(String.valueOf(call(record, "getRowKind")));
    }
    private static String identity(Object obj) {
        return obj == null ? "null" : obj.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(obj));
    }
    private static String job(Object owner) {
        String job = "unknown";
        try {
            Class<?> mdc = owner.getClass().getClassLoader().loadClass("org.slf4j.MDC");
            Object value = mdc.getMethod("get", String.class).invoke(null, "ST-JID");
            if (value != null) job = value.toString();
        } catch (Throwable ignored) { }
        return job;
    }
    private static void log(String stage, Object owner, String details) {
        System.err.println("[PG12382] run=" + System.getProperty("pg12382.run")
            + " job=" + job(owner)
            + " seq=" + SEQUENCE.incrementAndGet() + " time=" + System.currentTimeMillis()
            + " thread=" + Thread.currentThread().getName() + " owner=" + identity(owner)
            + " stage=" + stage + " " + details);
    }
    public static Object enter(Object owner, String method, Object[] args) {
        try {
            String detail = null;
            Object first = args.length == 0 ? null : args[0];
            if (method.equals("notifyCheckpointComplete") && FlushGate.isHeld(job(owner), first)) {
                log("GATE_COMPLETED_WHILE_HELD", owner, "checkpoint=" + first);
            }
            if (method.equals("changeRecord")) detail = source(args[3], args[4], call(args[5], "getOffset"));
            if (method.equals("enqueue")) detail = source(call(first, "getRecord"));
            if (method.equals("shouldEmit") || method.equals("processElement")) detail = source(first);
            if (method.equals("processElement") && detail == null) {
                Object value = call(first, "value");
                if (struct(value, "op") == null && String.valueOf(call(first, "topic")).contains("heartbeat")) {
                    detail = "kind=heartbeat offset={" + offset(call(first, "sourceOffset")) + "}";
                }
            }
            if (method.equals("collect") && row(first)) detail = "id=15 parent={" + ROW.get() + "}";
            if ((method.equals("write") || method.equals("addToBatch")) && row(first)) {
                detail = "id=15";
                TARGETS.put(owner, detail);
            }
            if (method.equals("attemptFlush") || method.equals("prepareCommitInternal")) detail = TARGETS.get(owner);
            if (method.equals("commit") || method.equals("rollback")) detail = TARGETS.get(owner);
            if (method.equals("snapshotState") || method.equals("notifyCheckpointComplete")) detail = "checkpoint=" + first;
            if (method.equals("commitCurrentOffset")) detail = "requestedOffset={" + offset(first) + "}";
            if (detail == null) return null;
            if (method.equals("prepareCommitInternal")) CHECKPOINT.set((Long) first);
            String previous = ROW.get();
            if (method.equals("processElement")) ROW.set(detail);
            if (method.equals("attemptFlush")) {
                Connection connection = (Connection) call(field(owner, "connectionProvider"), "getConnection");
                TARGETS.put(connection, detail);
                detail += " connection=" + identity(connection) + " autoCommit=" + connection.getAutoCommit();
                final Long checkpoint = CHECKPOINT.get();
                FlushGate.await(System.getProperty("pg12382.run"), job(owner), checkpoint,
                    () -> log("GATE_REACHED", owner, "id=15 checkpoint=" + checkpoint));
                if (System.getProperty("pg12382.run", "").startsWith("gate-"))
                    log("GATE_RELEASED", owner, "id=15 checkpoint=" + checkpoint);
            }
            log(method + "_ENTER", owner, detail);
            return new Object[]{owner, method, detail, previous, args};
        } catch (Throwable error) {
            log("TRACE_ERROR", owner, method + " error=" + error.getClass().getName());
            return null;
        }
    }
    public static void exit(Object token, Object result, Throwable failure) {
        if (token == null) return;
        Object[] state = (Object[]) token;
        Object owner = state[0];
        String method = (String) state[1];
        try {
            String detail = (String) state[2];
            Object[] args = (Object[]) state[4];
            if (method.equals("processElement") && args.length == 3
                    && (Boolean) call(args[2], "isIncrementalSplitState")) {
                detail += " readerOffset={" + offset(call(args[2], "getStartupOffset")) + "}";
            }
            if (method.equals("commitCurrentOffset")) detail += " taskLastCommitLsn=" + field(owner, "lastCommitLsn");
            if (method.equals("shouldEmit")) detail += " accepted=" + result;
            if (method.equals("snapshotState") && failure == null) {
                for (Object split : (Iterable<?>) result) {
                    if ((Boolean) call(split, "isIncrementalSplit")) {
                        detail += " split=" + call(split, "splitId")
                            + " savedOffset={" + offset(call(split, "getStartupOffset")) + "}";
                    }
                }
            }
            log(method + (failure == null ? "_RETURN" : "_THROW"), owner,
                detail + (failure == null ? "" : " error=" + failure.getClass().getName()));
        } catch (Throwable error) {
            log("TRACE_ERROR", owner, method + " error=" + error.getClass().getName());
        } finally {
            if (method.equals("prepareCommitInternal")) CHECKPOINT.remove();
            if (method.equals("processElement")) {
                if (state[3] == null) ROW.remove(); else ROW.set((String) state[3]);
            }
        }
    }
}
