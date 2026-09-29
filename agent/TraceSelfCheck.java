import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/** Checks real bytecode advice, including isolated loaders, void/boolean returns and exceptions. */
public class TraceSelfCheck {
    public static class Fixture {
        public boolean shouldEmit(SourceRecord record) { return false; }
        public void processElement(SourceRecord record) { throw new IllegalStateException("expected"); }
        public void processElement(SourceRecord record, Object unused, Split split) { collect(new SeaTunnelRow()); }
        public void collect(Object row) { }
        public void addToBatch(Object row) { }
        public java.util.List<Split> snapshotState(long checkpoint) { return Collections.singletonList(new Split()); }
        public void enqueue(Event event) { }
        public void changeRecord(Object partition, Object schema, Object operation, Object key, Object value, Offset offset) { }
    }
    public static class SeaTunnelRow {
        public Object getField(int index) { return 15; }
        public String getRowKind() { return "INSERT"; }
    }
    public static class Offset {
        public java.util.Map<String, Long> getOffset() { return Collections.singletonMap("lsn", 123L); }
    }
    public static class Split {
        public boolean isIncrementalSplit() { return true; }
        public boolean isIncrementalSplitState() { return true; }
        public String splitId() { return "test-split"; }
        public Offset getStartupOffset() { return new Offset(); }
    }
    public static class Event {
        public SourceRecord getRecord() { return record(15); }
    }
    private static SourceRecord record(int id) {
        Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        Schema sourceSchema = SchemaBuilder.struct().field("table", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA).field("lsn", Schema.INT64_SCHEMA)
            .field("txId", Schema.INT64_SCHEMA).build();
        Schema valueSchema = SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA)
            .field("source", sourceSchema).build();
        Struct info = new Struct(sourceSchema).put("table", "postgres_cdc_table_1")
            .put("schema", "inventory").put("lsn", 123L).put("txId", 77L);
        return new SourceRecord(Collections.emptyMap(), Collections.singletonMap("lsn", 123L),
            "inventory.postgres_cdc_table_1", keySchema, new Struct(keySchema).put("id", id),
            valueSchema, new Struct(valueSchema).put("op", "c").put("source", info));
    }
    public static void main(String[] args) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream original = System.err;
        System.setErr(new PrintStream(bytes, true, "UTF-8"));
        try {
            Fixture fixture = new Fixture();
            if (fixture.shouldEmit(record(15))) throw new AssertionError("return changed");
            fixture.shouldEmit(record(150));
            fixture.enqueue(new Event());
            SourceRecord target = record(15);
            fixture.changeRecord(null, null, null, target.key(), target.value(), new Offset());
            fixture.processElement(target, null, new Split());
            fixture.addToBatch(new SeaTunnelRow());
            fixture.snapshotState(7);
            try { fixture.processElement(record(15)); throw new AssertionError("exception swallowed"); }
            catch (IllegalStateException expected) { }
            // Force the observed class into a child loader; helper must still be reachable.
            URL location = TraceSelfCheck.class.getProtectionDomain().getCodeSource().getLocation();
            try (URLClassLoader child = new URLClassLoader(new URL[]{location}, TraceSelfCheck.class.getClassLoader()) {
                @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                    if (name.startsWith("TraceSelfCheck")) {
                        Class<?> loaded = findLoadedClass(name);
                        return loaded == null ? findClass(name) : loaded;
                    }
                    return super.loadClass(name, resolve);
                }
            }) {
                Object isolated = child.loadClass("TraceSelfCheck$Fixture").newInstance();
                isolated.getClass().getMethod("shouldEmit", SourceRecord.class).invoke(isolated, record(15));
            }
        } finally { System.setErr(original); }
        String trace = bytes.toString("UTF-8");
        if (!trace.contains("accepted=false") || !trace.contains("processElement_THROW")
                || trace.contains("id=150") || trace.contains("TRACE_ERROR") || trace.contains("[Byte Buddy] ERROR")) {
            throw new AssertionError(trace);
        }
        if (trace.split("stage=shouldEmit_ENTER", -1).length != 3) throw new AssertionError(trace);
        for (String marker : new String[]{"enqueue_RETURN", "changeRecord_RETURN", "collect_RETURN", "addToBatch_RETURN", "checkpoint=7", "savedOffset={lsn=123,}", "readerOffset={lsn=123,}"}) {
            if (!trace.contains(marker)) throw new AssertionError(marker + " missing: " + trace);
        }
        System.out.println("TRACE_SELF_CHECK_PASS: structured key, handoff, reader/row correlation, snapshot, return, exception, child loader");
        java.nio.file.Path directory = java.nio.file.Files.createTempDirectory("pg12382-gate-check");
        String gateRun = "gate-selfcheck-" + java.util.UUID.randomUUID();
        System.setProperty("pg12382.gateDir", directory.toString());
        java.util.concurrent.CountDownLatch reached = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CompletableFuture<Void> future = java.util.concurrent.CompletableFuture.runAsync(() -> {
            try { FlushGate.await(gateRun, "job", 7L, reached::countDown); }
            catch (Exception error) { throw new RuntimeException(error); }
        });
        try {
            if (!reached.await(2, java.util.concurrent.TimeUnit.SECONDS) || future.isDone()
                    || !FlushGate.isHeld("job", 7L) || FlushGate.isHeld("other-job", 7L)) {
                throw new AssertionError("Gate did not hold the intended checkpoint");
            }
        } finally { java.nio.file.Files.write(directory.resolve(gateRun + ".release"), new byte[0]); }
        future.get(2, java.util.concurrent.TimeUnit.SECONDS);
        if (FlushGate.isHeld("job", 7L)) throw new AssertionError("Gate remained held after release");
        java.nio.file.Files.delete(directory.resolve(gateRun + ".release"));
        java.nio.file.Files.delete(directory);
        System.clearProperty("pg12382.gateDir");
        System.out.println("FLUSH_GATE_SELF_CHECK_PASS: blocked, checkpoint identity, file release");
    }
}
