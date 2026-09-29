import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import static net.bytebuddy.matcher.ElementMatchers.*;

/** Local diagnostic agent only; never ship in a connector or production distribution. */
public final class TraceAgent {
    public static void premain(String run, Instrumentation instrumentation) {
        System.setProperty("pg12382.run", run == null ? "unknown" : run);
        new AgentBuilder.Default()
            .disableClassFormatChanges()
            .with(new AgentBuilder.Listener.StreamWriting(System.err).withErrorsOnly())
            .type(namedOneOf(
                "io.debezium.connector.base.ChangeEventQueue",
                "io.debezium.pipeline.EventDispatcher$StreamingChangeRecordReceiver",
                "org.apache.seatunnel.connectors.cdc.base.source.reader.external.IncrementalSourceStreamFetcher",
                "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter",
                "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceRecordEmitter$OutputCollector",
                "org.apache.seatunnel.connectors.cdc.base.source.reader.IncrementalSourceReader",
                "org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.reader.wal.PostgresWalFetchTask",
                "org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkWriter",
                "org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormat",
                "org.postgresql.jdbc.PgConnection",
                "TraceSelfCheck$Fixture"))
            .transform((builder, type, loader, module, domain) -> {
                System.err.println("[PG12382] HOOK class=" + type.getName());
                return builder.visit(Advice.to(Observe.class).on(
                    namedOneOf("changeRecord", "enqueue", "shouldEmit", "processElement", "collect",
                        "snapshotState", "notifyCheckpointComplete", "commitCurrentOffset",
                        "write", "addToBatch", "attemptFlush", "prepareCommitInternal",
                        "commit", "rollback").and(not(isBridge())).and(not(isAbstract()))));
            }).installOn(instrumentation);
        System.err.println("[PG12382] AGENT_READY run=" + run);
    }

    public static class Observe {
        @Advice.OnMethodEnter
        public static Object enter(@Advice.This Object owner, @Advice.Origin("#m") String method,
                @Advice.AllArguments Object[] args) {
            // Reflection via the system loader works across connector child-first classloaders.
            try {
                return ClassLoader.getSystemClassLoader().loadClass("TraceProbe")
                    .getMethod("enter", Object.class, String.class, Object[].class)
                    .invoke(null, owner, method, args);
            } catch (Throwable failure) {
                System.err.println("[PG12382] TRACE_ERROR advice-enter " + failure.getClass().getName());
                return null;
            }
        }
        @Advice.OnMethodExit(onThrowable = Throwable.class)
        public static void exit(@Advice.Enter Object token,
                @Advice.Return(typing = Assigner.Typing.DYNAMIC) Object result,
                @Advice.Thrown Throwable failure) {
            try {
                ClassLoader.getSystemClassLoader().loadClass("TraceProbe")
                    .getMethod("exit", Object.class, Object.class, Throwable.class)
                    .invoke(null, token, result, failure);
            } catch (Throwable observerFailure) {
                System.err.println("[PG12382] TRACE_ERROR advice-exit " + observerFailure.getClass().getName());
            }
        }
    }
}
