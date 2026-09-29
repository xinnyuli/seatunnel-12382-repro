import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/** One target batch per isolated test JVM. No source/slot/checkpoint mutation. */
public final class FlushGate {
    private static final AtomicBoolean USED = new AtomicBoolean();
    private static volatile String held;
    public static void await(String run, String job, Long checkpoint, Runnable reached) throws Exception {
        if (run == null || !run.startsWith("gate-")) return;
        if (!run.matches("gate-[a-zA-Z0-9-]+") || checkpoint == null || "unknown".equals(job)) {
            throw new IllegalStateException("Gate requires run, job and checkpoint identity");
        }
        if (!USED.compareAndSet(false, true)) return;
        Path release = Paths.get(System.getProperty("pg12382.gateDir", "/tmp"), run + ".release");
        if (Files.exists(release)) throw new IllegalStateException("Stale gate release signal");
        held = job + ":" + checkpoint;
        try {
            reached.run();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
            while (!Files.exists(release)) {
                if (System.nanoTime() >= deadline) {
                    System.err.println("[PG12382] GATE_TIMEOUT run=" + run + " checkpoint=" + checkpoint);
                    throw new IllegalStateException("GATE_TIMEOUT");
                }
                Thread.sleep(50);
            }
        } finally { held = null; }
    }
    public static boolean isHeld(String job, Object checkpoint) {
        return (job + ":" + checkpoint).equals(held);
    }
}
