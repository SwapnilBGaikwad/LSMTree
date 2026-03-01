package org.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

class HBaseDatabasePerfTest {
    private static final int TARGET_ACTIVE_BYTES = 10 * 1024 * 1024; // 10 MB
    private static final int VALUE_SIZE_BYTES = 256;
    private static final int KEY_ESTIMATED_BYTES = 12; // key-00000000
    private static final int ENTRY_ESTIMATED_BYTES = VALUE_SIZE_BYTES + KEY_ESTIMATED_BYTES;
    private static final int ACTIVE_KEY_COUNT = TARGET_ACTIVE_BYTES / ENTRY_ESTIMATED_BYTES;
    private static final int WARMUP_RUNS = 2;
    private static final String SCAN_PREFIX = "key-1";

    @TempDir
    Path tempDir;

    @Test
    void shouldMeasureAvgPutAndGetTimeForTenMbActiveKeys() {
        List<String> keys = generateKeys(ACTIVE_KEY_COUNT);
        List<String> values = generateValues(ACTIVE_KEY_COUNT, VALUE_SIZE_BYTES);

        // Warm-up to reduce first-run noise.
        for (int i = 0; i < WARMUP_RUNS; i++) {
            runPutGetCycle(keys, values);
        }

        RunMetrics metrics = runPutGetCycle(keys, values);

        double avgPutMicros = nanosToMicros(metrics.totalPutNanos() / (double) ACTIVE_KEY_COUNT);
        double avgGetMicros = nanosToMicros(metrics.totalGetNanos() / (double) ACTIVE_KEY_COUNT);
        double avgScanMicros = nanosToMicros(metrics.totalScanNanos() / (double) metrics.scanIterations());

        System.out.printf(
                Locale.ROOT,
                "Perf(10MB active): keys=%d, avgPut=%.3f us, avgGet=%.3f us, avgScan(%s)=%.3f us%n",
                ACTIVE_KEY_COUNT,
                avgPutMicros,
                avgGetMicros,
                SCAN_PREFIX,
                avgScanMicros
        );
    }

    private RunMetrics runPutGetCycle(List<String> keys, List<String> values) {
        HBaseConfig config = new HBaseConfig(tempDir.toString(), ACTIVE_KEY_COUNT);
        HBaseDatabase<String, String> database = new HBaseDatabase<>(config);

        long putStart = System.nanoTime();
        for (int i = 0; i < ACTIVE_KEY_COUNT; i++) {
            database.put(keys.get(i), values.get(i));
        }
        long putEnd = System.nanoTime();

        long getStart = System.nanoTime();
        for (int i = 0; i < ACTIVE_KEY_COUNT; i++) {
            database.get(keys.get(i));
        }
        long getEnd = System.nanoTime();

        int scanIterations = Math.min(100, ACTIVE_KEY_COUNT);
        long scanStart = System.nanoTime();
        for (int i = 0; i < scanIterations; i++) {
            database.scan(SCAN_PREFIX);
        }
        long scanEnd = System.nanoTime();

        return new RunMetrics(putEnd - putStart, getEnd - getStart, scanEnd - scanStart, scanIterations);
    }

    private static List<String> generateKeys(int count) {
        List<String> keys = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            keys.add(String.format(Locale.ROOT, "key-%08d", i));
        }
        return keys;
    }

    private static List<String> generateValues(int count, int valueSizeBytes) {
        List<String> values = new ArrayList<>(count);
        String payload = "v".repeat(valueSizeBytes);
        for (int i = 0; i < count; i++) {
            values.add(payload);
        }
        return values;
    }

    private static double nanosToMicros(double nanos) {
        return nanos / 1_000.0;
    }

    private record RunMetrics(long totalPutNanos, long totalGetNanos, long totalScanNanos, int scanIterations) {
    }
}
