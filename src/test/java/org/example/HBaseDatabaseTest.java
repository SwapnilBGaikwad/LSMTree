package org.example;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HBaseDatabaseTest {
    private static final int MAX_KEYS = 10;

    @TempDir
    Path tempDir;

    @Test
    void shouldCreateMultipleBlocksWhenMaxLimitIsHit() {
        HBaseDatabase<String, String> database = createDatabase();
        int totalKeys = 25;

        for (int i = 0; i < totalKeys; i++) {
            database.put(generateKey("key-", i), generateValue(i));
        }
        waitForBackupFileCount(2);

        // Limit is 10 by default, so 25 keys require 3 blocks.
        assertEquals(3, database.getBlockCount());
    }

    @Test
    void shouldReadKeysAcrossMultipleBlocks() {
        HBaseDatabase<String, String> database = createDatabase();

        for (int i = 0; i < 25; i++) {
            database.put(generateKey("key-", i), generateValue(i));
        }
        waitForBackupFileCount(2);

        assertEquals(generateValue(0), database.get(generateKey("key-", 0)));
        assertEquals(generateValue(24), database.get(generateKey("key-", 24)));
    }

    @Test
    void shouldScanAcrossBlocksWithPrefix() {
        HBaseDatabase<String, String> database = createDatabase();
        int matchingKeyCount = 21;

        for (int i = 0; i < matchingKeyCount; i++) {
            database.put(generateKey("acct-", i), generateValue(i));
        }
        for (int i = 0; i < 5; i++) {
            database.put(generateKey("user-", i), generateValue(100 + i));
        }
        waitForBackupFileCount(2);

        Map<String, String> scanResult = database.scan("acct-");
        List<String> keys = new ArrayList<>(scanResult.keySet());

        assertEquals(matchingKeyCount, scanResult.size());
        assertEquals("acct-000", keys.get(0));
        assertEquals("acct-020", keys.get(keys.size() - 1));
    }

    @Test
    void shouldDeleteKeyFromAllBlocks() {
        HBaseDatabase<String, String> database = createDatabase();

        database.put("dup-001", "old-value");
        for (int i = 0; i < 9; i++) {
            database.put(generateKey("base-", i), generateValue(i));
        }
        database.put(generateKey("rollover-", 0), generateValue(200));
        database.put("dup-001", "new-value");

        assertEquals("new-value", database.get("dup-001"));

        database.delete("dup-001");
        assertNull(database.get("dup-001"));
    }

    @Test
    void shouldWriteBackupFileWhenBlockRollsOver() throws IOException {
        HBaseDatabase<String, String> database = createDatabase();

        for (int i = 0; i < 11; i++) {
            database.put(generateKey("bk-", i), generateValue(i));
        }
        waitForBackupFileCount(1);

        Path backupFile = tempDir.resolve("block-000001.sst");
        List<String> lines = Files.readAllLines(backupFile);

        assertEquals(10, lines.size());
        assertEquals("bk-000=>value-0-payload", lines.get(0));
        assertEquals("bk-009=>value-9-payload", lines.get(lines.size() - 1));
    }

    @Test
    void shouldWriteMultipleBackupFilesInSequence() throws IOException {
        HBaseDatabase<String, String> database = createDatabase();

        for (int i = 0; i < 25; i++) {
            database.put(generateKey("multi-", i), generateValue(i));
        }
        waitForBackupFileCount(2);

        List<String> fileNames = Files.list(tempDir)
                .map(path -> path.getFileName().toString())
                .sorted()
                .collect(Collectors.toList());

        assertEquals(2, fileNames.size());
        assertEquals("block-000001.sst", fileNames.get(0));
        assertEquals("block-000002.sst", fileNames.get(1));
    }

    @Test
    void shouldNotPersistTombstoneEntriesInBackupFile() throws IOException {
        HBaseDatabase<String, String> database = createDatabase();

        for (int i = 0; i < 9; i++) {
            database.put(generateKey("live-", i), generateValue(i));
        }
        database.delete("key-should-not-persist");
        database.put("trigger-rollover", "v");
        waitForBackupFileCount(1);

        Path backupFile = tempDir.resolve("block-000001.sst");
        List<String> lines = Files.readAllLines(backupFile);

        assertEquals(9, lines.size());
        boolean tombstonePresent = lines.stream().anyMatch(line -> line.startsWith("key-should-not-persist=>"));
        assertEquals(false, tombstonePresent);
    }

    private static String generateKey(String prefix, int index) {
        return prefix + String.format("%03d", index);
    }

    private static String generateValue(int index) {
        return "value-" + index + "-payload";
    }

    private HBaseDatabase<String, String> createDatabase() {
        HBaseConfig config = new HBaseConfig(tempDir.toString(), MAX_KEYS);
        return new HBaseDatabase<>(config);
    }

    private void waitForBackupFileCount(int expectedCount) {
        long timeoutMillis = 2000L;
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMillis) {
            try (var stream = Files.list(tempDir)) {
                long count = stream.count();
                if (count >= expectedCount) {
                    return;
                }
            } catch (IOException ignored) {
                // no-op
            }
            sleepQuietly(25L);
        }
        throw new AssertionError("Timed out waiting for backup files. expected=" + expectedCount);
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted while waiting for backup completion", e);
        }
    }
}
