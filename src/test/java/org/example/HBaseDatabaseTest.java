package org.example;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HBaseDatabaseTest {

    @Test
    void shouldCreateMultipleBlocksWhenMaxLimitIsHit() {
        HBaseDatabase<String, String> database = new HBaseDatabase<>();
        int totalKeys = 25;

        for (int i = 0; i < totalKeys; i++) {
            database.put(generateKey("key-", i), generateValue(i));
        }

        // Limit is 10 by default, so 25 keys require 3 blocks.
        assertEquals(3, database.getBlockCount());
    }

    @Test
    void shouldReadKeysAcrossMultipleBlocks() {
        HBaseDatabase<String, String> database = new HBaseDatabase<>();

        for (int i = 0; i < 25; i++) {
            database.put(generateKey("key-", i), generateValue(i));
        }

        assertEquals(generateValue(0), database.get(generateKey("key-", 0)));
        assertEquals(generateValue(24), database.get(generateKey("key-", 24)));
    }

    @Test
    void shouldScanAcrossBlocksWithPrefix() {
        HBaseDatabase<String, String> database = new HBaseDatabase<>();
        int matchingKeyCount = 21;

        for (int i = 0; i < matchingKeyCount; i++) {
            database.put(generateKey("acct-", i), generateValue(i));
        }
        for (int i = 0; i < 5; i++) {
            database.put(generateKey("user-", i), generateValue(100 + i));
        }

        Map<String, String> scanResult = database.scan("acct-");
        List<String> keys = new ArrayList<>(scanResult.keySet());

        assertEquals(matchingKeyCount, scanResult.size());
        assertEquals("acct-000", keys.get(0));
        assertEquals("acct-020", keys.get(keys.size() - 1));
    }

    @Test
    void shouldDeleteKeyFromAllBlocks() {
        HBaseDatabase<String, String> database = new HBaseDatabase<>();

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

    private static String generateKey(String prefix, int index) {
        return prefix + String.format("%03d", index);
    }

    private static String generateValue(int index) {
        return "value-" + index + "-payload";
    }
}
