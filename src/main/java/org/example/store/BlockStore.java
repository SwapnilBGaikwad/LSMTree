package org.example.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class BlockStore<K extends Comparable<? super K>, V> {
    private final int maxActiveKeyCount;
    private final HashMap<K, V> data;

    public BlockStore(int maxActiveKeyCount) {
        if (maxActiveKeyCount <= 0) {
            throw new IllegalArgumentException("maxActiveKeyCount must be > 0");
        }
        this.maxActiveKeyCount = maxActiveKeyCount;
        this.data = new HashMap<>();
    }

    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        data.put(key, value);
    }

    public V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return data.get(key);
    }

    public boolean containsKey(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return data.containsKey(key);
    }

    public Map<K, V> scan(String prefix) {
        String effectivePrefix = prefix == null ? "" : prefix;
        Map<K, V> result = new TreeMap<>();
        for (Map.Entry<K, V> entry : data.entrySet()) {
            if (String.valueOf(entry.getKey()).startsWith(effectivePrefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public void delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        data.remove(key);
    }

    public boolean isFull() {
        return data.size() >= maxActiveKeyCount;
    }

    public Map<K, V> snapshotSorted() {
        return new TreeMap<>(data);
    }
}
