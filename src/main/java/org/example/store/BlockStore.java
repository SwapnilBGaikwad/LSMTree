package org.example.store;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class BlockStore<K extends Comparable<? super K>, V> {
    private enum State {
        IN_MEMORY,
        PERSISTING,
        PERSISTED
    }

    private final int maxActiveKeyCount;
    private final HashMap<K, V> data;
    private final TreeMap<String, FilePointer> index;
    private volatile State state;
    private volatile Path persistedFile;

    public BlockStore(int maxActiveKeyCount) {
        if (maxActiveKeyCount <= 0) {
            throw new IllegalArgumentException("maxActiveKeyCount must be > 0");
        }
        this.maxActiveKeyCount = maxActiveKeyCount;
        this.data = new HashMap<>();
        this.index = new TreeMap<>();
        this.state = State.IN_MEMORY;
    }

    public synchronized void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        state = State.IN_MEMORY;
        persistedFile = null;
        index.clear();
        data.put(key, value);
    }

    public synchronized V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return data.get(key);
    }

    public synchronized boolean containsKey(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return data.containsKey(key) || index.containsKey(String.valueOf(key));
    }

    public synchronized Map<K, V> scan(String prefix) {
        String effectivePrefix = prefix == null ? "" : prefix;
        Map<K, V> result = new TreeMap<>();
        for (Map.Entry<K, V> entry : data.entrySet()) {
            if (String.valueOf(entry.getKey()).startsWith(effectivePrefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public synchronized void delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        data.remove(key);
        index.remove(String.valueOf(key));
    }

    public synchronized boolean isFull() {
        return data.size() >= maxActiveKeyCount;
    }

    public synchronized Map<K, V> snapshotSorted() {
        return new TreeMap<>(data);
    }

    public synchronized void markPersisting() {
        if (state == State.IN_MEMORY) {
            state = State.PERSISTING;
        }
    }

    public synchronized void markPersisted(Path filePath, NavigableMap<String, FilePointer> sortedIndex) {
        persistedFile = filePath;
        index.clear();
        index.putAll(sortedIndex);
        data.clear();
        state = State.PERSISTED;
    }

    public synchronized boolean isPersisted() {
        return state == State.PERSISTED;
    }

    public synchronized Path getPersistedFile() {
        return persistedFile;
    }

    public synchronized FilePointer getFilePointer(K key) {
        return index.get(String.valueOf(key));
    }

    public synchronized Map<String, FilePointer> scanFilePointers(String prefix) {
        String effectivePrefix = prefix == null ? "" : prefix;
        Map<String, FilePointer> result = new TreeMap<>();
        for (Map.Entry<String, FilePointer> entry : index.entrySet()) {
            if (entry.getKey().startsWith(effectivePrefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}
