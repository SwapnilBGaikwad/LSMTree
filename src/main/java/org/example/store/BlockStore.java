package org.example.store;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class BlockStore<K extends Comparable<? super K>, V> {
    private enum State {
        IN_MEMORY,
        PERSISTING,
        PERSISTED
    }

    private final int maxActiveKeyCount;
    private final HashMap<K, V> data;
    private final HashSet<String> tombstones;
    private final TreeMap<String, FilePointer> index;
    private volatile State state;
    private volatile Path persistedFile;

    public BlockStore(int maxActiveKeyCount) {
        if (maxActiveKeyCount <= 0) {
            throw new IllegalArgumentException("maxActiveKeyCount must be > 0");
        }
        this.maxActiveKeyCount = maxActiveKeyCount;
        this.data = new HashMap<>();
        this.tombstones = new HashSet<>();
        this.index = new TreeMap<>();
        this.state = State.IN_MEMORY;
    }

    public synchronized void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        state = State.IN_MEMORY;
        persistedFile = null;
        index.clear();
        tombstones.remove(String.valueOf(key));
        data.put(key, value);
    }

    public synchronized V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        if (isTombstoned(key)) {
            return null;
        }
        if (state == State.PERSISTED && persistedFile != null) {
            FilePointer pointer = index.get(String.valueOf(key));
            if (pointer == null) {
                return null;
            }
            @SuppressWarnings("unchecked")
            V value = (V) FileUtils.readValue(persistedFile, pointer);
            return value;
        }
        return data.get(key);
    }

    public synchronized boolean containsKey(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        String keyAsString = String.valueOf(key);
        return tombstones.contains(keyAsString) || data.containsKey(key) || index.containsKey(keyAsString);
    }

    public synchronized boolean isTombstoned(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return tombstones.contains(String.valueOf(key));
    }

    public synchronized Map<K, V> scan(String prefix) {
        String effectivePrefix = prefix == null ? "" : prefix;
        Map<K, V> result = new TreeMap<>();
        if (state == State.PERSISTED && persistedFile != null) {
            for (Map.Entry<String, FilePointer> entry : index.entrySet()) {
                if (!entry.getKey().startsWith(effectivePrefix)) {
                    continue;
                }
                if (tombstones.contains(entry.getKey())) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                K key = (K) entry.getKey();
                @SuppressWarnings("unchecked")
                V value = (V) FileUtils.readValue(persistedFile, entry.getValue());
                result.put(key, value);
            }
            return result;
        }

        for (Map.Entry<K, V> entry : data.entrySet()) {
            if (String.valueOf(entry.getKey()).startsWith(effectivePrefix)) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    public synchronized void delete(K key) {
        markDeleted(key);
    }

    public synchronized void markDeleted(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        state = State.IN_MEMORY;
        persistedFile = null;
        index.clear();
        data.remove(key);
        tombstones.add(String.valueOf(key));
    }

    public synchronized boolean isFull() {
        return (data.size() + tombstones.size()) >= maxActiveKeyCount;
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

    public synchronized Set<String> tombstonesByPrefix(String prefix) {
        String effectivePrefix = prefix == null ? "" : prefix;
        Set<String> result = new HashSet<>();
        for (String key : tombstones) {
            if (key.startsWith(effectivePrefix)) {
                result.add(key);
            }
        }
        return result;
    }
}
