package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class SST<K extends Comparable<? super K>, V> implements Database<K, V> {
    private static final int DEFAULT_MAX_KEYS_PER_BLOCK = 10;

    private final int maxKeysPerBlock;
    private final List<BlockStore<K, V>> blockStores;

    public SST() {
        this(DEFAULT_MAX_KEYS_PER_BLOCK);
    }

    public SST(int maxKeysPerBlock) {
        if (maxKeysPerBlock <= 0) {
            throw new IllegalArgumentException("maxKeysPerBlock must be > 0");
        }
        this.maxKeysPerBlock = maxKeysPerBlock;
        this.blockStores = new ArrayList<>();
        // Sentinel block to ensure there is always an active writable block.
        this.blockStores.add(new BlockStore<>(maxKeysPerBlock));
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        BlockStore<K, V> latest = getLatestBlock();
        if (latest.isFull() && !latest.containsKey(key)) {
            latest = new BlockStore<>(maxKeysPerBlock);
            blockStores.add(0, latest);
        }
        latest.put(key, value);
    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        for (BlockStore<K, V> blockStore : blockStores) {
            if (blockStore.containsKey(key)) {
                return blockStore.get(key);
            }
        }
        return null;
    }

    @Override
    public Map<K, V> scan(String prefix) {
        Map<K, V> merged = new TreeMap<>();
        for (BlockStore<K, V> blockStore : blockStores) {
            Map<K, V> current = blockStore.scan(prefix);
            for (Map.Entry<K, V> entry : current.entrySet()) {
                merged.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        return merged;
    }

    @Override
    public void delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        for (BlockStore<K, V> blockStore : blockStores) {
            blockStore.delete(key);
        }
    }

    public boolean isActiveBlockFull() {
        return getLatestBlock().isFull();
    }

    public int getBlockCount() {
        return blockStores.size();
    }

    private BlockStore<K, V> getLatestBlock() {
        if (blockStores.isEmpty()) {
            BlockStore<K, V> blockStore = new BlockStore<>(maxKeysPerBlock);
            blockStores.add(blockStore);
            return blockStore;
        }
        return blockStores.get(0);
    }
}
