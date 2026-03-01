package org.example.store;

import org.example.Database;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class SST<K extends Comparable<? super K>, V> implements Database<K, V> {
    private static final int DEFAULT_MAX_KEYS_PER_BLOCK = 10;
    private static final Path DEFAULT_BACKUP_DIRECTORY = Paths.get("backup");

    private final int maxKeysPerBlock;
    private final Path backupDirectory;
    private final List<BlockStore<K, V>> blockStores;
    private int backupSequence;

    public SST() {
        this(DEFAULT_MAX_KEYS_PER_BLOCK, DEFAULT_BACKUP_DIRECTORY);
    }

    public SST(int maxKeysPerBlock) {
        this(maxKeysPerBlock, DEFAULT_BACKUP_DIRECTORY);
    }

    public SST(int maxKeysPerBlock, Path backupDirectory) {
        if (maxKeysPerBlock <= 0) {
            throw new IllegalArgumentException("maxKeysPerBlock must be > 0");
        }
        Objects.requireNonNull(backupDirectory, "backupDirectory cannot be null");
        this.maxKeysPerBlock = maxKeysPerBlock;
        this.backupDirectory = FileUtils.ensureDirectory(backupDirectory);
        this.blockStores = new ArrayList<>();
        this.backupSequence = 0;
        // Sentinel block to ensure there is always an active writable block.
        this.blockStores.add(new BlockStore<>(maxKeysPerBlock));
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        BlockStore<K, V> latest = getLatestBlock();
        if (latest.isFull() && !latest.containsKey(key)) {
            persistBlock(latest);
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

    public Path getBackupDirectory() {
        return backupDirectory;
    }

    private BlockStore<K, V> getLatestBlock() {
        if (blockStores.isEmpty()) {
            BlockStore<K, V> blockStore = new BlockStore<>(maxKeysPerBlock);
            blockStores.add(blockStore);
            return blockStore;
        }
        return blockStores.get(0);
    }

    private void persistBlock(BlockStore<K, V> blockStore) {
        Map<K, V> sortedSnapshot = blockStore.snapshotSorted();
        Map<String, String> persistable = new LinkedHashMap<>();

        for (Map.Entry<K, V> entry : sortedSnapshot.entrySet()) {
            persistable.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        backupSequence++;
        FileUtils.writeBlockBackup(backupDirectory, backupSequence, persistable);
    }
}
