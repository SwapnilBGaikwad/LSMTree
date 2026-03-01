package org.example;

import org.example.store.SST;

import java.util.Map;
import java.util.Objects;

public class HBaseDatabase<K extends Comparable<? super K>, V> implements Database<K, V> {
    private final SST<K, V> sst;

    public HBaseDatabase() {
        this.sst = new SST<>();
    }

    public HBaseDatabase(int maxKeysPerBlock) {
        this.sst = new SST<>(maxKeysPerBlock);
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key cannot be null");
        sst.put(key, value);
    }

    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        return sst.get(key);
    }

    @Override
    public Map<K, V> scan(String prefix) {
        return sst.scan(prefix);
    }

    @Override
    public void delete(K key) {
        Objects.requireNonNull(key, "key cannot be null");
        sst.delete(key);
    }

    int getBlockCount() {
        return sst.getBlockCount();
    }
}
