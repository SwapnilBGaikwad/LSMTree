package org.example;

import java.util.Map;

public interface Database<K extends Comparable<? super K>, V> {
    void put(K key, V value);
    V get(K key);
    Map<K, V> scan(String prefix);
    void delete(K key);
}
