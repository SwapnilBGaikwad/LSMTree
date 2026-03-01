# LSM

Library for Java implementation of an LSM-style key-value store.

## What LSM Means
LSM stands for Log-Structured Merge tree. It organizes writes in memory and periodically persists sorted data to disk for efficient reads and scans.

## Extending to a Key-Value Store with Scan Support
You can build a simple key-value store by layering:
1. An in-memory memtable (e.g., `TreeMap`) for writes.
2. Periodic flushes of the memtable to sorted segment files on disk.
3. A lookup path that checks the memtable first, then segment files (newest to oldest).
4. A merge iterator across segment files to support ordered scans and range queries.

With this structure, you can support:
- `put(key, value)` for writes
- `get(key)` for point reads
- `scan(startKey, endKey)` for ordered range reads

## Requirements
- Java 17+
- Maven 3.8+

## Build
```bash
mvn clean package
```

## Run
```bash
mvn -q exec:java -Dexec.mainClass=org.example.Main
```
