# SST

Library for Java implementation of SST DS (String Sorted Table DS).

## What SST Means
SST stands for String Sorted Table. It is an immutable, sorted collection of string keys, typically stored on disk, that supports efficient point lookups and sequential iteration.

## Extending to a Key-Value Store with Scan Support
You can build a simple key-value store by layering:
1. An in-memory memtable (e.g., `TreeMap`) for writes.
2. Periodic flushes of the memtable to SST files on disk.
3. A lookup path that checks the memtable first, then SST files (newest to oldest).
4. A merge iterator across SST files to support ordered scans and range queries.

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
