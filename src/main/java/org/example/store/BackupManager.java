package org.example.store;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class BackupManager {
    private final Path backupDirectory;
    private final AtomicInteger backupSequence;
    private final ExecutorService executorService;

    public BackupManager(Path backupDirectory) {
        this.backupDirectory = FileUtils.ensureDirectory(backupDirectory);
        this.backupSequence = new AtomicInteger(0);
        this.executorService = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "sst-backup-manager");
            thread.setDaemon(true);
            return thread;
        });
    }

    public <K extends Comparable<? super K>, V> void submit(BlockStore<K, V> blockStore) {
        blockStore.markPersisting();
        Map<K, V> snapshot = blockStore.snapshotSorted();
        Map<String, String> persistable = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : snapshot.entrySet()) {
            persistable.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }

        int sequence = backupSequence.incrementAndGet();
        executorService.submit(() -> {
            PersistedBlockResult result = FileUtils.writeBlockBackup(backupDirectory, sequence, persistable);
            blockStore.markPersisted(result.filePath(), result.index());
        });
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public Path getBackupDirectory() {
        return backupDirectory;
    }
}
