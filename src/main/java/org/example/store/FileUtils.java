package org.example.store;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

public final class FileUtils {
    private FileUtils() {
    }

    public static Path ensureDirectory(Path directory) {
        try {
            return Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create backup directory: " + directory, e);
        }
    }

    public static Path writeBlockBackup(Path directory, int blockSequence, Map<String, String> sortedEntries) {
        ensureDirectory(directory);
        Path file = directory.resolve(String.format("block-%06d.sst", blockSequence));

        try (BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            for (Map.Entry<String, String> entry : sortedEntries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                writer.write(key + "=>" + value);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write block backup file: " + file, e);
        }

        return file;
    }
}
