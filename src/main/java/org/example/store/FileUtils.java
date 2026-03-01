package org.example.store;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

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

    public static PersistedBlockResult writeBlockBackup(Path directory, int blockSequence, Map<String, String> sortedEntries) {
        ensureDirectory(directory);
        Path file = directory.resolve(String.format("block-%06d.sst", blockSequence));
        NavigableMap<String, FilePointer> index = new TreeMap<>();
        long offset = 0L;

        try (var outputStream = Files.newOutputStream(file)) {
            for (Map.Entry<String, String> entry : sortedEntries.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] separatorBytes = "=>".getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
                byte[] newlineBytes = "\n".getBytes(StandardCharsets.UTF_8);

                long valueOffset = offset + keyBytes.length + separatorBytes.length;
                index.put(key, new FilePointer(valueOffset, valueBytes.length));

                outputStream.write(keyBytes);
                outputStream.write(separatorBytes);
                outputStream.write(valueBytes);
                outputStream.write(newlineBytes);

                offset += keyBytes.length + separatorBytes.length + valueBytes.length + newlineBytes.length;
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write block backup file: " + file, e);
        }

        return new PersistedBlockResult(file, index);
    }

    public static String readValue(Path file, FilePointer pointer) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.toFile(), "r")) {
            randomAccessFile.seek(pointer.offset());
            byte[] valueBytes = new byte[pointer.length()];
            randomAccessFile.readFully(valueBytes);
            return new String(valueBytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read value from backup file: " + file, e);
        }
    }
}
