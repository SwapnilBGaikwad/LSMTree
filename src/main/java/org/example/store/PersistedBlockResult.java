package org.example.store;

import java.nio.file.Path;
import java.util.NavigableMap;

public record PersistedBlockResult(Path filePath, NavigableMap<String, FilePointer> index) {
}
