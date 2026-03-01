package org.example;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class HBaseConfig {
    @NonNull
    private final String dataDir;
    private final int maxKeyCount;
}
