package com.pradheeparul.storage;

import java.util.List;
import java.util.Map;

/**
 * Storage
 *
 * @author PradheepKumarA
 */
public interface Storage {
    void store(String shardId, List<Map<String, String>> records, String prefix);
}
