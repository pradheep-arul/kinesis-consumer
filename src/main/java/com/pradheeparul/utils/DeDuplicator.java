package com.pradheeparul.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * DeDuplicator
 *
 * @author PradheepKumarA
 */
public class DeDuplicator {
    private static final Logger logger = LogManager.getLogger(DeDuplicator.class);
    private static final Set<String> visited = new HashSet<>();
    private static final int DEDUPLICATE_WINDOW = PropertyUtils.getIntegerProperty("deduplicate", "deduplicate_window");

    public static String getCacheKey(String hashCode) {
        return "KINESIS_RECORD_HASH-" + hashCode;
    }


    public List<Map<String, String>> deduplicate(List<Map<String, String>> actualRecords, String shardId) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> record : actualRecords) {
            if (shouldSkipProcessing(record, shardId)) {
                logger.info("Duplicated record: " + record);
                continue;
            }
            markRecordProcessing(record, shardId);
            result.add(record);
        }
        return result;
    }


    /**
     * Determines whether to skip processing a record based on its cache status.
     */
    public boolean shouldSkipProcessing(Map<String, String> record, String shardId) {
        String hashCode = record.get("hashCode");
        String value = Cache.get(getCacheKey(hashCode));

        // If the record is not in the Cache, it has not been processed, so we should not skip it
        if (value == null) {
            return false;
        }

        // If the Cache returns "PROCESSED" response, the record has already been processed, so we can skip it
        else if (value.equals("PROCESSED")) {
            return true;
        }

        // If the Cache doesn't return "PROCESSED" and doesn't contain the current shard ID,
        // it's being processed by another shard processor, so we can skip it.
        // This scenario doesn't apply now, but useful when we consider running multiple parallel consumers.
        else if (!value.contains(shardId)) {
            return true;
        }

        // If the record is present in the "visited" set, it means this same(duplicate) record have already been
        // considered for processing in the current iteration, so we can skip it
        else if (visited.contains(hashCode)) {
            return true;
        }

        // It reaches this condition only when the previous processing was not successful, so we should not skip it.
        return false;
    }


    public void markRecordProcessing(Map<String, String> record, String shardId) {
        String hashCode = record.get("hashCode");
        visited.add(hashCode);
        Cache.set(getCacheKey(hashCode), "Processing-in-" + shardId, DEDUPLICATE_WINDOW);
    }

    public void markRecordsProcessed(List<Map<String, String>> records) {
        for (Map<String, String> record : records) {
            String hashCode = record.get("hashCode");
            visited.remove(hashCode);
            Cache.set(getCacheKey(hashCode), "PROCESSED", DEDUPLICATE_WINDOW);
        }
    }


}
