package org.pradheeparul.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * DeDuplicator
 *
 * @author PradheepKumarA
 */
public class DeDuplicator {
    private static final Logger logger = LogManager.getLogger(DeDuplicator.class);

    public static String getCacheKey(String hashCode) {
        return "KINESIS_RECORD_HASH-" + hashCode;
    }


    public static List<Map<String, String>> deduplicate(List<Map<String, String>> actualRecords) {
        List<Map<String, String>> result = new ArrayList<>();
        for (Map<String, String> record : actualRecords) {
            String hashCode = record.get("hashCode");
            String cacheKey = getCacheKey(hashCode);
            if (Cache.exists(cacheKey)) {
                logger.info("Duplicated record: " + record);
                continue;
            }
            result.add(record);
        }
        return result;
    }


    public static void cacheHashCodes(List<Map<String, String>> records) {
        for (Map<String, String> record : records) {
            String cacheKey = getCacheKey(record.get("hashCode"));
            Cache.set(cacheKey, "exists");
        }
    }
}
