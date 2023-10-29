package org.pradheeparul.metrics;

import java.util.List;
import java.util.Map;

/**
 * UniqueLoginMetric
 *
 * @author PradheepKumarA
 */
public class UniqueLoginMetric implements Metric {

    private long totalUniqueLogins = 0L;

    @Override
    public void computeMetric(List<Map<String, String>> records) {
        for (Map<String, String> record : records) {
            if (!record.getOrDefault("eventType", "").equals("login")) {
                continue;
            }
            totalUniqueLogins += 1;
        }
    }

    public void logMetrics(String shardId) {
        System.out.println("Total Unique Logins (" + shardId + ") : " + totalUniqueLogins);
    }

}
