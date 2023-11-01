package com.pradheeparul.metrics;

import java.util.List;
import java.util.Map;

/**
 * Metric
 *
 * @author PradheepKumarA
 */
public interface Metric {
    void computeMetric(List<Map<String, String>> records);

    void logMetrics(String shardId);
}