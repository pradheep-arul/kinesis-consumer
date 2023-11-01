package com.pradheeparul.metrics;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MetricProcessor
 *
 * @author PradheepKumarA
 */
public class MetricProcessor {
    private final List<Metric> metrics;
    private final String shardId;

    public MetricProcessor(String shardId) {
        this.shardId = shardId;
        // Initialize the metric calculators
        metrics = new ArrayList<>();
        metrics.add(new LoginMetric());
        metrics.add(new CountryLoginMetric());
        // We could register more metrics in the future
    }

    public void processMetrics(List<Map<String, String>> records) {
        // Compute all registered metrics
        for (Metric calculator : metrics) {
            calculator.computeMetric(records);
        }
    }

    public void logMetrics() {
        for (Metric calculator : metrics) {
            calculator.logMetrics(shardId);
        }
    }

}