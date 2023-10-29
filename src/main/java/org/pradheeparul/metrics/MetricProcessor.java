package org.pradheeparul.metrics;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * MetricProcessor
 *
 * @author PradheepKumarA
 */
public class MetricProcessor {
    private List<Metric> metrics;
    private String shardId;

    public MetricProcessor(String shardId) {
        this.shardId = shardId;
        // Initialize the metric calculators
        metrics = new ArrayList<>();
        metrics.add(new UniqueLoginMetric());
        metrics.add(new UniqueCountryLoginMetric());
    }

    public void processMetrics(List<Map<String, String>> records) {
        // Notify all metric calculators when new data arrives
        for (Metric calculator : metrics) {
            calculator.computeMetric(records);
        }
    }

    public void logMetrics() {
        // Notify all metric calculators when new data arrives
        for (Metric calculator : metrics) {
            calculator.logMetrics(shardId);
        }
    }

}