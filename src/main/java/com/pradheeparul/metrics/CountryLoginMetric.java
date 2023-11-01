package com.pradheeparul.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CountryLoginMetric
 *
 * @author PradheepKumarA
 */
public class CountryLoginMetric implements Metric {


    Map<String, Long> countryLogins = new HashMap<>();

    @Override
    public void computeMetric(List<Map<String, String>> records) {
        for (Map<String, String> record : records) {
            if (!record.getOrDefault("eventType", "").equals("login")) {
                continue;
            }
            String country = record.getOrDefault("country", "--");
            countryLogins.put(country, countryLogins.getOrDefault(country, 0L) + 1);
        }
    }

    public void logMetrics(String shardId) {
        for (Map.Entry<String, Long> entry : countryLogins.entrySet())
            System.out.println("        Country = " + entry.getKey() + ", Unique Logins = " + entry.getValue());
    }

}
