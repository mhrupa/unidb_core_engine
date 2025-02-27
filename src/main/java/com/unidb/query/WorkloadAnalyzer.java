package com.unidb.query;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WorkloadAnalyzer {
    private final Map<String, Integer> queryFrequency;

    public WorkloadAnalyzer() {
        this.queryFrequency = new ConcurrentHashMap<>();
    }

    public void analyze(String query) {
        queryFrequency.put(query, queryFrequency.getOrDefault(query, 0) + 1);
        log.info("Analyzed query workload: {} executed {} times", query, queryFrequency.get(query));
    }
}