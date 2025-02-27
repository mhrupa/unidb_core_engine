package com.unidb.query;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class IndexManager {
    private final Map<Long, Long> index;

    public IndexManager() {
        this.index = new java.util.HashMap<>();
    }

    public boolean containsKey(long key) {
        return index.containsKey(key);
    }

    public long lookup(long key) {
        log.info("Index lookup for key: {}", key);
        return index.getOrDefault(key, key);
    }

    public void insert(long key) {
        index.put(key, key);
        log.info("Inserted key into index: {}", key);
    }
}
