package com.unidb.query;

import java.util.TreeMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class BPlusTreeIndexManager extends IndexManager{
    @SuppressWarnings("unused")
    private final int order;
    private final TreeMap<Long, Long> index;

    public BPlusTreeIndexManager(int order) {
        this.order = order;
        this.index = new TreeMap<>();
    }

    @Override
    public boolean containsKey(long key) {
        return index.containsKey(key);
    }

    @Override
    public long lookup(long key) {
        log.info("B+ Tree Index lookup for key: {}", key);
        return index.getOrDefault(key, key);
    }

    @Override
    public void insert(long key) {
        index.put(key, key);
        log.info("Inserted key into B+ Tree Index: {}", key);
    }
}