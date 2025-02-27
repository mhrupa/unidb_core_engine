package com.unidb.query;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdaptiveQueryCache {
    private final Map<String, CacheEntry> cache;
    private final int capacity;
    private long expirationTime;
    private final long minExpirationTime = 10000; // 10s
    private final long maxExpirationTime = 60000; // 60s
    
    public AdaptiveQueryCache(int capacity, long initialExpirationTime) {
        this.capacity = capacity;
        this.expirationTime = initialExpirationTime;
        this.cache = new LinkedHashMap<>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
                return size() > AdaptiveQueryCache.this.capacity || eldest.getValue().isExpired();
            }
        };
    }

    public boolean contains(String query) {
        return cache.containsKey(query) && !cache.get(query).isExpired();
    }

    public String get(String query) {
        CacheEntry entry = cache.get(query);
        return (entry != null && !entry.isExpired()) ? entry.result : null;
    }

    public void put(String query, String result) {
        cache.put(query, new CacheEntry(result, System.currentTimeMillis() + expirationTime));
        log.info("Cached result for query: {} with expiration: {} ms", query, expirationTime);
        adjustExpirationTime(query);
    }
    
    private void adjustExpirationTime(String query) {
        if (cache.containsKey(query)) {
            expirationTime = Math.min(expirationTime + 5000, maxExpirationTime);
        } else {
            expirationTime = Math.max(expirationTime - 5000, minExpirationTime);
        }
        log.info("Adaptive expiration time adjusted to: {} ms", expirationTime);
    }
}