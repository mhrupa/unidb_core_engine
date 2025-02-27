package com.unidb.query;

public class CacheEntry {
    final String result;
    final long expiryTime;

    public CacheEntry(String result, long expiryTime) {
        this.result = result;
        this.expiryTime = expiryTime;
    }

    public boolean isExpired() {
        return System.currentTimeMillis() > expiryTime;
    }
}
