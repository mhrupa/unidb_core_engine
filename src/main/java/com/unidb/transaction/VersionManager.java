package com.unidb.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class VersionManager {
    private final ConcurrentHashMap<Long, Map<Long, String>> versionStore;

    public VersionManager() {
        this.versionStore = new ConcurrentHashMap<>();
    }

    public String readVersion(long transactionId, long key, IsolationLevel isolationLevel) {
        Map<Long, String> versions = versionStore.get(key);
        if (versions == null) return null;
        
        if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
            return versions.values().iterator().next(); // Read the most recent version
        }
        
        return versions.entrySet().stream()
                .filter(entry -> entry.getKey() <= transactionId)
                .max(Map.Entry.comparingByKey())
                .map(Map.Entry::getValue)
                .orElse(null);
    }

    public void writeVersion(long transactionId, long key, String value) {
        versionStore.computeIfAbsent(key, k -> new HashMap<>()).put(transactionId, value);
        log.info("Transaction {} wrote version for key {}: {}", transactionId, key, value);
    }

    public void commitTransaction(long transactionId) {
        versionStore.forEach((key, versions) -> versions.keySet().removeIf(txId -> txId < transactionId));
        log.info("Transaction {} committed versions.", transactionId);
    }

    public void rollbackTransaction(long transactionId) {
        versionStore.forEach((key, versions) -> versions.remove(transactionId));
        log.info("Transaction {} rolled back versions.", transactionId);
    }
    
    public void cleanupOldVersions() {
        versionStore.forEach((key, versions) -> {
            if (versions.size() > 1) {
                long minTransaction = Collections.min(versions.keySet());
                versions.keySet().removeIf(txId -> txId < minTransaction);
                log.info("Cleaned up old versions for key {}.", key);
            }
        });
    }
}

