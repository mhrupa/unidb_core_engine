package com.unidb.transaction;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.unidb.storage.WalManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransactionManager {
    private final WalManager walManager;
    private final ConcurrentHashMap<Long, String> activeTransactions;
    private final LockManager lockManager;
    private final VersionManager versionManager;

    public TransactionManager(WalManager walManager) {
        this.walManager = walManager;
        this.activeTransactions = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
        this.versionManager = new VersionManager();
    }

    // Start a new transaction with a specified isolation level
    public long beginTransaction(IsolationLevel isolationLevel) {
        long transactionId = System.nanoTime(); // Unique transaction ID
        activeTransactions.put(transactionId, isolationLevel.name());
        log.info("Transaction {} started with isolation level: {}", transactionId, isolationLevel);
        return transactionId;
    }

    // Commit a transaction
    public void commitTransaction(long transactionId) throws IOException {
        if (activeTransactions.containsKey(transactionId)) {
            walManager.logWrite(transactionId, -1, "COMMIT".getBytes());
            activeTransactions.remove(transactionId);
            lockManager.releaseLocks(transactionId);
            versionManager.commitTransaction(transactionId);
            versionManager.cleanupOldVersions();
            log.info("Transaction {} committed.", transactionId);
        } else {
            throw new IllegalStateException("Transaction " + transactionId + " not found.");
        }
    }

    // Rollback a transaction
    public void rollbackTransaction(long transactionId) throws IOException {
        if (activeTransactions.containsKey(transactionId)) {
            walManager.logWrite(transactionId, -1, "ROLLBACK".getBytes());
            activeTransactions.remove(transactionId);
            lockManager.releaseLocks(transactionId);
            versionManager.rollbackTransaction(transactionId);
            versionManager.cleanupOldVersions();
            log.info("Transaction {} rolled back.", transactionId);
        } else {
            throw new IllegalStateException("Transaction " + transactionId + " not found.");
        }
    }
}
