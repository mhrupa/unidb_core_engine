package com.unidb.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class LockManager {
    private final ConcurrentHashMap<Long, ReentrantReadWriteLock> locks;
    private final ConcurrentHashMap<Long, Long> lockOwners;
    private final Map<Long, Set<Long>> waitForGraph;

    public LockManager() {
        this.locks = new ConcurrentHashMap<>();
        this.lockOwners = new ConcurrentHashMap<>();
        this.waitForGraph = new HashMap<>();
    }

    public synchronized void acquireLock(long transactionId, long resourceId, boolean isWriteLock) {
        locks.putIfAbsent(resourceId, new ReentrantReadWriteLock());
        ReentrantReadWriteLock lock = locks.get(resourceId);
        if (isWriteLock) {
            lock.writeLock().lock();
        } else {
            lock.readLock().lock();
        }
        lockOwners.put(resourceId, transactionId);
        waitForGraph.computeIfAbsent(transactionId, k -> new HashSet<>()).add(resourceId);
        log.info("Transaction {} acquired {} lock on resource {}", transactionId, isWriteLock ? "WRITE" : "READ",
                resourceId);
        if (detectDeadlock(transactionId)) {
            log.warn("Deadlock detected! Rolling back transaction {}", transactionId);
            releaseLocks(transactionId);
        }
    }

    public synchronized void releaseLocks(long transactionId) {
        waitForGraph.remove(transactionId);
        lockOwners.forEach((resourceId, ownerId) -> {
            if (ownerId == transactionId) {
                ReentrantReadWriteLock lock = locks.get(resourceId);
                if (lock != null) {
                    lock.writeLock().unlock();
                    lock.readLock().unlock();
                }
                lockOwners.remove(resourceId);
                log.info("Transaction {} released locks on resource {}", transactionId, resourceId);
            }
        });
    }

    private boolean detectDeadlock(long transactionId) {
        Set<Long> visited = new HashSet<>();
        return hasCycle(transactionId, visited);
    }

    private boolean hasCycle(long transactionId, Set<Long> visited) {
        if (visited.contains(transactionId)) {
            return true;
        }
        visited.add(transactionId);
        Set<Long> dependencies = waitForGraph.get(transactionId);
        if (dependencies != null) {
            for (long dep : dependencies) {
                if (hasCycle(dep, visited)) {
                    return true;
                }
            }
        }
        visited.remove(transactionId);
        return false;
    }
}