package com.unidb.query;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.unidb.transaction.IsolationLevel;
import com.unidb.transaction.VersionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryExecutor {
    private final VersionManager versionManager;
    private final QueryOptimizer queryOptimizer;
    private final QueryParser queryParser;
    private final QueryExecutionPlan executionPlan;
    private final IndexManager indexManager;
    private final ExecutorService executorService;
    private final AdaptiveQueryCache queryCache;
    private final QueryPrefetcher queryPrefetcher;
    private final WorkloadAnalyzer workloadAnalyzer;
    private final MachineLearningOptimizer mlOptimizer;

    public QueryExecutor(VersionManager versionManager) {
        this.versionManager = versionManager;
        this.queryOptimizer = new CostBasedQueryOptimizer();
        this.queryParser = new QueryParser();
        this.executionPlan = new QueryExecutionPlan();
        this.indexManager = new BPlusTreeIndexManager(4); // B+ Tree order 4
        this.executorService = Executors.newFixedThreadPool(4); // Parallel execution
        this.queryCache = new AdaptiveQueryCache(100, 30000); // Adaptive cache
        this.queryPrefetcher = new QueryPrefetcher(queryCache);
        this.workloadAnalyzer = new WorkloadAnalyzer();
        this.mlOptimizer = new MachineLearningOptimizer("ml_model.h5");
    }

    public String executeQuery(String sqlQuery, long transactionId, IsolationLevel isolationLevel)
            throws Exception {
        workloadAnalyzer.analyze(sqlQuery);

        if (queryCache.contains(sqlQuery)) {
            log.info("Cache hit for query: {}", sqlQuery);
            return queryCache.get(sqlQuery);
        }

        ParsedQuery parsedQuery = queryParser.parse(sqlQuery);
        ExecutionPlan plan = mlOptimizer.optimize(queryOptimizer.optimize(executionPlan.generatePlan(parsedQuery)));

        String result = executorService.submit(() -> processQuery(plan, transactionId, isolationLevel)).get();
        queryCache.put(sqlQuery, result);
        queryPrefetcher.prefetchRelatedQueries(sqlQuery);
        mlOptimizer.updateModel(plan, result);
        return result;
    }

    private String processQuery(ExecutionPlan plan, long transactionId, IsolationLevel isolationLevel) {
        switch (plan.getOptimizedQuery().getType()) {
            case READ:
                return executeReadQuery(transactionId, plan.getOptimizedQuery().getKey(), isolationLevel);
            case WRITE:
                executeWriteQuery(transactionId, plan.getOptimizedQuery().getKey(),
                        plan.getOptimizedQuery().getValue());
                return "WRITE SUCCESS";
            case COMMIT:
                executeCommit(transactionId);
                return "COMMIT SUCCESS";
            case ROLLBACK:
                executeRollback(transactionId);
                return "ROLLBACK SUCCESS";
            default:
                throw new IllegalArgumentException("Unknown query type");
        }
    }

    private String executeReadQuery(long transactionId, long key, IsolationLevel isolationLevel) {
        key = queryOptimizer.optimizeReadKey(key);
        if (indexManager.containsKey(key)) {
            key = indexManager.lookup(key);
        }
        String result = versionManager.readVersion(transactionId, key, isolationLevel);
        log.info("Executed READ query: Transaction {} fetched key {} -> {}", transactionId, key, result);
        return result;
    }

    private void executeWriteQuery(long transactionId, long key, String value) {
        key = queryOptimizer.optimizeWriteKey(key);
        versionManager.writeVersion(transactionId, key, value);
        indexManager.insert(key);
        log.info("Executed WRITE query: Transaction {} set key {} -> {}", transactionId, key, value);
    }

    private void executeCommit(long transactionId) {
        versionManager.commitTransaction(transactionId);
        log.info("Transaction {} committed successfully.", transactionId);
    }

    private void executeRollback(long transactionId) {
        versionManager.rollbackTransaction(transactionId);
        log.info("Transaction {} rolled back successfully.", transactionId);
    }
}