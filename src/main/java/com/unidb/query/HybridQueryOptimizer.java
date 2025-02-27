package com.unidb.query;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HybridQueryOptimizer extends QueryOptimizer {
    private final CostBasedQueryOptimizer costOptimizer;
    private final MachineLearningOptimizer mlOptimizer;

    public HybridQueryOptimizer(CostBasedQueryOptimizer costOptimizer, MachineLearningOptimizer mlOptimizer) {
        this.costOptimizer = costOptimizer;
        this.mlOptimizer = mlOptimizer;
    }

    @Override
    public ExecutionPlan optimize(ExecutionPlan plan) throws Exception {
        log.info("Applying Hybrid Optimization to Execution Plan.");
        plan = costOptimizer.optimize(plan);
        plan = mlOptimizer.optimize(plan);
        return plan;
    }
}
