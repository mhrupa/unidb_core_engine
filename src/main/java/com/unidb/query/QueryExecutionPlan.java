package com.unidb.query;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class QueryExecutionPlan {
    public ExecutionPlan generatePlan(ParsedQuery query) {
        log.info("Generating execution plan for query: {}", query.getType());
        return new ExecutionPlan(query);
    }
}