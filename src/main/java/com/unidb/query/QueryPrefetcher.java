package com.unidb.query;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class QueryPrefetcher {
    private final AdaptiveQueryCache queryCache;

    public QueryPrefetcher(AdaptiveQueryCache queryCache) {
        this.queryCache = queryCache;
    }

    public void prefetchRelatedQueries(String query) {
        List<String> relatedQueries = getRelatedQueries(query);
        for (String relatedQuery : relatedQueries) {
            if (!queryCache.contains(relatedQuery)) {
                log.info("Prefetching query: {}", relatedQuery);
                queryCache.put(relatedQuery, "PREFETCHED_RESULT");
            }
        }
    }

    private List<String> getRelatedQueries(String query) {
        List<String> relatedQueries = new ArrayList<>();
        if (query.contains("SELECT")) {
            relatedQueries.add(query.replace("SELECT", "COUNT"));
        }
        return relatedQueries;
    }
}

