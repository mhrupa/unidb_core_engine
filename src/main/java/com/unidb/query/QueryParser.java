package com.unidb.query;

class QueryParser {
    public ParsedQuery parse(String sqlQuery) {
        sqlQuery = sqlQuery.trim().toUpperCase();
        if (sqlQuery.startsWith("SELECT")) {
            long key = Long.parseLong(sqlQuery.replaceAll("[^0-9]", ""));
            return new ParsedQuery(QueryType.READ, key, null);
        } else if (sqlQuery.startsWith("INSERT") || sqlQuery.startsWith("UPDATE")) {
            String[] parts = sqlQuery.split(" ");
            long key = Long.parseLong(parts[1]);
            String value = parts[3];
            return new ParsedQuery(QueryType.WRITE, key, value);
        } else if (sqlQuery.startsWith("COMMIT")) {
            return new ParsedQuery(QueryType.COMMIT, -1, null);
        } else if (sqlQuery.startsWith("ROLLBACK")) {
            return new ParsedQuery(QueryType.ROLLBACK, -1, null);
        }
        throw new IllegalArgumentException("Invalid SQL Query");
    }
}