package com.unidb.query;

class ParsedQuery {
    private final QueryType type;
    private final long key;
    private final String value;

    public ParsedQuery(QueryType type, long key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public QueryType getType() { return type; }
    public long getKey() { return key; }
    public String getValue() { return value; }
}