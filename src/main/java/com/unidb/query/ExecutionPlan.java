package com.unidb.query;

public class ExecutionPlan {
    private ParsedQuery optimizedQuery;
    private float optimizedScore;
    private double optimizedCost;
    private int numJoins;
    private int numFilters;
    private int dataSize;

    public ExecutionPlan(ParsedQuery query) {
        this.optimizedQuery = query;
    }

    public ExecutionPlan(int numJoins, int numFilters, int dataSize) {
        this.dataSize = dataSize;
        this.numJoins = numJoins;
        this.numFilters = numFilters;
    }

    public ParsedQuery getOptimizedQuery() {
        return optimizedQuery;
    }

    public void setOptimizedQuery(ParsedQuery optimizedQuery) {
        this.optimizedQuery = optimizedQuery;
    }

    public float getOptimizedScore() {
        return optimizedScore;
    }

    public void setOptimizedScore(float optimizedScore) {
        this.optimizedScore = optimizedScore;
    }

    public double getOptimizedCost() {
        return optimizedCost;
    }

    public void setOptimizedCost(double optimizedCost) {
        this.optimizedCost = optimizedCost;
    }

    public int getNumJoins() {
        return numJoins;
    }

    public void setNumJoins(int numJoins) {
        this.numJoins = numJoins;
    }

    public int getNumFilters() {
        return numFilters;
    }

    public void setNumFilters(int numFilters) {
        this.numFilters = numFilters;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

}
