package com.unidb.transaction;

public enum IsolationLevel {
    READ_UNCOMMITTED, // Transactions can read uncommitted data (dirty reads allowed)
    READ_COMMITTED, // Transactions only read committed data
    REPEATABLE_READ, // Ensures the same result is read multiple times within the transaction
    SERIALIZABLE // Full isolation, transactions execute sequentially
}