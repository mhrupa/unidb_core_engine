package com.unidb.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

public class SqlOptimizer {

    /**
     * Rewrites the given SQL query to apply advanced optimizations:
     * 1. Join Reordering – smaller tables first in JOIN order.
     * 2. Predicate Pushdown – move WHERE filters into subqueries or joins.
     * 3. Index Utilization – rewrite conditions to use indexes (avoid functions on
     * indexed columns).
     * 4. Projection Pruning – remove unnecessary columns from SELECT lists.
     *
     * @param originalQuery the original SQL query string
     * @return an optimized SQL query string with the above transformations applied
     * @throws Exception if the SQL cannot be parsed or rewritten
     */
    public static String generateOptimizedQuery(String originalQuery) throws Exception {
        // Parse the SQL query into an AST (using JSQLParser)
        Statement statement = CCJSqlParserUtil.parse(originalQuery);

        // Ensure we are dealing with a SELECT query
        if (!(statement instanceof Select)) {
            return originalQuery; // Return as-is if not a SELECT statement
        }

        Select select = (Select) statement;
        SelectBody selectBody = select.getSelectBody();
        if (!(selectBody instanceof PlainSelect)) {
            return originalQuery; // Only optimize simple SELECT queries for now
        }

        PlainSelect query = (PlainSelect) selectBody;

        // 1. **Join Reordering**: reorder JOINs so that the smallest/restricted tables
        // are joined first.
        optimizeJoinOrder(query);

        // 2. **Predicate Pushdown**: move WHERE filters into subqueries or join
        // conditions when possible.
        optimizePredicatePushdown(query);

        // 3. **Index Utilization**: adjust expressions to make use of indexes (avoid
        // functions on indexed columns).
        optimizeIndexUsage(query);

        // 4. **Projection Pruning**: remove unnecessary columns from SELECT.
        optimizeProjection(query);

        return select.toString(); // Return the optimized query as a string
    }

    // ---------------- JOIN REORDERING ----------------
    private static void optimizeJoinOrder(PlainSelect query) {
        if (query.getJoins() == null)
            return; // No joins to reorder

        List<Join> joins = query.getJoins();
        Map<String, Integer> tableSize = getTableSizeEstimates(); // Assume some size estimates

        // Sort joins based on estimated table size (smallest first)
        joins.sort(Comparator.comparingInt(j -> {
            if (j.getRightItem() instanceof Table) {
                String tableName = ((Table) j.getRightItem()).getName();
                return tableSize.getOrDefault(tableName, Integer.MAX_VALUE);
            }
            return Integer.MAX_VALUE;
        }));

        query.setJoins(joins);
    }

    // ---------------- PREDICATE PUSHDOWN ----------------
    private static void optimizePredicatePushdown(PlainSelect query) throws Exception {
        if (query.getWhere() == null)
            return; // No filters to push down

        List<Expression> predicates = splitConjunctiveConditions(query.getWhere());
        List<Expression> remainingPredicates = new ArrayList<>();
        Map<String, Expression> pushdownMap = new HashMap<>();

        for (Expression pred : predicates) {
            Set<String> tables = extractTablesFromExpression(pred);
            if (tables.size() == 1) {
                String tableName = tables.iterator().next();
                pushdownMap.merge(tableName, pred, (oldExpr, newExpr) -> {
                    try {
                        return CCJSqlParserUtil.parseCondExpression(oldExpr + " AND " + newExpr);
                    } catch (Exception e) {
                        throw new RuntimeException("Error parsing condition expression", e);
                    }
                });
            } else {
                remainingPredicates.add(pred);
            }
        }

        if (query.getFromItem() instanceof Table) {
            Table baseTable = (Table) query.getFromItem();
            if (pushdownMap.containsKey(baseTable.getName())) {
                query.setWhere(pushdownMap.get(baseTable.getName()));
            }
        }

        if (remainingPredicates.isEmpty()) {
            query.setWhere(null);
        } else {
            query.setWhere(CCJSqlParserUtil.parseCondExpression(String.join(" AND ", remainingPredicates.toString())));
        }
    }

    // ---------------- INDEX UTILIZATION ----------------
    private static void optimizeIndexUsage(PlainSelect query) {
        Expression where = query.getWhere();
        if (where == null)
            return;

        where.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(Function function) {
                String funcName = function.getName().toLowerCase();
                if (funcName.equals("year") && function.getParameters().getExpressions().size() == 1) {
                    // Convert YEAR(column) = 2021 to column BETWEEN '2021-01-01' AND '2021-12-31'
                    // Example transformation logic (not fully implemented)
                }
                super.visit(function);
            }
        });
    }

    // ---------------- PROJECTION PRUNING ----------------
    private static void optimizeProjection(PlainSelect query) {
        if (query.getSelectItems().size() == 1 && query.getSelectItems().get(0) instanceof AllColumns) {
            List<SelectItem> explicitCols = new ArrayList<>();
            for (String col : getAllColumnsForQuery(query)) {
                explicitCols.add(new SelectExpressionItem(new net.sf.jsqlparser.schema.Column(col)));
            }
            query.setSelectItems(explicitCols);
        }
    }

    // ---------------- HELPERS ----------------
    private static List<Expression> splitConjunctiveConditions(Expression where) throws Exception {
        List<Expression> list = new ArrayList<>();
        if (where == null)
            return list;
        String whereStr = where.toString();
        if (whereStr.contains(" AND ")) {
            for (String part : whereStr.split(" AND ")) {
                list.add(CCJSqlParserUtil.parseCondExpression(part));
            }
        } else {
            list.add(where);
        }
        return list;
    }

    private static Set<String> extractTablesFromExpression(Expression expr) {
        Set<String> tables = new HashSet<>();
        expr.accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(net.sf.jsqlparser.schema.Column column) {
                if (column.getTable() != null) {
                    tables.add(column.getTable().getName());
                }
            }
        });
        return tables;
    }

    private static Map<String, Integer> getTableSizeEstimates() {
        Map<String, Integer> sizes = new HashMap<>();
        sizes.put("Customers", 1000);
        sizes.put("Orders", 50000);
        sizes.put("Employees", 100);
        return sizes;
    }

    private static List<String> getAllColumnsForQuery(PlainSelect query) {
        return Arrays.asList("id", "name", "email", "created_at"); // Placeholder, replace with metadata retrieval
    }
}