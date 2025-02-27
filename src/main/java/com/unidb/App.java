package com.unidb;

import com.unidb.query.CostBasedQueryOptimizer;
import com.unidb.query.ExecutionPlan;
import com.unidb.query.HybridQueryOptimizer;
import com.unidb.query.MachineLearningOptimizer;
import com.unidb.query.SqlOptimizer;

import lombok.extern.slf4j.Slf4j;

/**
 * Hello world!
 *
 */
@Slf4j
public class App {
    public static void main(String[] args) {
        // log.info("Hello World!");
        try {
            testQueryOptimiser();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        // try {
        //     String query = "SELECT * FROM Orders JOIN Customers ON Orders.customer_id = Customers.id WHERE YEAR(Orders.date) = 2021";
        //     String optimizedQuery = SqlOptimizer.generateOptimizedQuery(query);
        //     System.out.println("Optimized SQL: " + optimizedQuery);
        // } catch (Exception e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }

    }

    private static void testQueryOptimiser() throws Exception {
        CostBasedQueryOptimizer costOptimizer = new CostBasedQueryOptimizer();
        MachineLearningOptimizer mlOptimizer = new MachineLearningOptimizer();

        // Create a Hybrid Optimizer
        HybridQueryOptimizer hybridOptimizer = new HybridQueryOptimizer(costOptimizer, mlOptimizer);

        // Sample Execution Plan with dummy data
        ExecutionPlan plan = new ExecutionPlan(3, 5, 1000); // 3 Joins, 5 Filters, 1000 Data Size

        log.info("Initial Execution Plan Cost: {}", plan.getOptimizedCost());

        // Optimize the Execution Plan
        ExecutionPlan optimizedPlan = hybridOptimizer.optimize(plan);

        log.info("Final Optimized Execution Plan Cost: {}", optimizedPlan.getOptimizedCost());
    }

}
