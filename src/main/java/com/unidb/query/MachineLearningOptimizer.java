package com.unidb.query;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.deeplearning4j.datasets.iterator.utilty.ListDataSetIterator;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.nd4j.evaluation.regression.RegressionEvaluation;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.memory.conf.WorkspaceConfiguration;
import org.nd4j.linalg.api.memory.enums.AllocationPolicy;
import org.nd4j.linalg.api.memory.enums.LearningPolicy;
import org.nd4j.linalg.api.memory.enums.MirroringPolicy;
import org.nd4j.linalg.api.memory.enums.SpillPolicy;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.dataset.api.preprocessor.NormalizerMinMaxScaler;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.learning.config.Adam;
import org.nd4j.linalg.lossfunctions.LossFunctions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MachineLearningOptimizer {
    private MultiLayerNetwork model;
    private double learningRate = 0.001;
    private int batchSize = 128; // Increased batch size for performance
    private int epochs = 20; // Increased training epochs for better learning
    private ExecutorService executorService = Executors.newFixedThreadPool(8); // Optimized thread pool
    private String modelFilePath = "ml_model.zip";
    private String hashFilePath = "ml_model.hash";

    public MachineLearningOptimizer() {
        if (!verifyIntegrity() || !loadModel()) {
            initializeNewModel();
        }
    }

    public MachineLearningOptimizer(String modelFilePath) {
        this.modelFilePath = modelFilePath;
        this.hashFilePath = modelFilePath + ".hash";
        if (!verifyIntegrity() || !loadModel()) {
            initializeNewModel();
        }
    }

    private boolean loadModel() {
        try {
            File modelFile = new File(modelFilePath);
            if (modelFile.exists()) {
                model = MultiLayerNetwork.load(modelFile, true);
                log.info("Loaded ML model from disk.");
                return true;
            }
        } catch (IOException e) {
            log.error("Failed to load ML model from disk.", e);
        }
        return false;
    }

    private void saveModel() {
        try {
            model.save(new File(modelFilePath), true);
            log.info("Saved ML model to disk.");
        } catch (IOException e) {
            log.error("Failed to save ML model to disk.", e);
        }
    }

    private void initializeNewModel() {
        MultiLayerConfiguration config = new NeuralNetConfiguration.Builder()
                .seed(123)
                .updater(new Adam(learningRate))
                .list()
                .layer(0, new DenseLayer.Builder().nIn(1).nOut(128)
                        .activation(Activation.RELU)
                        .build())
                .layer(1, new DenseLayer.Builder().nIn(128).nOut(64)
                        .activation(Activation.RELU)
                        .build())
                .layer(2, new OutputLayer.Builder(LossFunctions.LossFunction.MSE)
                        .activation(Activation.IDENTITY)
                        .nIn(64).nOut(1)
                        .build())
                .build();

        model = new MultiLayerNetwork(config);
        model.init();
        model.setListeners(new ScoreIterationListener(10));
        log.info("Initialized new ML model with optimized hyperparameters for scalability.");
    }

    public ExecutionPlan optimize(ExecutionPlan plan) {
        Instant startTime = Instant.now();
        INDArray input = prepareInput(plan);
        Future<INDArray> futureOutput = executorService.submit(() -> model.output(input));
        try {
            INDArray output = futureOutput.get();
            logExecutionTime("optimize", startTime);
            return applyOptimization(plan, output);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error during inference execution", e);
            return plan;
        }
    }

    private INDArray prepareInput(ExecutionPlan plan) {
        return Nd4j.create(new float[] { plan.getOptimizedQuery().getKey() }, new int[] { 1, 1 });
    }

    private ExecutionPlan applyOptimization(ExecutionPlan plan, INDArray output) {
        log.info("ML Optimizer adjusted execution plan with predicted score: {}", output.getFloat(0));
        plan.setOptimizedScore(output.getFloat(0)); // Setting the optimized score to the plan
        return plan;
    }

    public void updateModel(List<ExecutionPlan> plans, List<String> executionResults) {
        log.info("Updating ML model with batch query execution feedback for scalability");
        List<DataSet> trainingData = prepareTrainingData(plans, executionResults);
        trainModel(trainingData);
        evaluateModel(trainingData);
        saveModel();
    }

    public void updateModel(ExecutionPlan plan, String executionResult) {
        List<ExecutionPlan> planList = new ArrayList<>();
        planList.add(plan);

        List<String> resultList = new ArrayList<>();
        resultList.add(executionResult);

        updateModel(planList, resultList); // Calls the existing batch method
    }

    private void evaluateModel(List<DataSet> testData) {
        log.info("Evaluating ML model performance on batch data...");
        RegressionEvaluation eval = new RegressionEvaluation(1);
        for (DataSet data : testData) {
            INDArray predicted = model.output(data.getFeatures());
            eval.eval(data.getLabels(), predicted);
        }

        log.info("Model Evaluation Metrics:");
        log.info("MSE: {}", eval.meanSquaredError(0));
        log.info("MAE: {}", eval.meanAbsoluteError(0));
        log.info("R Squared: {}", eval.rSquared(0));
    }

    private List<DataSet> prepareTrainingData(List<ExecutionPlan> plans, List<String> executionResults) {
        List<DataSet> dataSetList = new ArrayList<>();
        for (int i = 0; i < plans.size(); i++) {
            INDArray features = prepareInput(plans.get(i));
            INDArray labels = Nd4j.create(new float[] { Float.parseFloat(executionResults.get(i)) },
                    new int[] { 1, 1 });
            dataSetList.add(new DataSet(features, labels));
        }
        return dataSetList;
    }

    private void trainModel(List<DataSet> trainingData) {
        NormalizerMinMaxScaler normalizer = new NormalizerMinMaxScaler(0, 1);
        ListDataSetIterator<DataSet> iterator = new ListDataSetIterator<>(trainingData, batchSize);
        normalizer.fit(iterator);
        iterator.setPreProcessor(normalizer);

        executorService.submit(() -> {
            for (int i = 0; i < epochs; i++) {
                model.fit(iterator);
            }
            log.info("ML model retrained with batch data for improved scalability.");
            saveModel();
        });
    }

    public void enableFaultTolerance() {
        log.info("Enabling fault tolerance mechanisms...");
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown detected, saving model state...");
            saveModel();
        }));
    }

    public void recoverFromFailure() {
        log.info("Attempting to recover from failure...");
        if (loadModel()) {
            log.info("Model successfully recovered from disk.");
        } else {
            log.warn("No saved model found. Initializing a new model.");
            initializeNewModel();
        }
    }

    public void optimizeMemory() {
        log.info("Optimizing memory usage...");
        Nd4j.getMemoryManager().setAutoGcWindow(5000);

        WorkspaceConfiguration workspaceConfig = WorkspaceConfiguration.builder()
                .initialSize(100 * 1024L * 1024L) // 100MB initial workspace size
                .policyAllocation(AllocationPolicy.OVERALLOCATE)
                .policySpill(SpillPolicy.REALLOCATE)
                .policyLearning(LearningPolicy.FIRST_LOOP)
                .policyMirroring(MirroringPolicy.FULL)
                .build();

        Nd4j.getWorkspaceManager().setDefaultWorkspaceConfiguration(workspaceConfig);
        log.info("Memory optimization applied.");
    }

    private void logExecutionTime(String operation, Instant startTime) {
        Instant endTime = Instant.now();
        long timeElapsed = endTime.toEpochMilli() - startTime.toEpochMilli();
        log.info("Execution time for {}: {} ms", operation, timeElapsed);
    }

    private boolean verifyIntegrity() {
        try {
            File modelFile = new File(modelFilePath);
            File hashFile = new File(hashFilePath);
            if (!modelFile.exists() || !hashFile.exists()) {
                log.warn("Model or hash file not found, skipping integrity check.");
                return false;
            }

            String storedHash = new String(java.nio.file.Files.readAllBytes(hashFile.toPath()));
            String computedHash = computeHash(modelFile);
            if (!storedHash.equals(computedHash)) {
                log.error("Model integrity check failed. Possible corruption or tampering detected!");
                return false;
            }
            log.info("Model integrity verified.");
            return true;
        } catch (IOException | NoSuchAlgorithmException e) {
            log.error("Error verifying model integrity.", e);
            return false;
        }
    }

    private String computeHash(File file) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = new FileInputStream(file)) {
            byte[] byteArray = new byte[1024];
            int bytesRead;
            while ((bytesRead = fis.read(byteArray)) != -1) {
                digest.update(byteArray, 0, bytesRead);
            }
        }
        return Base64.getEncoder().encodeToString(digest.digest());
    }

    public void optimizePerformance() {
        log.info("Optimizing ML execution performance...");
        Nd4j.getMemoryManager().setAutoGcWindow(10000); // Increase garbage collection window
        log.info("Increased GC window to improve performance.");

        WorkspaceConfiguration workspaceConfig = WorkspaceConfiguration.builder()
                .initialSize(200 * 1024L * 1024L) // Increased workspace memory
                .policyAllocation(AllocationPolicy.STRICT)
                .policySpill(SpillPolicy.REALLOCATE)
                .policyLearning(LearningPolicy.OVER_TIME)
                .policyMirroring(MirroringPolicy.FULL)
                .build();

        Nd4j.getWorkspaceManager().setDefaultWorkspaceConfiguration(workspaceConfig);
        log.info("Optimized memory workspace settings for performance.");
    }

}
