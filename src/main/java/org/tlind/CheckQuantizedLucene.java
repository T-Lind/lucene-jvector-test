package org.tlind;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public class CheckQuantizedLucene {
    private static final int memorySleepAmount = 100; // Sleep interval in milliseconds -- set as needed
    private static final int numberOfVectorsToIndex = 100000; // Adjust based on your dataset size
    private static volatile long maxMemoryUsage = 0;

    public static void main(String[] args) throws Exception {
        Thread memoryMonitor = new Thread(CheckQuantizedLucene::monitorMemoryUsage);
        memoryMonitor.start();

        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("Lucene Bench\nTest run on: " + timeStamp);
        System.out.println("(Heap space available is " + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB)");

        long startTime = System.currentTimeMillis();

        Directory groundTruthIndex = new ByteBuffersDirectory();
        Directory queryIndex = new ByteBuffersDirectory();

        StandardAnalyzer analyzer = new StandardAnalyzer();

        IndexWriterConfig groundTruthConfig = new IndexWriterConfig(analyzer);
        groundTruthConfig.setMergePolicy(new LogByteSizeMergePolicy());
        groundTruthConfig.setRAMBufferSizeMB(256.0);
        IndexWriter groundTruthWriter = new IndexWriter(groundTruthIndex, groundTruthConfig);

        IndexWriterConfig queryConfig = new IndexWriterConfig(analyzer);
        queryConfig.setMergePolicy(new LogByteSizeMergePolicy());
        queryConfig.setRAMBufferSizeMB(128.0);
        IndexWriter queryWriter = new IndexWriter(queryIndex, queryConfig);

        String txtFilePath = args[0];

        // Find min and max values for quantization
        float[] minMax = findMinMaxValues(txtFilePath);
        float min = minMax[0];
        float max = minMax[1];

        loadDatasetAndIndex(groundTruthWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex, min, max, false);
        loadDatasetAndIndex(queryWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex, min, max, true);

        logMemoryUsage("after indexing");

        System.out.println("\nIndexing complete. Merging segments...");

        long startMergeTime = System.currentTimeMillis();

        groundTruthWriter.forceMerge(1);
        queryWriter.forceMerge(1);

        long endMergeTime = System.currentTimeMillis();
        System.out.println("\nMerge time: " + (endMergeTime - startMergeTime) + " milliseconds");

        groundTruthWriter.close();
        queryWriter.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        memoryMonitor.interrupt();

        StringBuilder metricsContent = new StringBuilder(
                "\nTotal execution time: " + duration + " milliseconds\n" +
                        "Max memory usage: " + maxMemoryUsage / (1024 * 1024) + " MB\n");

        System.out.println(metricsContent);

        IndexSearcher groundTruthSearcher = new IndexSearcher(DirectoryReader.open(groundTruthIndex));
        IndexSearcher querySearcher = new IndexSearcher(DirectoryReader.open(queryIndex));

        int k = 5; // Number of nearest neighbors
        computeMetrics(groundTruthSearcher, querySearcher, txtFilePath, min, max, k);

        groundTruthIndex.close();
        queryIndex.close();
    }

    private static void loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads, int nToIndex, float min, float max, boolean quantized) throws InterruptedException, ExecutionException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            ConcurrentLinkedQueue<TitleEmbPair> batch = new ConcurrentLinkedQueue<>();
            int batchSize = 1000;
            int count = 0;
            while ((line = br.readLine()) != null && count < nToIndex) {
                batch.add(parseLine(line));
                count++;

                if (batch.size() >= batchSize) {
                    submitBatch(writer, batch, completionService, min, max, quantized);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                submitBatch(writer, batch, completionService, min, max, quantized);
            }
        }

        for (int i = 0; i < numThreads; i++) {
            completionService.submit(() -> null);
        }

        for (int i = 0; i < numThreads; i++) {
            completionService.take().get();
        }

        executorService.shutdown();
    }

    private static void submitBatch(IndexWriter writer, ConcurrentLinkedQueue<TitleEmbPair> batch, CompletionService<Void> completionService, float min, float max, boolean quantized) {
        for (TitleEmbPair pair : batch) {
            completionService.submit(() -> {
                float[] embeddingArray = new float[pair.getEmb().size()];
                int j = 0;
                for (Float vec : pair.getEmb()) {
                    embeddingArray[j++] = vec;
                }

                if (quantized) {
                    byte[] quantizedVector = quantizeToByteVector(embeddingArray, min, max);
                    addQuantizedDoc(writer, pair.getTitle(), quantizedVector);
                } else {
                    addDoc(writer, pair.getTitle(), embeddingArray);
                }
                return null;
            });
        }
    }

    private static void addDoc(IndexWriter writer, String title, float[] vector) {
        Document doc = new Document();
        doc.add(new TextField("unique_id", title, TextField.Store.YES)); // Use unique ID
        doc.add(new KnnFloatVectorField("vector", vector));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addQuantizedDoc(IndexWriter writer, String title, byte[] vector) {
        Document doc = new Document();
        doc.add(new TextField("unique_id", title, TextField.Store.YES)); // Use unique ID
        doc.add(new KnnByteVectorField("vector", vector));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static TitleEmbPair parseLine(String line) {
        String[] parts = line.split("\t");
        String uniqueId = parts[0]; // Unique ID from the first column
        String[] embStrs = parts[2].split(","); // Embeddings from the third column
        ConcurrentLinkedQueue<Float> emb = new ConcurrentLinkedQueue<>();
        for (String embStr : embStrs) {
            emb.add((float) Double.parseDouble(embStr));
        }
        return new TitleEmbPair(uniqueId, emb);
    }

    private static void monitorMemoryUsage() {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        while (!Thread.currentThread().isInterrupted()) {
            MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
            long usedMemory = heapMemoryUsage.getUsed();
            if (usedMemory > maxMemoryUsage) {
                maxMemoryUsage = usedMemory;
            }
            try {
                Thread.sleep(memorySleepAmount); // Adjust the sleep interval as needed
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void logMemoryUsage(String phase) {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest to the JVM to run the garbage collector
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long usedMemoryMB = usedMemory / (1024 * 1024);
        System.out.println("\nMemory used " + phase + ": " + usedMemoryMB + " MB");
    }

    private static void computeMetrics(IndexSearcher groundTruthSearcher, IndexSearcher querySearcher, String txtFilePath, float min, float max, int k) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            int queryCount = 0;
            int totalQueries = 100; // Number of queries to test
            int relevantRetrieved = 0;
            int relevantRetrievedQuantized = 0;

            KNN knn = new KNN();
            knn.loadVectors(txtFilePath);

            while ((line = br.readLine()) != null && queryCount < totalQueries) {
                TitleEmbPair pair = parseLine(line);
                float[] queryVector = new float[pair.getEmb().size()];
                int j = 0;
                for (Float vec : pair.getEmb()) {
                    queryVector[j++] = vec;
                }

                byte[] quantizedQueryVector = quantizeToByteVector(queryVector, min, max);

                TopDocs groundTruthResults = getNearestNeighbors(groundTruthSearcher, queryVector, k);
                TopDocs queryResultsQuantized = getNearestNeighborsQuantized(querySearcher, quantizedQueryVector, k);
                List<Vector> knnResults = knn.computeKNN(queryVector, k);

                Set<String> groundTruthIds = new HashSet<>();
                for (ScoreDoc scoreDoc : groundTruthResults.scoreDocs) {
                    Document doc = groundTruthSearcher.doc(scoreDoc.doc);
                    String uniqueId = doc.get("unique_id");
                    groundTruthIds.add(uniqueId);
                }

                for (Vector result : knnResults) {
                    if (groundTruthIds.contains(result.id)) {
                        relevantRetrieved++;
                    }
                }

                for (ScoreDoc scoreDoc : queryResultsQuantized.scoreDocs) {
                    Document doc = querySearcher.doc(scoreDoc.doc);
                    String uniqueId = doc.get("unique_id");
                    if (groundTruthIds.contains(uniqueId)) {
                        relevantRetrievedQuantized++;
                    }
                }

                queryCount++;
            }

            double recall = (double) relevantRetrieved / (k * totalQueries);
            double accuracy = recall;
            double recallQuantized = (double) relevantRetrievedQuantized / (k * totalQueries);
            double accuracyQuantized = recallQuantized;

            System.out.println("Accuracy (Unquantized): " + accuracy);
            System.out.println("Accuracy (Quantized): " + accuracyQuantized);
        }
    }

    private static TopDocs getNearestNeighbors(IndexSearcher searcher, float[] queryVector, int k) throws IOException {
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery("vector", queryVector, k);
        return searcher.search(knnQuery, k);
    }

    private static TopDocs getNearestNeighborsQuantized(IndexSearcher searcher, byte[] queryVector, int k) throws IOException {
        KnnByteVectorQuery knnQuery = new KnnByteVectorQuery("vector", queryVector, k);
        return searcher.search(knnQuery, k);
    }

    private static float[] findMinMaxValues(String txtFilePath) throws IOException {
        float min = Float.MAX_VALUE;
        float max = Float.MIN_VALUE;

        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                TitleEmbPair pair = parseLine(line);
                for (float value : pair.getEmb()) {
                    if (value < min) {
                        min = value;
                    }
                    if (value > max) {
                        max = value;
                    }
                }
            }
        }

        return new float[]{min, max};
    }

    private static byte[] quantizeToByteVector(float[] floatVector, float min, float max) {
        int length = floatVector.length;
        byte[] result = new byte[length];

        // Edge case: If all values are the same, set all to 0
        if (min == max) {
            return result; // All zeros
        }

        // Quantize the values to the range of int8 [-128, 127]
        for (int i = 0; i < length; i++) {
            // Normalize the float value to [0, 1]
            float normalizedValue = (floatVector[i] - min) / (max - min);

            // Scale and shift to the range [-128, 127]
            int quantizedValue = Math.round(normalizedValue * 255) - 128;

            // Ensure the value fits in the byte range
            quantizedValue = Math.max(-128, Math.min(127, quantizedValue));

            result[i] = (byte) quantizedValue;
        }

        return result;
    }
}