package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

public class CheckQuantizedLucene {
    private static final int memorySleepAmount = 100; // Sleep interval in milliseconds -- set as needed
    private static final int numberOfVectorsToIndex = 100_000; // Adjust based on your dataset size
    private static volatile long maxMemoryUsage = 0;

    public static void main(String[] args) throws Exception {
        Thread memoryMonitor = new Thread(CheckQuantizedLucene::monitorMemoryUsage);
        memoryMonitor.start();

        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("Lucene Bench\nTest run on: " + timeStamp);
        System.out.println("(Heap space available is " + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB)");

        long startTime = System.currentTimeMillis();

        Directory groundTruthIndex = new ByteBuffersDirectory();
        Directory quantizedIndex = new ByteBuffersDirectory();

        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config1 = new IndexWriterConfig(analyzer);
        config1.setMergePolicy(new LogByteSizeMergePolicy());
        config1.setRAMBufferSizeMB(256.0);

        IndexWriterConfig config2 = new IndexWriterConfig(analyzer);
        config2.setMergePolicy(new LogByteSizeMergePolicy());
        config2.setRAMBufferSizeMB(256.0);

        IndexWriter groundTruthWriter = new IndexWriter(groundTruthIndex, config1);
        IndexWriter quantizedWriter = new IndexWriter(quantizedIndex, config2);

        String workingDirectory = System.getProperty("user.dir");
        String txtFilePath = args[0];

        float[] minMax = findMinMaxValues(txtFilePath);
        float min = minMax[0];
        float max = minMax[1];

        System.out.println("Found max and min used for int8 quantization.");

        loadDatasetAndIndex(groundTruthWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex, min, max, false);
        loadDatasetAndIndex(quantizedWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex, min, max, true);

        logMemoryUsage("after indexing");

        System.out.println("\nIndexing complete. Merging segments...");

        long startMergeTime = System.currentTimeMillis();

        groundTruthWriter.forceMerge(1);
        quantizedWriter.forceMerge(1);

        long endMergeTime = System.currentTimeMillis();
        System.out.println("\nMerge time: " + (endMergeTime - startMergeTime) + " milliseconds");

        groundTruthWriter.close();
        quantizedWriter.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        memoryMonitor.interrupt();

        StringBuilder metricsContent = new StringBuilder(
                "\nTotal execution time: " + duration + " milliseconds\n" +
                        "Max memory usage: " + maxMemoryUsage / (1024 * 1024) + " MB\n");

        System.out.println(metricsContent);

        IndexSearcher groundTruthSearcher = new IndexSearcher(DirectoryReader.open(groundTruthIndex));
        IndexSearcher quantizedSearcher = new IndexSearcher(DirectoryReader.open(quantizedIndex));

        int k = 5; // Number of nearest neighbors
        evaluateAllVectors(groundTruthSearcher, quantizedSearcher, txtFilePath, min, max, k);

        groundTruthIndex.close();
        quantizedIndex.close();
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

    private static void loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads, int nToIndex, float min, float max, boolean quantized) throws InterruptedException, ExecutionException, IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executorService);

        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            ConcurrentLinkedQueue<TitleEmbPair> batch = new ConcurrentLinkedQueue<>();
            int batchSize = 1000;
            while ((line = br.readLine()) != null) {
                batch.add(parseLine(line));

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
                    byte[] byteVector = quantizeToByteVector(embeddingArray, min, max);
                    addDoc(writer, pair.getTitle(), byteVector);
                } else {
                    addDoc(writer, pair.getTitle(), embeddingArray);
                }
                return null;
            });
        }
    }

    private static void addDoc(IndexWriter writer, String title, float[] vector) {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new KnnFloatVectorField("vector", vector));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addDoc(IndexWriter writer, String title, byte[] vector) {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new KnnByteVectorField("vector", vector));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void logMemoryUsage(String message) {
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
        long usedMemory = heapMemoryUsage.getUsed();
        System.out.println(message + ": " + usedMemory / (1024 * 1024) + " MB");
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

    private static byte[] quantizeToByteVector(float[] floatVector, float min, float max) {
        int length = floatVector.length;
        byte[] result = new byte[length];

        if (min == max) {
            return result; // All zeros
        }

        for (int i = 0; i < length; i++) {
            float normalizedValue = (floatVector[i] - min) / (max - min);
            int quantizedValue = Math.round(normalizedValue * 255) - 128;
            quantizedValue = Math.max(-128, Math.min(127, quantizedValue));
            result[i] = (byte) quantizedValue;
        }

        return result;
    }

    private static void evaluateAllVectors(IndexSearcher groundTruthSearcher, IndexSearcher quantizedSearcher, String txtFilePath, float min, float max, int k) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            int totalQueries = 0;
            int totalRelevantRetrieved = 0;
            int totalResultsQuantized = 0;

            while ((line = br.readLine()) != null) {
                TitleEmbPair pair = parseLine(line);
                float[] queryVector = new float[pair.getEmb().size()];
                int j = 0;
                for (Float vec : pair.getEmb()) {
                    queryVector[j++] = vec;
                }
                byte[] queryByteVector = quantizeToByteVector(queryVector, min, max);
                TopDocs groundTruthResults = getNearestNeighbors(groundTruthSearcher, queryVector, k);
                TopDocs quantizedResults = getNearestNeighborsQuantized(quantizedSearcher, queryByteVector, k);

                int relevantRetrieved = 0;
                for (int i = 0; i < quantizedResults.scoreDocs.length; i++) {
                    int quantizedDocId = quantizedResults.scoreDocs[i].doc;
                    for (int m = 0; m < groundTruthResults.scoreDocs.length; m++) {
                        int groundTruthDocId = groundTruthResults.scoreDocs[m].doc;
                        if (quantizedDocId == groundTruthDocId) {
                            relevantRetrieved++;
                            break;
                        }
                    }
                }

                totalQueries++;
                totalRelevantRetrieved += relevantRetrieved;
                totalResultsQuantized += quantizedResults.totalHits.value;
            }

            double recall = (double) totalRelevantRetrieved / (totalQueries * k);
            double accuracy = (double) totalRelevantRetrieved / totalResultsQuantized;

            System.out.println("Recall: " + recall);
            System.out.println("Accuracy: " + accuracy);
        }
    }

    private static TopDocs getNearestNeighbors(IndexSearcher searcher, float[] queryVector, int k) throws IOException {
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery("vector", queryVector, k);
        return searcher.search(knnQuery, k);
    }

    private static TopDocs getNearestNeighborsQuantized(IndexSearcher searcher, byte[] queryByteVector, int k) throws IOException {
        KnnByteVectorQuery knnQuery = new KnnByteVectorQuery("vector", queryByteVector, k);
        return searcher.search(knnQuery, k);
    }

    private static TitleEmbPair parseLine(String line) {
        String[] parts = line.split("\t");
        String title = parts[0];
        String[] embStrs = parts[1].split(",");
        ConcurrentLinkedQueue<Float> emb = new ConcurrentLinkedQueue<>();
        for (String embStr : embStrs) {
            emb.add((float) Double.parseDouble(embStr));
        }
        return new TitleEmbPair(title, emb);
    }
}