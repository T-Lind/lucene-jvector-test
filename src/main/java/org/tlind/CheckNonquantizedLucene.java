package org.tlind;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.search.IndexSearcher;
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
import java.util.Set;
import java.util.concurrent.*;

public class CheckNonquantizedLucene {
    private static final int memorySleepAmount = 100; // Sleep interval in milliseconds -- set as needed
    private static final int numberOfVectorsToIndex = 100_000; // Adjust based on your dataset size
    private static volatile long maxMemoryUsage = 0;

    public static void main(String[] args) throws Exception {
        Thread memoryMonitor = new Thread(CheckNonquantizedLucene::monitorMemoryUsage);
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

        loadDatasetAndIndex(groundTruthWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex);
        loadDatasetAndIndex(queryWriter, txtFilePath, Runtime.getRuntime().availableProcessors(), numberOfVectorsToIndex);

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
        computeMetrics(groundTruthSearcher, querySearcher, txtFilePath, k);

        groundTruthIndex.close();
        queryIndex.close();
    }

    private static void loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads, int nToIndex) throws InterruptedException, ExecutionException, IOException {
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
                    submitBatch(writer, batch, completionService);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                submitBatch(writer, batch, completionService);
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

    private static void submitBatch(IndexWriter writer, ConcurrentLinkedQueue<TitleEmbPair> batch, CompletionService<Void> completionService) {
        for (TitleEmbPair pair : batch) {
            completionService.submit(() -> {
                float[] embeddingArray = new float[pair.getEmb().size()];
                int j = 0;
                for (Float vec : pair.getEmb()) {
                    embeddingArray[j++] = vec;
                }

                addDoc(writer, pair.getTitle(), embeddingArray);
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

    private static void computeMetrics(IndexSearcher groundTruthSearcher, IndexSearcher querySearcher, String txtFilePath, int k) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            int queryCount = 0;
            int totalQueries = 100; // Number of queries to test
            int relevantRetrieved = 0;

            while ((line = br.readLine()) != null && queryCount < totalQueries) {
                TitleEmbPair pair = parseLine(line);
                float[] queryVector = new float[pair.getEmb().size()];
                int j = 0;
                for (Float vec : pair.getEmb()) {
                    queryVector[j++] = vec;
                }

                TopDocs groundTruthResults = getNearestNeighbors(groundTruthSearcher, queryVector, k);
                TopDocs queryResults = getNearestNeighbors(querySearcher, queryVector, k);

                Set<String> groundTruthTitles = new HashSet<>();
                for (ScoreDoc scoreDoc : groundTruthResults.scoreDocs) {
                    Document doc = groundTruthSearcher.doc(scoreDoc.doc);
                    String title = doc.get("title");
                    groundTruthTitles.add(title);
                }

                for (ScoreDoc scoreDoc : queryResults.scoreDocs) {
                    Document doc = querySearcher.doc(scoreDoc.doc);
                    String title = doc.get("title");
                    if (groundTruthTitles.contains(title)) {
                        relevantRetrieved++;
                    }
                }

                queryCount++;
            }

            double recall = (double) relevantRetrieved / (k * totalQueries);
            double accuracy = recall;

            System.out.println("Recall: " + recall);
            System.out.println("Accuracy: " + accuracy);
        }
    }

    private static TopDocs getNearestNeighbors(IndexSearcher searcher, float[] queryVector, int k) throws IOException {
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery("vector", queryVector, k);
        return searcher.search(knnQuery, k);
    }
}