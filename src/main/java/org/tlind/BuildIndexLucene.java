package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.*;

public class BuildIndexLucene {
    private static final int memorySleepAmount = 100; // Sleep interval in milliseconds -- set as needed
    private static final int numberOfVectorsToIndex = 100_000; // TODO: SET THIS BASED ON THE SIZE OF YOUR DATASET!

    private static volatile long maxMemoryUsage = 0;

    private static float min;
    private static float max;

    public static void main(String[] args) throws Exception {
        // Start memory monitoring thread
        Thread memoryMonitor = new Thread(BuildIndexLucene::monitorMemoryUsage);
        memoryMonitor.start();

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("Lucene Bench\nTest run on: " + timeStamp);
        System.out.println("(Heap space available is " + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB)");

        long startTime = System.currentTimeMillis();

        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setRAMBufferSizeMB(256.0);
        IndexWriter writer = new IndexWriter(index, config);

        String workingDirectory = System.getProperty("user.dir");
        String fvecPath = args[0];

        // First pass to find the global min and max values used in int8 quantization
        float[] minMax = VectorFileLoader.findMinAndMax(fvecPath);
        min = minMax[0];
        max = minMax[1];

        float indexLatency = loadFvecsAndIndex(writer, fvecPath, min, max);

        logMemoryUsage("after indexing");

        System.out.println("\nIndexing complete. Merging segments...");

        long startMergeTime = System.currentTimeMillis();

        writer.forceMerge(1);

        long endMergeTime = System.currentTimeMillis();

        System.out.println("Merge time: " + (endMergeTime - startMergeTime) + " milliseconds");

        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Stop memory monitoring thread
        memoryMonitor.interrupt();

        // Prepare metrics content
        StringBuilder metricsContent = new StringBuilder(
                "\nTotal execution time: " + duration + " milliseconds\n" +
                "Max memory usage: " + maxMemoryUsage / (1024 * 1024) + " MB\n");


        // Prepare the content for the metrics file
        metricsContent.append("Average index latency: ").append(indexLatency).append(" milliseconds\n");

        // Print the final metrics
        System.out.println(metricsContent);

        // Run an example search
        float[] queryVector = loadQuery(workingDirectory + "/src/main/java/org/tlind/examplequery.json");

        // Let's perform a basic vector search using a query vector defined above.
        int k = 5; // Number of nearest neighbors
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(index));
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery("vector", queryVector, k);
        TopDocs topDocs = searcher.search(knnQuery, k);

        // Display the results
        System.out.println("Example Vector Search Query Found " + topDocs.totalHits + ":");
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            System.out.println("\t- Doc ID: " + topDocs.scoreDocs[i].doc + ", Score: " + topDocs.scoreDocs[i].score);
        }

        // Load fvec queries from  using VectorFileLoader
        ArrayList<float[]> queries = VectorFileLoader.readFvecs("/Users/tiernan.lindauer/IdeaProjects/jvector/fvec/wikipedia_squad/100k/cohere_embed-english-v3.0_1024_query_vectors_10000.fvec");

        int indices = 0;
        for (float[] query: queries) {
            KnnFloatVectorQuery knnQuery2 = new KnnFloatVectorQuery("vector", query, k);
            TopDocs topDocs2 = searcher.search(knnQuery2, k);

            System.out.println("Example Vector Search Query Found " + topDocs2.totalHits + ":");
            for (int i = 0; i < topDocs2.scoreDocs.length; i++) {
                System.out.println("\t- Doc ID: " + topDocs2.scoreDocs[i].doc + ", Score: " + topDocs2.scoreDocs[i].score);
            }
            indices++;

            if (indices > 10) {
                break;
            }
        }

        // Close the index
        index.close();
    }

    private static float loadDatasetAndIndex(IndexWriter writer, String txtFilePath) throws InterruptedException, ExecutionException, IOException {
        return loadDatasetAndIndex(writer, txtFilePath, Runtime.getRuntime().availableProcessors());
    }

    private static float loadFvecsAndIndex(IndexWriter writer, String fvecFilePath, float min, float max) {
        ProgressBar progressBar = new ProgressBar(numberOfVectorsToIndex);
        long totalIndexLatency = 0;
        long count = 0;
        try (var dis = new DataInputStream(new BufferedInputStream(new FileInputStream(fvecFilePath)))) {
            while (dis.available() > 0) {
                long start = System.currentTimeMillis();
                var dimension = Integer.reverseBytes(dis.readInt());
                assert dimension > 0 : dimension;
                var buffer = new byte[dimension * Float.BYTES];
                dis.readFully(buffer);
                var byteBuffer = ByteBuffer.wrap(buffer).order(ByteOrder.LITTLE_ENDIAN);

                var vector = new float[dimension];
                var floatBuffer = byteBuffer.asFloatBuffer();
                floatBuffer.get(vector);

                addDoc(writer, "title", vector);
                long end = System.currentTimeMillis();
                totalIndexLatency += end - start;
                count++;
                progressBar.update();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return (float) totalIndexLatency / count;
    }
    private static float loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads) throws InterruptedException, ExecutionException, IOException {
        return loadDatasetAndIndex(writer, txtFilePath, numThreads, 100_000);
    }

    private static float loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads, int nToIndex) throws InterruptedException, ExecutionException, IOException {
        ConcurrentLinkedQueue<Long> metrics = new ConcurrentLinkedQueue<>();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CompletionService<Long> completionService = new ExecutorCompletionService<>(executorService);

        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            ConcurrentLinkedQueue<TitleEmbPair> batch = new ConcurrentLinkedQueue<>();
            int batchSize = 1000;
            ProgressBar progressBar = new ProgressBar(nToIndex);

            while ((line = br.readLine()) != null) {
                batch.add(parseLine(line));

                if (batch.size() >= batchSize) {
                    submitBatch(writer, batch, completionService, progressBar);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                submitBatch(writer, batch, completionService, progressBar);
            }
        }

        for (int i = 0; i < numThreads; i++) {
            completionService.submit(() -> null);
        }

        for (int i = 0; i < numThreads; i++) {
            Future<Long> future = completionService.take();
            if (future != null) {
                metrics.add(future.get());
            }
        }

        executorService.shutdown();

        long sum = 0;
        for (long metric : metrics) {
            sum += metric;
        }

        return (float) sum / metrics.size();
    }

    private static void logMemoryUsage(String phase) {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest to the JVM to run the garbage collector
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
//        System.out.println("\nMemory used " + phase + ": " + usedMemory + " bytes");
        long usedMemoryMB = usedMemory / (1024 * 1024);
        System.out.println("\nMemory used " + phase + ": " + usedMemoryMB + " MB");
    }

    private static void submitBatch(IndexWriter writer, ConcurrentLinkedQueue<TitleEmbPair> batch, CompletionService<Long> completionService, ProgressBar progressBar) {
        for (TitleEmbPair pair : batch) {
            completionService.submit(() -> {
                ConcurrentLinkedQueue<Float> embeddingList = pair.getEmb();
                float[] embeddingArray = new float[embeddingList.size()];
                int j = 0;
                for (Float vec : embeddingList) {
                    embeddingArray[j++] = vec;
                }

                long start = System.currentTimeMillis();
                addDoc(writer, pair.getTitle(), embeddingArray);
                long end = System.currentTimeMillis();
                progressBar.update();
                return end - start;
            });
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

    private static void addDocs(IndexWriter writer, String title, ArrayList<float[]> vectors) {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        for (float[] vector : vectors) {
            doc.add(new KnnFloatVectorField("vector", vector));
        }
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static float[] loadQuery(String queryJsonPath) {
        // Query file will have a single field “emb”
        ObjectMapper objectMapper = new ObjectMapper();
        Embedding queryList;
        try {
            queryList = objectMapper.readValue(Paths.get(queryJsonPath).toFile(), Embedding.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading query JSON file", e);
        }
        float[] query = new float[queryList.getEmb().size()];

        for (int i = 0; i < queryList.getEmb().size(); i++) {
            query[i] = queryList.getEmb().get(i).floatValue();
        }

        return query;
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
}
