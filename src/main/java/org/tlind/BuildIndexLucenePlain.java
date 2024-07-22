package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;


public class BuildIndexLucenePlain {
    private static volatile long maxMemoryUsage = 0;

    private static final int memorySleepAmount = 100; // Sleep for n milliseconds between memory checks

    public static void main(String[] args) throws Exception {
        // Start memory monitoring thread
        Thread memoryMonitor = new Thread(BuildIndexLucenePlain::monitorMemoryUsage);
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
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(index, config);

        String workingDirectory = System.getProperty("user.dir");
        String jsonFilePath = args[0];

        // Detailed metrics
        ArrayList<Long> indexLatencies = loadDatasetAndIndex(writer, jsonFilePath);

        writer.forceMerge(1);
        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Stop memory monitoring thread
        memoryMonitor.interrupt();

        // Prepare metrics content
        StringBuilder metricsContent = new StringBuilder("Total execution time: " + duration + " milliseconds\n" +
                "Max memory usage: " + maxMemoryUsage / (1024 * 1024) + " MB\n");


        // Calculate the average index latency and error bars
        long sum = 0;
        for (long latency : indexLatencies) {
            sum += latency;
        }
        double averageIndexLatency = (double) sum / indexLatencies.size();
        double error = 1.96 * Math.sqrt((double) sum / indexLatencies.size() * (1 - (double) sum / indexLatencies.size()) / indexLatencies.size());

        // Prepare the content for the metrics file
        metricsContent.append("\t- Average index latency: ").append(averageIndexLatency).append(" milliseconds\n");
        metricsContent.append("\t- MOE: ").append(error).append(" milliseconds\n");
        metricsContent.append("\t- Total index latency: ").append(sum / 1000.0).append(" seconds\n");


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

        // Close the index
        index.close();
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath) throws InterruptedException, ExecutionException {
        // Default to using all available processors
        return loadDatasetAndIndex(writer, jsonFilePath, Runtime.getRuntime().availableProcessors());
    }

    private static void logMemoryUsage(String phase) {
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest to the JVM to run the garbage collector
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
//        System.out.println("\nMemory used " + phase + ": " + usedMemory + " bytes");
        long usedMemoryMB = usedMemory / (1024 * 1024);
        System.out.println("\nMemory used " + phase + ": " + usedMemoryMB + " MB");
    }
    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath, int numThreads) throws InterruptedException, ExecutionException {
        ArrayList<Long> metrics = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        // Read the JSON file into a Data object
        Data data;
        try {
            data = objectMapper.readValue(new File(jsonFilePath), Data.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON file", e);
        }

        // Extract titles and embeddings
        List<String> titles = data.getTitle();
        List<List<Double>> embeddings = data.getEmb();

        List<TitleEmbPairOld> titleEmbPairs = new ArrayList<>();
        for (int i = 0; i < titles.size() && i < embeddings.size(); i++) {
            titleEmbPairs.add(new TitleEmbPairOld(titles.get(i), embeddings.get(i)));
        }

        data = null;
        System.gc();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Long>> futures = new ArrayList<>();

        System.out.println("Indexing " + titleEmbPairs.size() + " documents...");

        ProgressBar progressBar = new ProgressBar(titles.size());

        for (int i = 0; i < titleEmbPairs.size(); i++) {
            final int index = i;
            futures.add(executorService.submit(() -> {
                // Convert List<Double> to float[]
                List<Double> embeddingList = titleEmbPairs.get(index).getEmb();
                float[] embeddingArray = new float[embeddingList.size()];
                for (int j = 0; j < embeddingList.size(); j++) {
                    embeddingArray[j] = embeddingList.get(j).floatValue();
                }

                long start = System.currentTimeMillis();

                addDoc(writer, titleEmbPairs.get(index).getTitle(), embeddingArray);

                long end = System.currentTimeMillis();
                progressBar.update();
                return end - start;
            }));
        }

        for (Future<Long> future : futures) {
            metrics.add(future.get());
        }

        executorService.shutdown();

        System.out.println();

        return metrics;
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

    private static float[] loadQuery(String queryJsonPath) {
        // Query file will have a single field "emb"
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

class TitleEmbPairOld {
    private String title;
    private List<Double> emb;

    public TitleEmbPairOld(String title, List<Double> emb) {
        this.title = title;
        this.emb = emb;
    }

    // Getters and setters
    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Double> getEmb() {
        return emb;
    }

    public void setEmb(List<Double> emb) {
        this.emb = emb;
    }

    @Override
    public String toString() {
        return "TitleEmbPair{" +
                "title='" + title + '\'' +
                ", emb=" + emb +
                '}';
    }
}
