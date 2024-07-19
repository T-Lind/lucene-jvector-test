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
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class Bench {

    public static void main(String[] args) throws Exception {
        System.out.println("Heap space available is " + Runtime.getRuntime().maxMemory());

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        System.out.println("Lucene Bench\nTest run on: " + timeStamp + "\n");

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


        // Prepare metrics content
        StringBuilder metricsContent = new StringBuilder(
                "Total execution time: " + duration + " milliseconds\n" +
                "Metrics:\n");


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
        System.out.println("Found " + topDocs.totalHits + ":");
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            System.out.println("\t- Doc ID: " + topDocs.scoreDocs[i].doc + ", Score: " + topDocs.scoreDocs[i].score);
        }

        // Close the index
        index.close();
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath) throws InterruptedException, ExecutionException {
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

        data = null;
        System.gc();

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Long>> futures = new ArrayList<>();

        System.out.println("Indexing " + titles.size() + " documents...");

        ProgressBar progressBar = new ProgressBar(titles.size());

        for (int i = 0; i < titles.size() && i < embeddings.size(); i++) {
            final int index = i;
            futures.add(executorService.submit(() -> {
                // Convert List<Double> to float[]
                List<Double> embeddingList = embeddings.get(index);
                float[] embeddingArray = new float[embeddingList.size()];
                for (int j = 0; j < embeddingList.size(); j++) {
                    embeddingArray[j] = embeddingList.get(j).floatValue();
                }

                long start = System.currentTimeMillis();

                addDoc(writer, titles.get(index), embeddingArray);

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
}