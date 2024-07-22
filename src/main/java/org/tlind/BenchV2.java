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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class BenchV2 {
    public static void main(String[] args) throws Exception {

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        System.out.println("Lucene Bench\nTest run on: " + timeStamp);
        System.out.println("(Heap space available is " + Runtime.getRuntime().maxMemory() + " bytes)");

        long startTime = System.currentTimeMillis();

        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter writer = new IndexWriter(index, config);

        String workingDirectory = System.getProperty("user.dir");
        String txtFilePath = args[0];

        // Detailed metrics
        ArrayList<Long> indexLatencies = loadDatasetAndIndex(writer, txtFilePath);

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
        System.out.println("Example Vector Search Query Found " + topDocs.totalHits + ":");
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            System.out.println("\t- Doc ID: " + topDocs.scoreDocs[i].doc + ", Score: " + topDocs.scoreDocs[i].score);
        }

        // Close the index
        index.close();
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String txtFilePath) throws InterruptedException, ExecutionException, IOException {
        return loadDatasetAndIndex(writer, txtFilePath, Runtime.getRuntime().availableProcessors());
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads) throws InterruptedException, ExecutionException, IOException {
        return loadDatasetAndIndex(writer, txtFilePath, numThreads, 100_000);
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String txtFilePath, int numThreads, int nToIndex) throws InterruptedException, ExecutionException, IOException {
        ArrayList<Long> metrics = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Long>> futures = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(txtFilePath))) {
            String line;
            List<TitleEmbPair> batch = new ArrayList<>();
            int batchSize = 1000;
            ProgressBar progressBar = new ProgressBar(nToIndex);

            while ((line = br.readLine()) != null) {
                batch.add(parseLine(line));

                if (batch.size() >= batchSize) {
                    futures.addAll(processBatch(writer, batch, executorService, progressBar));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                futures.addAll(processBatch(writer, batch, executorService, progressBar));
            }
        }

        for (Future<Long> future : futures) {
            metrics.add(future.get());
        }

        executorService.shutdown();

        return metrics;
    }

    private static List<Future<Long>> processBatch(IndexWriter writer, List<TitleEmbPair> batch, ExecutorService executorService, ProgressBar progressBar) {
        List<Future<Long>> futures = new ArrayList<>();

        for (TitleEmbPair pair : batch) {
            futures.add(executorService.submit(() -> {
                List<Double> embeddingList = pair.getEmb();
                float[] embeddingArray = new float[embeddingList.size()];
                for (int j = 0; j < embeddingList.size(); j++) {
                    embeddingArray[j] = embeddingList.get(j).floatValue();
                }

                long start = System.currentTimeMillis();
                addDoc(writer, pair.getTitle(), embeddingArray);
                long end = System.currentTimeMillis();
                progressBar.update();
                return end - start;
            }));
        }

        return futures;
    }

    private static TitleEmbPair parseLine(String line) {
        String[] parts = line.split("\t");
        String title = parts[0];
        String[] embStrs = parts[1].split(",");
        List<Double> emb = new ArrayList<>();
        for (String embStr : embStrs) {
            emb.add(Double.parseDouble(embStr));
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