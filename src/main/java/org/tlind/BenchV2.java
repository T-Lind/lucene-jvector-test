package org.tlind;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
        System.out.println("Example Vector Search Query Found " + topDocs.totalHits + ":");
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            System.out.println("\t- Doc ID: " + topDocs.scoreDocs[i].doc + ", Score: " + topDocs.scoreDocs[i].score);
        }

        // Close the index
        index.close();
    }



    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath) throws InterruptedException, ExecutionException, IOException {
        ArrayList<Long> metrics = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonFactory jsonFactory = objectMapper.getFactory();

        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        List<Future<Long>> futures = new ArrayList<>();

        try (JsonParser jsonParser = jsonFactory.createParser(new File(jsonFilePath))) {
            if (jsonParser.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalStateException("Expected content to be an object");
            }

            String fieldName;
            long totalDocuments = 0;
            List<TitleEmbPair> batch = new ArrayList<>();
            int batchSize = 1000; // Adjust this based on your memory constraints and performance needs

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                fieldName = jsonParser.getCurrentName();
                if ("title".equals(fieldName) || "emb".equals(fieldName)) {
                    jsonParser.nextToken();
                    if (jsonParser.currentToken() == JsonToken.START_ARRAY) {
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            if ("title".equals(fieldName)) {
                                String title = jsonParser.getText();
                                batch.add(new TitleEmbPair(title, null));
                            } else if ("emb".equals(fieldName)) {
                                List<Double> emb = objectMapper.readValue(jsonParser, new TypeReference<List<Double>>() {});
                                if (!batch.isEmpty()) {
                                    batch.get(batch.size() - 1).setEmb(emb);
                                }
                            }

                            if (batch.size() == batchSize) {
                                processBatch(batch, writer, executorService, futures);
                                totalDocuments += batch.size();
                                batch.clear();
                            }
                        }
                    }
                } else {
                    jsonParser.skipChildren();
                }
            }

            // Process any remaining documents in the last batch
            if (!batch.isEmpty()) {
                processBatch(batch, writer, executorService, futures);
                totalDocuments += batch.size();
            }

            System.out.println("Indexed " + totalDocuments + " documents");
        }

        for (Future<Long> future : futures) {
            metrics.add(future.get());
        }

        executorService.shutdown();

        return metrics;
    }

    private static void processBatch(List<TitleEmbPair> batch, IndexWriter writer, ExecutorService executorService, List<Future<Long>> futures) {
        for (TitleEmbPair pair : batch) {
            futures.add(executorService.submit(() -> {
                float[] embeddingArray = new float[pair.getEmb().size()];
                for (int j = 0; j < pair.getEmb().size(); j++) {
                    embeddingArray[j] = pair.getEmb().get(j).floatValue();
                }

                long start = System.currentTimeMillis();
                addDoc(writer, pair.getTitle(), embeddingArray);
                long end = System.currentTimeMillis();
                return end - start;
            }));
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