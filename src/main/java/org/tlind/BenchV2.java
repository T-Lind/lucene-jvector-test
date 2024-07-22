package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.TieredMergePolicy;
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

public class BenchV2 {
    public static void main(String[] args) throws Exception {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        System.out.println("Lucene Bench\nTest run on: " + timeStamp);
        System.out.println("(Heap space available is " + Runtime.getRuntime().maxMemory() + " bytes)");

        long startTime = System.currentTimeMillis();

        Directory index = new ByteBuffersDirectory();
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        MergePolicy mergePolicy = new TieredMergePolicy();
        config.setMergePolicy(mergePolicy);
        IndexWriter writer = new IndexWriter(index, config);

        String workingDirectory = System.getProperty("user.dir");
        String jsonFilePath = args[0];

        ArrayList<Long> indexLatencies = loadDatasetAndIndex(writer, jsonFilePath);

        writer.forceMerge(1);
        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        StringBuilder metricsContent = new StringBuilder(
                "Total execution time: " + duration + " milliseconds\n" +
                        "Metrics:\n");

        long sum = 0;
        for (long latency : indexLatencies) {
            sum += latency;
        }
        double averageIndexLatency = (double) sum / indexLatencies.size();
        double error = 1.96 * Math.sqrt((double) sum / indexLatencies.size() * (1 - (double) sum / indexLatencies.size()) / indexLatencies.size());

        metricsContent.append("\t- Average index latency: ").append(averageIndexLatency).append(" milliseconds\n");
        metricsContent.append("\t- MOE: ").append(error).append(" milliseconds\n");
        metricsContent.append("\t- Total index latency: ").append(sum / 1000.0).append(" seconds\n");

        System.out.println(metricsContent);

        float[] queryVector = loadQuery(workingDirectory + "/src/main/java/org/tlind/examplequery.json");

        int k = 5;
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(index));
        KnnFloatVectorQuery knnQuery = new KnnFloatVectorQuery("vector", queryVector, k);
        TopDocs topDocs = searcher.search(knnQuery, k);

        System.out.println("Example Vector Search Query Found " + topDocs.totalHits + ":");
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            System.out.println("\t- Doc ID: " + topDocs.scoreDocs[i].doc + ", Score: " + topDocs.scoreDocs[i].score);
        }

        index.close();
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath) throws InterruptedException, ExecutionException {
        return loadDatasetAndIndex(writer, jsonFilePath, Runtime.getRuntime().availableProcessors());
    }

    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String jsonFilePath, int numThreads) throws InterruptedException, ExecutionException {
        ArrayList<Long> metrics = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        Data data;
        try {
            data = objectMapper.readValue(new File(jsonFilePath), Data.class);
        } catch (IOException e) {
            throw new RuntimeException("Error reading JSON file", e);
        }

        List<String> titles = data.getTitle();
        List<List<Double>> embeddings = data.getEmb();

        List<TitleEmbPair> titleEmbPairs = new ArrayList<>();
        for (int i = 0; i < titles.size() && i < embeddings.size(); i++) {
            titleEmbPairs.add(new TitleEmbPair(titles.get(i), embeddings.get(i)));
        }

        data = null;
        System.gc();

        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        CompletionService<Long> completionService = new ExecutorCompletionService<>(executorService);

        System.out.println("Indexing " + titleEmbPairs.size() + " documents...");

        ProgressBar progressBar = new ProgressBar(titles.size());

        for (int i = 0; i < titleEmbPairs.size(); i++) {
            final int index = i;
            completionService.submit(() -> {
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
            });
        }

        for (int i = 0; i < titleEmbPairs.size(); i++) {
            Future<Long> future = completionService.take();
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
