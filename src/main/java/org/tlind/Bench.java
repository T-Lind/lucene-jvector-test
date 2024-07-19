package org.tlind;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Bench {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);

        String csvFilePath = "/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/train.csv";

        // Detailed metrics
        ArrayList<Long> indexLatencies = loadDatasetAndIndex(writer, csvFilePath);

        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        // Prepare metrics content
        StringBuilder metricsContent = new StringBuilder("Test run on: " + timeStamp + "\n" +
                "Total execution time: " + duration + " milliseconds\n" +
                "Metrics:\n--\n");


        // Print the metrics to the terminal
        System.out.println(metricsContent);

        // Calculate the average index latency and error bars
        long sum = 0;
        for (long latency : indexLatencies) {
            sum += latency;
        }
        double averageIndexLatency = (double) sum / indexLatencies.size();
        double error = 1.96 * Math.sqrt((double) sum / indexLatencies.size() * (1 - (double) sum / indexLatencies.size()) / indexLatencies.size());

        // Prepare the content for the metrics file
        metricsContent.append("Average index latency: ").append(averageIndexLatency).append(" milliseconds\n");
        metricsContent.append("Error: ").append(error).append(" milliseconds\n");
    }


    private static ArrayList<Long> loadDatasetAndIndex(IndexWriter writer, String csvFilePath) {
        ArrayList<Long> metrics = new ArrayList<>();
        System.out.println("Loading dataset and indexing...");
        return metrics;
    }

    private static void addDoc(IndexWriter writer, String title, float[] vector) {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new KnnVectorField("vector", vector));
        try {
            writer.addDocument(doc);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}