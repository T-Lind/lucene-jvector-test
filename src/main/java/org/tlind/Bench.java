package org.tlind;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Bench {

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();

        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);

        String filePath = "/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/";

        // Load dataset and index documents
        loadDatasetAndIndex(writer, filePath);

        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());

        // Prepare metrics content
        String metricsContent = "Test run on: " + timeStamp.replace('_', ' ') + "\n";
        metricsContent += "Execution time: " + duration + " milliseconds\n";

        // Print the metrics to the terminal
        System.out.println(metricsContent);

        // Save the metrics to a file in the tests/ folder
        String metricsFilePath = "tests/metrics_" + timeStamp + ".txt";
        Files.createDirectories(Paths.get("tests"));
        try (FileWriter fileWriter = new FileWriter(metricsFilePath)) {
            fileWriter.write(metricsContent);
        }
    }

    private static void loadDatasetAndIndex(IndexWriter writer, String datasetFilePath) throws IOException {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ObjectMapper mapper = new ObjectMapper();
        File metadataFile = new File(datasetFilePath + "metadata.json");

        JsonNode rootNode = mapper.readTree(metadataFile);
        JsonNode splits = rootNode.get("splits");

        splits.fields().forEachRemaining(entry -> {
            String splitName = entry.getKey();
            String arrowFilePath = datasetFilePath + splitName + ".arrow";

            File arrowFile = new File(arrowFilePath);
            if (arrowFile.exists()) {
                try (FileInputStream fis = new FileInputStream(arrowFile);
                     FileChannel channel = fis.getChannel()) {
                    SeekableReadChannel readChannel = new SeekableReadChannel(channel);
                    ArrowFileReader reader = new ArrowFileReader(readChannel, allocator);

                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    reader.loadNextBatch();

                    VarCharVector titleVector = (VarCharVector) root.getVector("title");
                    Float4Vector embVector = (Float4Vector) root.getVector("emb");

                    for (int i = 0; i < root.getRowCount(); i++) {
                        String title = titleVector.getObject(i).toString();
                        float[] emb = new float[embVector.getValueCount()];
                        for (int j = 0; j < embVector.getValueCount(); j++) {
                            emb[j] = embVector.get(i);
                        }

                        addDoc(writer, title, emb);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Arrow file not found for split: " + splitName);
            }
        });

        allocator.close();
    }

    private static void addDoc(IndexWriter writer, String title, float[] vector) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new KnnVectorField("vector", vector));
        writer.addDocument(doc);
    }
}
