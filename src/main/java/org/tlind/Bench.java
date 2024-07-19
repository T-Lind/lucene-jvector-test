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
import java.io.IOException;
import java.nio.channels.FileChannel;
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

        // Detailed metrics
        long metadataLoadStartTime = System.currentTimeMillis();
        long metadataLoadEndTime;

        // Load dataset and index documents
        metadataLoadStartTime = System.currentTimeMillis();
        loadDatasetAndIndex(writer, filePath);
        metadataLoadEndTime = System.currentTimeMillis();

        writer.close();

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        // Get the current time and date
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        // Prepare metrics content
        StringBuilder metricsContent = new StringBuilder();
        metricsContent.append("Test run on: ").append(timeStamp).append("\n");
        metricsContent.append("Total execution time: ").append(duration).append(" milliseconds\n");
        metricsContent.append("Metadata loading time: ").append(metadataLoadEndTime - metadataLoadStartTime).append(" milliseconds\n");

        // Print the metrics to the terminal
        System.out.println(metricsContent);
    }

    private static void loadDatasetAndIndex(IndexWriter writer, String datasetFilePath) throws IOException {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ObjectMapper mapper = new ObjectMapper();
        File metadataFile = new File(datasetFilePath + "dataset_dict.json");

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

                    long vectorLoadStartTime = System.currentTimeMillis();
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    reader.loadNextBatch();
                    long vectorLoadEndTime = System.currentTimeMillis();

                    VarCharVector titleVector = (VarCharVector) root.getVector("title");
                    Float4Vector embVector = (Float4Vector) root.getVector("emb");

                    long indexStartTime = System.currentTimeMillis();
                    for (int i = 0; i < root.getRowCount(); i++) {
                        String title = titleVector.getObject(i).toString();
                        float[] emb = new float[embVector.getValueCount()];
                        for (int j = 0; j < embVector.getValueCount(); j++) {
                            emb[j] = embVector.get(i);
                        }

                        addDoc(writer, title, emb);
                    }
                    long indexEndTime = System.currentTimeMillis();

                    System.out.println("Split: " + splitName);
                    System.out.println("Vector loading time: " + (vectorLoadEndTime - vectorLoadStartTime) + " milliseconds");
                    System.out.println("Indexing time: " + (indexEndTime - indexStartTime) + " milliseconds");

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
