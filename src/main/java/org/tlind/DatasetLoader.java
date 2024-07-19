package org.tlind;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;

public class DatasetLoader {

    public static void main(String[] args) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ObjectMapper mapper = new ObjectMapper();
        File metadataFile = new File("/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/metadata.json");

        try {
            JsonNode rootNode = mapper.readTree(metadataFile);
            JsonNode splits = rootNode.get("splits");

            Iterator<Map.Entry<String, JsonNode>> fields = splits.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String splitName = entry.getKey();
                String arrowFilePath = "/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset" + splitName + ".arrow";

                File arrowFile = new File(arrowFilePath);
                if (arrowFile.exists()) {
                    try (FileInputStream fis = new FileInputStream(arrowFile);
                         FileChannel channel = fis.getChannel()) {
                        SeekableReadChannel readChannel = new SeekableReadChannel(channel);
                        ArrowFileReader reader = new ArrowFileReader(readChannel, allocator);

                        Schema schema = reader.getVectorSchemaRoot().getSchema();
                        System.out.println("Schema for split " + splitName + ": " + schema);

                        while (reader.loadNextBatch()) {
                            VectorSchemaRoot root = reader.getVectorSchemaRoot();
                            System.out.println("Read " + root.getRowCount() + " records from split " + splitName);

                            // Process the data here
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Arrow file not found for split: " + splitName);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            allocator.close();
        }
    }
}
