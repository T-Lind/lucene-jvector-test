package org.example;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

public class VectorSearchExample {
    public static void main(String[] args) throws Exception {
        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter writer = new IndexWriter(index, config);

        // Add documents with vector fields to the index
        addDoc(writer, "Document 1", new float[]{1.0f, 2.0f, 3.0f});
        addDoc(writer, "Document 2", new float[]{2.0f, 3.0f, 4.0f});
        addDoc(writer, "Document 3", new float[]{3.0f, 4.0f, 5.0f});
        writer.close();

        // Perform a vector search
        float[] queryVector = new float[]{2.5f, 3.5f, 4.5f};
        int k = 2; // Number of nearest neighbors
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        KnnVectorQuery knnQuery = new KnnVectorQuery("vector", queryVector, k);
        TopDocs topDocs = searcher.search(knnQuery, k);

        // Display the results
        System.out.println("Found " + topDocs.totalHits + " hits.");
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = searcher.doc(scoreDoc.doc);
            System.out.println("Doc ID: " + scoreDoc.doc + ", Title: " + doc.get("title") + ", Score: " + scoreDoc.score);
        }

        // Close the reader
        reader.close();
    }

    private static void addDoc(IndexWriter writer, String title, float[] vector) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("title", title, TextField.Store.YES));
        doc.add(new KnnVectorField("vector", vector));
        writer.addDocument(doc);
    }
}