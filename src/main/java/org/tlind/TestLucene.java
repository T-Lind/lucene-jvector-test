package org.tlind;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;

public class TestLucene {
    public static void main(String[] args) throws Exception {
        // Create a new index in memory
        Directory index = new ByteBuffersDirectory();

        // Set up an analyzer and index writer configuration
        StandardAnalyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, config);

        // Add a document to the index
        addDoc(w, "Lucene in Action", "193398817");
        addDoc(w, "Lucene for Dummies", "55320055Z");
        addDoc(w, "Managing Gigabytes", "55063554A");
        addDoc(w, "The Art of Computer Science", "9900333X");
        w.close();

        // Search the index
        String querystr = args.length > 0 ? args[0] : "lucene";

        // Parse the query
        Query q = new QueryParser("title", analyzer).parse(querystr);

        // Search the index
        int hitsPerPage = 10;
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs docs = searcher.search(q, hitsPerPage);
        ScoreDoc[] hits = docs.scoreDocs;

        // Display the results
        System.out.println("Found " + hits.length + " hits.");
        for (int i = 0; i < hits.length; ++i) {
            int docId = hits[i].doc;
            Document d = searcher.doc(docId);
            System.out.println((i + 1) + ". " + d.get("isbn") + "\t" + d.get("title"));
        }

        // Close the reader
        reader.close();
    }

    private static void addDoc(IndexWriter w, String title, String isbn) throws Exception {
        Document doc = new Document();
        doc.add(new TextField("title", title, Field.Store.YES));
        doc.add(new TextField("isbn", isbn, Field.Store.YES));
        w.addDocument(doc);
    }
}