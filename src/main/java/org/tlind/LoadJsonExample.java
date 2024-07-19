package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LoadJsonExample {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // Path to the JSON file
            String filePath = "/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/small_train.json";

            // Read the original JSON file into the Data class
            Data data = objectMapper.readValue(new File(filePath), Data.class);

            // Extract only the title and emb fields and create TitleEmbPair objects
            List<TitleEmbPair> pairs = new ArrayList<>();
            List<String> titles = data.getTitle();
            List<List<Double>> embeddings = data.getEmb();

            for (int i = 0; i < titles.size() && i < embeddings.size(); i++) {
                pairs.add(new TitleEmbPair(titles.get(i), embeddings.get(i)));
            }

            // Create a new SimplifiedData object with the list of TitleEmbPair objects
            SimplifiedData simplifiedData = new SimplifiedData(pairs);

            // Discard the original Data object to free up memory
            data = null;

            // Get the first two entries of title and emb
            if (pairs.size() >= 2) {
                System.out.println("Pair 0: " + simplifiedData.getPairs().get(0));
                System.out.println("Pair 1: " + simplifiedData.getPairs().get(1));
            } else {
                System.out.println("Not enough entries in the JSON file.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
