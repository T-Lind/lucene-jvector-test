package org.tlind;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class LoadJsonExample {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            Data data = objectMapper.readValue(new File("/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/train.json"), Data.class);

            // Get the first two entries of title and emb
            List<String> titles = data.getTitle();
            List<List<Double>> embeddings = data.getEmb();

            if (titles.size() >= 2 && embeddings.size() >= 2) {
                System.out.println("Title 0: " + titles.get(0));
                System.out.println("Emb 0: " + embeddings.get(0));
                System.out.println("Title 1: " + titles.get(1));
                System.out.println("Emb 1: " + embeddings.get(1));
            } else {
                System.out.println("Not enough entries in the JSON file.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
