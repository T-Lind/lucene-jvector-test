package org.tlind;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class PeekDataset {

    public static void main(String[] args) {
        String filePath = "/Users/tiernan.lindauer/Desktop/wikipedia-en-dataset/train.csv";
        peekDataset(filePath);
    }

    private static void peekDataset(String csvFilePath) {
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            List<String[]> rows = reader.readAll();
            System.out.println("First two rows of the dataset:");
            for (int i = 0; i < Math.min(2, rows.size()); i++) {
                String[] row = rows.get(i);
                System.out.println("Row " + i + ":");
                for (String cell : row) {
                    System.out.print(cell + " ");
                }
                System.out.println();
            }
        } catch (IOException | CsvException e) {
            e.printStackTrace();
        }
    }
}
