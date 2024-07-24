package org.tlind;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class RunLoadDataset {

    public static void main(String[] args) {
        // Define the output file and max entries
        String outputFile = args[0]; // Replace with your actual path -- you can specify as string or use args
        int maxEntries = -1; // Replace with your actual value, use -1 for max

        // Additional parameters for the datasource, split, and subset. Default is to wikipedia-en database.
        String datasource = "ashraq/cohere-wiki-embedding-100k"; // Default datasource
        String split = "train"; // Default split
        String subset = ""; // Default subset, can be null

        // Get the current working directory
        String workingDirectory = System.getProperty("user.dir");

        // Build the command to run the bash script with the arguments
        String[] command;
        if (subset == null || subset.isEmpty()) {
            command = new String[]{workingDirectory + "/run_load_wikipedia.sh", outputFile, String.valueOf(maxEntries), datasource, split};
        } else {
            command = new String[]{workingDirectory + "/run_load_wikipedia.sh", outputFile, String.valueOf(maxEntries), datasource, split, subset};
        }

        // Execute the command
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(new java.io.File(workingDirectory));
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            // Read the output from the command
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            // Wait for the process to complete
            int exitCode = process.waitFor();
            System.out.println("Process exited with code: " + exitCode);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
