package org.tlind;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class RunLoadWikipedia {

    public static void main(String[] args) {
        String outputFile = "/path/to/output_file.txt"; // TODO: Replace with your actual path
        int maxEntries = -1; // TODO: Replace with your actual value, use -1 for max

        // Get the current working directory
        String workingDirectory = System.getProperty("user.dir");

        // Build the command to run the bash script with the arguments
        String[] command;
        if (maxEntries == -1) {
            command = new String[]{workingDirectory + "/run_load_wikipedia.sh", outputFile};
        } else {
            command = new String[]{workingDirectory + "/run_load_wikipedia.sh", outputFile, String.valueOf(maxEntries)};
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