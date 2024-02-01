package org.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class FileSplitter {

    public static void splitAndLowercase(String inputFilePath, String outputDirPath, int chunkSize) throws IOException {
        File outputDir = new File(outputDirPath);

        // Delete the output directory if it already exists
        if (outputDir.exists()) {
            deleteDirectory(outputDir);
        }

        // Create the output directory
        outputDir.mkdirs();

        try (FileInputStream inputStream = new FileInputStream(inputFilePath)) {
            byte[] buffer = new byte[chunkSize];
            int bytesRead;
            int chunkNumber = 1;

            while ((bytesRead = inputStream.read(buffer)) != -1) {
                // Create a new chunk file
                String chunkFileName = outputDirPath + File.separator + "chunk_" + chunkNumber + ".txt";
                try (FileOutputStream outputStream = new FileOutputStream(chunkFileName)) {
                    // Convert the chunk data to lowercase and write it to the new file
                    outputStream.write(new String(buffer, 0, bytesRead).toLowerCase().getBytes());
                }

                chunkNumber++;
            }
        }
    }

    private static void deleteDirectory(File directory) {
        File[] allContents = directory.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directory.delete();
    }

    public static void main(String[] args) throws IOException {
        String inputFilePath = "path/to/your/input/file.txt";
        String outputDirPath = "path/to/your/output/directory";
        int chunkSize = 30 * 1024 * 1024;

        splitAndLowercase(inputFilePath, outputDirPath, chunkSize);
    }
}