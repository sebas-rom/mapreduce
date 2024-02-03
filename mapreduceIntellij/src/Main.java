import java.io.IOException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        // Replace these paths with your actual file paths
        String inputFilePath = "texts/bible.txt";
        String outputDirPath = "chunks";
        int chunkSize = 30 * 1024 * 1024;

        try {
            FileSplitter.splitAndLowercase(inputFilePath, outputDirPath, chunkSize);
            System.out.println("File splitting and lowercase conversion completed successfully.");
        } catch (IOException e) {
            System.err.println("Error occurred: " + e.getMessage());
            e.printStackTrace();
        }
    }
}