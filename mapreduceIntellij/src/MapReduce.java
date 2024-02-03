import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


// Simple class to represent a key-value pair
class KeyValue {
    String key;
    Integer value;

    KeyValue(String key, Integer value) {
        this.key = key;
        this.value = value;
    }
}

public class MapReduce {


    // Function to convert List<Map.Entry<String, Integer>> to List<KeyValue>
    private static List<KeyValue> convertToKeyValueList(List<Map.Entry<String, Integer>> entryList) {
        List<KeyValue> keyValueList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : entryList) {
            keyValueList.add(new KeyValue(entry.getKey(), entry.getValue()));
        }
        return keyValueList;
    }

    // Function to convert List<KeyValue> to List<Map.Entry<String, Integer>>
    private static List<Map.Entry<String, Integer>> convertToMapEntryList(List<KeyValue> keyValueList) {
        List<Map.Entry<String, Integer>> entryList = new ArrayList<>();
        for (KeyValue keyValue : keyValueList) {
            entryList.add(Map.entry(keyValue.key, keyValue.value));
        }
        return entryList;
    }

    // Function to save a List of Map Entries to a JSON file
    public static void saveJson(List<Map.Entry<String, Integer>> result, String outputPath) {
        List<KeyValue> keyValueList = convertToKeyValueList(result);
        try (FileWriter writer = new FileWriter(outputPath)) {
            Gson gson = new Gson();
            gson.toJson(keyValueList, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Function to read a JSON file and convert it back to a List of Map Entries
    public static List<Map.Entry<String, Integer>> readJson(String filePath) {
        List<KeyValue> keyValueList = new ArrayList<>();

        try (FileReader reader = new FileReader(filePath)) {
            Gson gson = new Gson();
            Type type = new TypeToken<List<KeyValue>>(){}.getType();
            keyValueList = gson.fromJson(reader, type);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return convertToMapEntryList(keyValueList);
    }


    // Function to read a chunk from a file and return its content as a string
    public static String readChunk(String filepath) {
        StringBuilder content = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            String line;
            while ((line = br.readLine()) != null) {
                content.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return content.toString();
    }

    // Function to map words in a chunk to the number 1 and store in a list of tuples
    public static List<Map.Entry<String, Integer>> map(String chunk) {
        Map<String, Integer> wordCountMap = new HashMap<>();

        // Splitting the chunk into words and updating the word count map
        String[] words = chunk.split("\\s+");
        for (String word : words) {
            wordCountMap.put(word, 1);
        }

        // Converting the map entries to a list of tuples
        List<Map.Entry<String, Integer>> wordList = new ArrayList<>(wordCountMap.entrySet());

        return wordList;
    }
    // Helper method
    // to create directories if they don't exist
    private static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            boolean created = directory.mkdirs();
            if (created) {
                System.out.println("Directory created: " + directoryPath);
            } else {
                System.out.println("Failed to create directory: " + directoryPath);
            }
        }
    }

    public static void main(String[] args) {
        // Example usage in the main function
        String filePath = "chunks/chunk_1.txt";
        String chunk = readChunk(filePath);

        String outputPath = "output/result.json";
        saveJson(map(chunk), outputPath);

        //Group step
        //
        // Reading the JSON file back to a List of Map Entries
        List<Map.Entry<String, Integer>> readResult = readJson(outputPath);

    }
}
