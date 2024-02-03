import java.io.*;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;

// Simple class to represent a key-value pair
class KeyValue {
    String key;
    List<Integer> value;

    KeyValue(String key, List<Integer> value) {
        this.key = key;
        this.value = value;
    }
}

public class MapReduce {
    // Updated readChunk function to extract words without numbers or punctuations
    // Updated readChunk function to filter out numbers and punctuation marks
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

        return filterWords(content.toString());
    }

    // Helper function to filter out numbers and punctuation marks
    private static String filterWords(String input) {
        // Use a regular expression to match words and filter out numbers or punctuation marks
        String regex = "\\b(?:[a-zA-Z]+)\\b";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        StringBuilder filteredContent = new StringBuilder();

        // Append matched words to the filtered content
        while (matcher.find()) {
            filteredContent.append(matcher.group()).append(" ");
        }

        return filteredContent.toString().trim();
    }

    // Function to save a List of KeyValues to a JSON file
    public static void saveJson(List<KeyValue> result, String outputPath) {
        try (FileWriter writer = new FileWriter(outputPath)) {
            Gson gson = new Gson();
            gson.toJson(result, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Function to read a JSON file and convert it back to a List of KeyValues
    public static List<KeyValue> readJson(String filePath) {
        List<KeyValue> keyValueList = new ArrayList<>();

        try (FileReader reader = new FileReader(filePath)) {
            Gson gson = new Gson();
            Type type = new TypeToken<List<KeyValue>>(){}.getType();
            keyValueList = gson.fromJson(reader, type);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return keyValueList;
    }

    // Function to map words in a chunk to the number 1 and store in a list of KeyValue objects
    public static List<KeyValue> map(String chunk) {
        List<KeyValue> wordList = new ArrayList<>();

        // Splitting the chunk into words and updating the word count list
        String[] words = chunk.split("\\s+");
        for (String word : words) {
            List<Integer> countList = new ArrayList<>();
            countList.add(1);
            wordList.add(new KeyValue(word, countList));
        }

        return wordList;
    }

    // Function to group repeated key values and store their values in the same array
    // Function to group repeated key values and store their values in the same array
    public static List<KeyValue> group(List<KeyValue> mapResult) {
        List<KeyValue> groupedList = new ArrayList<>();

        for (KeyValue keyValue : mapResult) {
            String key = keyValue.key;
            List<Integer> values = keyValue.value;

            // Check if the key already exists in the grouped list
            boolean keyExists = false;
            for (KeyValue groupItem : groupedList) {
                if (groupItem.key.equals(key)) {
                    keyExists = true;
                    groupItem.value.addAll(values);
                    break;
                }
            }

            // If the key is not in the grouped list, add it with the current values
            if (!keyExists) {
                groupedList.add(new KeyValue(key, new ArrayList<>(values)));
            }
        }

        // Sort the grouped list alphabetically based on keys
        Collections.sort(groupedList, (a, b) -> a.key.compareTo(b.key));

        return groupedList;
    }

    public static void main(String[] args) {
        // Example usage in the main function
        String filePath = "chunks/chunk_1.txt";
        String chunk = readChunk(filePath);

        saveJson(map(chunk), "output/map.json");

        // Group step
        List<KeyValue> mapResult = readJson("output/map.json");
        saveJson(group(mapResult), "output/group.json");

        //Reduce step
        // 
    }
}
