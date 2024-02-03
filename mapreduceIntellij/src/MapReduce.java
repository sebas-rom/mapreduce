import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;


class KeyValue {
    String key;
    List<Integer> value;

    KeyValue(String key, List<Integer> value) {
        this.key = key;
        this.value = value;
    }
}

public class MapReduce {
    public static String readChunk(String filepath) {
        StringBuilder content = new StringBuilder();

        try (BufferedReader br = new BufferedReader(new FileReader(filepath))) {
            br.lines().forEach(line -> content.append(line).append("\n"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return filterWords(content.toString());
    }

    private static String filterWords(String input) {
        String regex = "\\b(?:[a-zA-Z]+)\\b";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        StringBuilder filteredContent = new StringBuilder();

        while (matcher.find()) {
            filteredContent.append(matcher.group()).append(" ");
        }

        return filteredContent.toString().trim();
    }

    public static void saveJson(List<KeyValue> result, String outputPath) {
        try (FileWriter writer = new FileWriter(outputPath)) {
            new Gson().toJson(result, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<KeyValue> readJson(String filePath) {
        List<KeyValue> keyValueList = new ArrayList<>();

        try (FileReader reader = new FileReader(filePath)) {
            Type type = new TypeToken<List<KeyValue>>(){}.getType();
            keyValueList = new Gson().fromJson(reader, type);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return keyValueList;
    }

    public static List<KeyValue> map(String chunk) {
        List<KeyValue> wordList = new ArrayList<>();

        String[] words = chunk.split("\\s+");
        for (String word : words) {
            wordList.add(new KeyValue(word, Collections.singletonList(1)));
        }

        return wordList;
    }

    public static List<KeyValue> group(List<KeyValue> mapResult) {
        Map<String, List<Integer>> groupedMap = new HashMap<>();

        for (KeyValue keyValue : mapResult) {
            groupedMap.computeIfAbsent(keyValue.key, k -> new ArrayList<>()).addAll(keyValue.value);
        }

        return groupedMap.entrySet().stream()
                .map(entry -> new KeyValue(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(kv -> kv.key))
                .toList();
    }

    public static List<KeyValue> reduce(List<KeyValue> groupResult) {
        return groupResult.stream()
                .map(keyValue -> new KeyValue(keyValue.key, Collections.singletonList(keyValue.value.stream().mapToInt(Integer::intValue).sum())))
                .toList();
    }

    private static void createDirectory(String directoryPath) {
        File directory = new File(directoryPath);
        if (!directory.exists() && directory.mkdirs()) {
            System.out.println("Directory created: " + directoryPath);
        } else {
            System.out.println("Failed to create directory: " + directoryPath);
        }
    }

    public static void deleteDirectory(String path) {
        File directory = new File(path);

        if (directory.exists()) {
            Arrays.stream(directory.listFiles()).forEach(file -> {
                if (file.isDirectory()) {
                    deleteDirectory(file.getAbsolutePath());
                } else {
                    file.delete();
                }
            });

            if (directory.delete()) {
                System.out.println("Directory deleted: " + path);
            } else {
                System.out.println("Failed to delete directory: " + path);
            }
        } else {
            System.out.println("Directory does not exist: " + path);
        }
    }

    public static void directoryUtils(){
        List.of("output", "output/map", "output/group", "output/reduce", "output/join").forEach(MapReduce::deleteDirectory);
        List.of("output", "output/map", "output/group", "output/reduce", "output/join").forEach(MapReduce::createDirectory);
    }

    public static void mapTask(){
        // Implement the map task logic here
    }

    public static void main(String[] args) {
        directoryUtils();

        String filePath = "chunks/chunk_1.txt";
        String chunk = readChunk(filePath);

        saveJson(map(chunk), "output/map.json");

        saveJson(group(readJson("output/map.json")), "output/group.json");

        saveJson(reduce(readJson("output/group.json")), "output/reduced.json");
    }
}
