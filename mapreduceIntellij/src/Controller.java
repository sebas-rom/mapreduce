import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

enum FileStatus {
    AVAILABLE,
    IN_USE,
    COMPLETED,
    FAILED
}

class Task1Thread implements Runnable {
    private String poolIdentifier;
    private String threadIdentifier;
    private String filePath;

    public Task1Thread(String poolIdentifier, String threadIdentifier, String filePath) {
        this.poolIdentifier = poolIdentifier;
        this.threadIdentifier = threadIdentifier;
        this.filePath = filePath;
    }

    @Override
    public void run() {
        try {
            // Simulate some processing
            System.out.println(poolIdentifier + " " + threadIdentifier + " Processing file: " + filePath);
            // If processing fails, mark the file as failed
            // For simplicity, let's assume processing always succeeds

            // Add a delay of 5 seconds after printing
            TimeUnit.SECONDS.sleep(5);
            System.out.println(poolIdentifier + " " + threadIdentifier + " Finished Processing file: " + filePath);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

public class Controller {
    private Map<String, FileStatus> fileStatusMap = new HashMap<>();
    private ExecutorService pool1 = Executors.newFixedThreadPool(2, r -> new Thread(r, "pool1-thread"));
    private ExecutorService pool2 = Executors.newFixedThreadPool(2, r -> new Thread(r, "pool2-thread"));

    public Controller() {
        initializeFileStatusMap();
        startProcessing();
        waitForCompletion();
        shutdownPools();
    }

    private void initializeFileStatusMap() {
        // Read the folder /chunks and initialize file status map
        File chunksFolder = new File("chunks"); // Assuming chunks folder is in the project root
        if (chunksFolder.exists() && chunksFolder.isDirectory()) {
            for (File file : chunksFolder.listFiles()) {
                fileStatusMap.put(file.getName(), FileStatus.AVAILABLE);
            }
        }
    }

    private void startProcessing() {
        int poolToggle = 1; // Toggle between pools
        for (String fileName : fileStatusMap.keySet()) {
            if (fileStatusMap.get(fileName) == FileStatus.AVAILABLE) {
                fileStatusMap.put(fileName, FileStatus.IN_USE);
                if (poolToggle == 1) {
                    pool1.submit(new Task1Thread("pool1", Thread.currentThread().getName(), fileName));
                } else {
                    pool2.submit(new Task1Thread("pool2", Thread.currentThread().getName(), fileName));
                }
                poolToggle = 3 - poolToggle; // Toggle between 1 and 2
            }
        }
    }

    private void waitForCompletion() {
        // Wait for all tasks to complete
        try {
            pool1.shutdown();
            pool1.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            pool2.shutdown();
            pool2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void shutdownPools() {
        pool1.shutdownNow();
        pool2.shutdownNow();
    }

    public static void main(String[] args) {
        new Controller();
    }
}
