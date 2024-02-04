import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class Controller {
    private final Map<Integer, String> chunkState = new ConcurrentHashMap<>();
    private final List<Integer> availableChunks = new ArrayList<>();
    private final List<Integer> completedChunks = new ArrayList<>();
    private final List<Integer> failedChunks = new ArrayList<>();
    private final Object lock = new Object();
    private final int totalChunks;
    private final int halfChunks;

    public Controller(int totalChunks) {
        this.totalChunks = totalChunks;
        this.halfChunks = (totalChunks + 1) / 2;
    }

    public void initializeChunks(String folderPath) {
        File folder = new File(folderPath);
        for (File file : folder.listFiles()) {
            if (file.isFile() && file.getName().endsWith(".txt")) {
                int chunkId = Integer.parseInt(file.getName().split("_")[1].split("\\.")[0]);
                chunkState.put(chunkId, "available");
                availableChunks.add(chunkId);
            }
        }
    }
    public int getHalfChunks() {
        return halfChunks;
    }
    public int getNextAvailableChunk() {
        synchronized (lock) {
            if (!availableChunks.isEmpty()) {
                return availableChunks.remove(0);
            } else {
                return -1;
            }
        }
    }

    public void markChunkCompleted(int chunkId) {
        synchronized (lock) {
            chunkState.put(chunkId, "completed");
            completedChunks.add(chunkId);
        }
    }

    public void markChunkFailed(int chunkId) {
        synchronized (lock) {
            chunkState.put(chunkId, "failed");
            failedChunks.add(chunkId);
        }
    }
}

class MapNode implements Runnable {
    private final int threadID;
    private final String name;
    private final Controller controller;
    private final int executorId;
    private final int maxChunksPerExecutor;
    private int processedChunks = 0;

    public MapNode(int threadID, String name, Controller controller, int executorId, int maxChunksPerExecutor) {
        this.threadID = threadID;
        this.name = name;
        this.controller = controller;
        this.executorId = executorId;
        this.maxChunksPerExecutor = maxChunksPerExecutor;
    }

    @Override
    public void run() {
        while (true) {
            int chunkId = controller.getNextAvailableChunk();
            if (chunkId != -1) {
                if (processedChunks >= maxChunksPerExecutor) {
                    System.out.println(name + " reached maximum chunks. Stopping.");
                    break;
                }

                try {
                    System.out.println("computer"+executorId + " processing chunk_" + chunkId);
                    MapReduce.saveJson(MapReduce.map(MapReduce.readChunk("chunks/chunk_"+chunkId+".txt")), "output/map"+executorId+"/" + chunkId + ".json");
                    processedChunks++;
                    System.out.println("\tcomputer"+executorId + " completed chunk_" + chunkId);
                } catch (Exception e) {
                    System.out.println(name + " failed processing chunk_" + chunkId + ": " + e.getMessage());
                    controller.markChunkFailed(chunkId);
                }
            } else {
                System.out.println(name + " no more chunks available. Stopping.");
                break;
            }
        }
    }
}

public class Main {
    public static void runMapReduceTask(int executorId, Controller controller, int maxChunksPerExecutor) {
        ExecutorService mapExecutor = Executors.newFixedThreadPool(2);

        List<Future<?>> mapThreads = new ArrayList<>();
        for (int i = 1; i <= maxChunksPerExecutor; i++) {
            MapNode node = new MapNode(1, "mapNode-" + i + "-" + executorId, controller, executorId, maxChunksPerExecutor);
            Future<?> future = mapExecutor.submit(node);
            mapThreads.add(future);
        }

        for (Future<?> future : mapThreads) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        mapExecutor.shutdown();
        System.out.println("Map Executor " + executorId + " finished. Starting groupNode.");
    }

    public static void runComputers(int computerNumber) {
        File[] files = new File("chunks").listFiles(file -> file.isFile() && file.getName().endsWith(".txt"));

        if (files != null) {
            int totalChunks = files.length;
            Controller controller = new Controller(totalChunks);
            controller.initializeChunks("chunks");

            ExecutorService executor = Executors.newFixedThreadPool(computerNumber);
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 1; i <= computerNumber; i++) {
                final int executorId = i;
                Future<?> future = executor.submit(() -> runMapReduceTask(executorId, controller, controller.getHalfChunks()));
                futures.add(future);
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
            System.out.println("Exiting Main Thread");
        } else {
            System.err.println("Error: Unable to retrieve files from the 'chunks' directory.");
        }
    }

    public static void main(String[] args) {
        runComputers(2);
    }
}
