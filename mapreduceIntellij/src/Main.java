import java.io.File;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

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

    public List<Integer> getFailedChunks() {
        return failedChunks;
    }

    public List<Integer> getAvailableChunks() {
        return availableChunks;
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
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            while (!controller.getAvailableChunks().isEmpty() || !controller.getFailedChunks().isEmpty()) {

                int chunkId = controller.getNextAvailableChunk();
                AtomicBoolean isFinished = new AtomicBoolean(false);
                while (!isFinished.get()) {
                    if (chunkId != -1) {
                        if (processedChunks >= maxChunksPerExecutor) {
                            System.out.println(name + " reached maximum chunks. Stopping.");
                            break;
                        }

                        Future<?> future = executorService.submit(() -> {
                            try {
                                System.out.println("\tcomputer" + executorId + " Processing chunk_" + chunkId);
                                MapReduce.saveJson(MapReduce.map(MapReduce.readChunk("chunks/chunk_" + chunkId + ".txt")), "output/map" + executorId + "/" + chunkId + ".json");
                                System.out.println("\tcomputer" + executorId + " completed chunk_" + chunkId);
                                processedChunks++;
                                isFinished.set(true); // Update the flag when the work is done
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

                        try {
                            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                            future.get(35, TimeUnit.SECONDS);//BREAK HERE for map node
                            //------------------------------------------------------------------------------------------------------------------------------------------------------------------------

                        } catch (TimeoutException e) {
                            System.err.println(name + " timed out processing chunk_" + chunkId);
//                            controller.markChunkFailed(chunkId);
                        } catch (Exception e) {
                            System.err.println(name + " failed processing chunk_" + chunkId + ": ");
//                            controller.markChunkFailed(chunkId);
                        }
                    } else if (!controller.getFailedChunks().isEmpty()) {
                        int failedChunkId = controller.getFailedChunks().remove(0);
                        System.err.println(name + " retrying failed chunk_" + failedChunkId);

                        try {
                            MapReduce.saveJson(MapReduce.map(MapReduce.readChunk("chunks/chunk_" + failedChunkId + ".txt")), "output/map" + executorId + "/" + failedChunkId + ".json");
                            System.out.println("\tcomputer" + executorId + " completed chunk_" + failedChunkId);
                            processedChunks++;
                        } catch (Exception e) {
                            System.out.println(name + " failed processing chunk_" + failedChunkId + ": " + e.getMessage());
                            controller.markChunkFailed(failedChunkId);
                        }
                    }
                }

            }
            System.out.println(name + " no more chunks available. Stopping.");
        } finally {
            executorService.shutdown();
        }
    }

}

class GroupNode implements Runnable {
    private final Controller controller;
    private final int executorId;

    public GroupNode(Controller controller, int executorId) {
        this.controller = controller;
        this.executorId = executorId;
    }

    @Override
    public void run() {
        MapReduce.deleteDirectory("output/group" + executorId);
        MapReduce.deleteDirectory("output/group" + executorId + "final");
        MapReduce.createDirectory("output/group" + executorId);
        MapReduce.createDirectory("output/group" + executorId + "final");
        System.out.println("GroupNode for executor " + executorId + " is running.");

        // Assuming "output/map" + executorId is the directory to list files from
        File mapOutputDirectory = new File("output/map" + executorId);

//        Grouping individuall files into indiviual outputs
        File[] mapFiles = mapOutputDirectory.listFiles();
        if (mapFiles != null) {
            for (File file : mapFiles) {
                System.out.println("Computer" + executorId + " executing " + file.getName());
                List<KeyValue> temporal = new ArrayList<>(MapReduce.readJson("output/map" + executorId + "/" + file.getName()));
                MapReduce.saveJson(MapReduce.group(temporal), "output/group" + executorId + "/" + file.getName());
//                System.out.println("Computer" + executorId + " executing " + file.getName() + "reached 3");
            }
        }

//      Grouping all individual outputs into 1
        File GroupOutputDirectory = new File("output/group" + executorId);

        File[] groupFiles = GroupOutputDirectory.listFiles();
        if (groupFiles != null) {
            List<KeyValue> temporal = new ArrayList<>();
            for (File file : groupFiles) {
                System.out.println("\t\tgroupNode" + executorId + " processing final:" + file.getName());
                temporal.addAll(MapReduce.readJson("output/group" + executorId + "/" + file.getName()));

            }
            MapReduce.saveJson(MapReduce.group(temporal), "output/group" + executorId + "final/group.json");
        }


        System.out.println("GroupNode for executor " + executorId + " finished.");
    }


}

class ReduceNode implements Runnable {
    private final Controller controller;
    private final int executorId;

    public ReduceNode(Controller controller, int executorId) {
        this.controller = controller;
        this.executorId = executorId;

    }

    @Override
    public void run() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            AtomicBoolean isFinished = new AtomicBoolean(false);
            while (!isFinished.get()) {
                Future<?> future = executorService.submit(() -> {
                    try {
                        System.out.println("\treducer" + executorId + " started working");
                        // Assuming "output/map" + executorId is the directory to list files from
                        File mapOutputDirectory = new File("output/group" + executorId + "final");

                        //Reducing
                        File[] mapFiles = mapOutputDirectory.listFiles();
                        if (mapFiles != null) {
                            for (File file : mapFiles) {
                                System.out.println("Reducer" + executorId + " executing " + file.getName());
                                List<KeyValue> temporal = new ArrayList<>(MapReduce.readJson("output/group" + executorId + "final" + "/" + file.getName()));
                                MapReduce.saveJson(MapReduce.reduce(temporal), "output/reduce" + "/" + "reduceResult" + executorId+".json");
                            }
                        }
                        isFinished.set(true); // Update the flag when the work is done
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                try {
                    //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    future.get(35, TimeUnit.SECONDS); //Break Here for reducer node  ---------------------------------
                    //------------------------------------------------------------------------------------------------------------------------------------------------------------------------System.out.println("\treducer" + executorId + " completed reduce");
                } catch (TimeoutException e) {
                    System.err.println("reducer timed out processing ");
                } catch (Exception e) {
                    System.err.println("reducer failed processing ");
                }
            }
        } finally {
            executorService.shutdown();
        }
    }

}

class ReduceFinalNode implements Runnable {

    public ReduceFinalNode() {

    }

    @Override
    public void run() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            AtomicBoolean isFinished = new AtomicBoolean(false);
            while (!isFinished.get()) {
                Future<?> future = executorService.submit(() -> {
                    try {
                        System.out.println("\n\t\tFinal reducer started working");
                        // Assuming "output/map" + executorId is the directory to list files from
                        File mapOutputDirectory = new File("output/reduce");

                        //Reducing
                        File[] mapFiles = mapOutputDirectory.listFiles();
                        List<KeyValue> temporal = new ArrayList<>();
                        if (mapFiles != null) {
                            for (File file : mapFiles) {
                                System.out.println("Final reducer executing " + file.getName());
                                temporal.addAll(MapReduce.readJson("output/reduce/" + file.getName()));
                            }

                            MapReduce.saveJson(MapReduce.reduce( MapReduce.group(temporal)), "output/result" + "/" + "finalResult.json");
                        }
                        isFinished.set(true); // Update the flag when the work is done
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                try {
                    //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    future.get(35, TimeUnit.SECONDS); //BREAK HERE for final reducer
                    //------------------------------------------------------------------------------------------------------------------------------------------------------------------------
                    System.out.println("\tFinal reducer completed reduce");
                } catch (TimeoutException e) {
                    System.err.println("Final reducer timed out processing ");
                } catch (Exception e) {
                    System.err.println("Final reducer failed processing ");
                }
            }
        } finally {
            executorService.shutdown();
        }
    }

}

class NamedThreadFactory implements ThreadFactory {
    private final String namePrefix;

    public NamedThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(namePrefix + t.getId());
        return t;
    }
}


public class Main {
    public static void directoryUtils() {
        List.of("output").forEach(MapReduce::deleteDirectory);
        List.of("output", "output/map1", "output/map2", "output/group1", "output/group2", "output/group1final", "output/group2final", "output/reduce", "output/result", "output/join").forEach(MapReduce::createDirectory);
    }

    public static void runMapReduceTask(int executorId, Controller controller, int maxChunksPerExecutor) {
        ExecutorService mapExecutor = Executors.newFixedThreadPool(2, new NamedThreadFactory("MapExecutor-" + executorId + "-Thread-"));

        List<Future<?>> mapThreads = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {  // Change the loop limit to 2
            System.out.println("Starting computer" + executorId + "-thread-" + i);
            MapNode node = new MapNode(i, "mapNode-" + executorId + "-" + i, controller, executorId, maxChunksPerExecutor);

            // Instead of running the node directly, submit it to the executor
            Future<?> future = mapExecutor.submit(node);
            mapThreads.add(future);
        }

        for (Future<?> future : mapThreads) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("An error occurred during map processing.");
            }
        }

        mapExecutor.shutdown();
        System.out.println("Map Executor " + executorId + " finished.");

        // Create a new thread to execute when mapExecutor is finished
        Thread groupNodeThread = new Thread(() -> {
            boolean success = false;
            int attempts = 0;
            int MAX_RETRIES = 3;
            while (!success && attempts < MAX_RETRIES) {
                try {
                    System.out.println("Starting groupNode attempt " + (attempts + 1) + " for executor " + executorId);
                    // Assuming the GroupNode class is implemented similarly to MapNode
                    GroupNode groupNode = new GroupNode(controller, executorId);
                    groupNode.run();  // or use the executor to submit the groupNode if it implements Runnable
                    success = true;
                    System.out.println("GroupNode finished successfully.");
                } catch (Exception e) {
                    System.err.println("Error in GroupNode attempt " + (attempts + 1) + " for executor " + executorId);
                    attempts++;
                }
            }

            if (!success) {
                System.err.println("GroupNode failed after " + MAX_RETRIES + " attempts. Skipping.");
            }
        });

        groupNodeThread.start();

        try {
            groupNodeThread.join(); // Wait for groupNodeThread to finish before proceeding
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Reduce node
        ExecutorService reduceExecutor = Executors.newFixedThreadPool(1, new NamedThreadFactory("ReduceExecutor-" + executorId + "-Thread-"));

        List<Future<?>> reduceThreads = new ArrayList<>();
        for (int i = 1; i <= 1; i++) {
            System.out.println("Starting reducer" + executorId);
            ReduceNode reduceNode = new ReduceNode(controller, executorId);

            // Instead of running the node directly, submit it to the executor
            Future<?> futureReduce = reduceExecutor.submit(reduceNode);
            reduceThreads.add(futureReduce);
        }

        for (Future<?> futureReduce : reduceThreads) {
            try {
                futureReduce.get();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("An error occurred during map processing.");
            }
        }

        reduceExecutor.shutdown();
        System.out.println("Reduce Executor " + executorId + " finished.");
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

            // Shutdown the executor for runMapReduceTask
            executor.shutdown();

            // Run ReduceFinalNode
            ExecutorService finalReduceExecutor = Executors.newSingleThreadExecutor(new NamedThreadFactory("FinalReduceExecutor-Thread-"));
            Future<?> finalReduceFuture = finalReduceExecutor.submit(new ReduceFinalNode());

            try {
                finalReduceFuture.get(); // Wait for ReduceFinalNode to finish before proceeding
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }

            finalReduceExecutor.shutdown();

            System.out.println("Exiting Main Thread");
        } else {
            System.err.println("Error: Unable to retrieve files from the 'chunks' directory.");
        }
    }

    public static void generateChunks() {
        int ChunkSize = 30 * 1024 * 1024;
        try {
            FileSplitter.splitAndLowercase("texts/bible.txt", "chunks", ChunkSize);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public static void main(String[] args) {
//        generateChunks();
        directoryUtils();
        runComputers(2);
    }
}