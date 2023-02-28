package cis5550.generic;

import cis5550.kvs.DataManager;
import cis5550.kvs.Row;
import cis5550.kvs.Table;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Worker {
    private static final long THREAD_SLEEP_TIME = 1000 * 5; // 5 seconds wait time for the thread to sleep

    private static final long GARBAGE_COLLECTION_WAIT_TIME = 1000 * 10; // time to collect garbage

    private static final long REPLICATION_TIME = 1000 * 5; // time to replicate

    public static String[] workerList = null;

    protected static boolean isLastRequestTime = false;

    /*
     * Starts a new thread and pings the server using given arguments
     * */
    protected static void startPingThread(String address, String portToPing, String filePath) {
        //starting the pining thread and hitting the master /ping every 5 seconds
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Hitting Master");
                    String idToPing = "";
                    try {
                        //checking if the directory exists
                        File file = new File(filePath);
                        if (file.exists() && file.isDirectory()) {
                            File idFile = new File(filePath, "id");
                            if (idFile.exists()) {
                                idToPing = Files.readString(idFile.toPath());
                            } else {
                                idToPing = String.valueOf(new Random().nextInt(1000000));
                                idFile.createNewFile();
                                Files.writeString(idFile.toPath(), idToPing);
                            }
                        } else {
                            idToPing = String.valueOf(new Random().nextInt(1000000));
                            file.mkdir();
                            new File(filePath, "id").createNewFile();
                            Files.writeString(Path.of(filePath, "id"), idToPing);
                        }
                        URL urlToPing = new URL("http://" + address + "/ping?id=" + idToPing + "&port=" + portToPing);
                        urlToPing.getContent();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 0, THREAD_SLEEP_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected static void collectGarbage(DataManager dataManager, String storageDirectory) {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (!isLastRequestTime) {
                        return;
                    }
                    System.out.println("Collecting garbage");
                    for (Table table : dataManager.getAllTables()) {
                        String currentLogFile = storageDirectory + "/" + table.getName();
                        String newLogFile = storageDirectory + "/" + "_new" + table.getName();
                        // Open the current log file and read all its entries into a list
                        List<String> currentEntries = readLogEntries(currentLogFile);
                        // Filter out entries for rows that are no longer in the table
                        List<String> filteredEntries = filterLogEntries(storageDirectory, currentEntries, table.getName());
                        // Open the new log file and write the filtered entries to it
                        writeLogEntries(newLogFile, filteredEntries);
                        // Atomically replace the current log file with the new one
                        replaceLogFile(currentLogFile, newLogFile);
                    }
                    isLastRequestTime = false;
                }
            }, 0, GARBAGE_COLLECTION_WAIT_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<String> readLogEntries(String logFile) {
        List<String> entries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                entries.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return entries;
    }

    private static List<String> filterLogEntries(String storageDirectory, List<String> entries, String table) {
        try {
            File file = new File(storageDirectory + "/" + table);
            long fileCount = Files.lines(file.toPath()).count();
            InputStream is = new FileInputStream(file);
            List<String> lines = new ArrayList<>();
            for (int i = 0; i < fileCount; i++) {
                Row r = Row.readFrom(is);
                lines.add(new String(r.toByteArray()));
            }
            Map<Integer, String> recentLines = new ConcurrentHashMap<>();
            for (String s : lines) {
                String[] split = s.split(" ");
                int index = Integer.parseInt(split[0]);
                if (recentLines.containsKey(index)) {
                    recentLines.replace(index, s);
                } else {
                    recentLines.put(index, s);
                }
            }
            is.close();
            return new ArrayList<>(recentLines.values());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static void writeLogEntries(String logFile, List<String> entries) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile))) {
            for (String entry : entries) {
                writer.write(entry);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void replaceLogFile(String currentLogFile, String newLogFile) {
        try {
            // Atomically replace the current log file with the new one
            File currentLogPath = new File(currentLogFile);
            File newLogPath = new File(newLogFile);
            if (currentLogPath.delete()) {
                if (newLogPath.renameTo(currentLogPath)) {
                    System.out.println("Garbage Collection Successful");
                }
            } else {
                System.out.println("Garbage Collection Failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void downloadWorkers(String masterAddress, int currentPort) {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        // download the list of workers from the master
                        URL url = new URL("http://" + masterAddress + "/getWorkers?port=" + currentPort);
                        String workers = new String(url.openStream().readAllBytes(), StandardCharsets.UTF_8);
                        workerList = workers.split("\n");
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }, 0, REPLICATION_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
