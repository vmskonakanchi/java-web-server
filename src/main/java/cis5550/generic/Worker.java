package cis5550.generic;

import cis5550.kvs.ReplicationItem;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Time;
import java.util.*;
import java.util.concurrent.BlockingQueue;

public class Worker {
    private static final long THREAD_SLEEP_TIME = 1000 * 5; // 5 seconds wait time for the thread
    private static final long GARBAGE_COLLECTION_WAIT_TIME = 1000 * 15;

    private static final long REPLICATION_TIME = 1000 * 5;

    /*
     * A Queue for processing the replication items
     * */
    protected static final Queue<ReplicationItem> replicationQueue = new LinkedList<>();

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

    protected static void collectGarbage() {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Collecting garbage");
                }
            }, 0, GARBAGE_COLLECTION_WAIT_TIME);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected static void doReplication(String masterAddress) {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    System.out.println("Replicating");
                    try {
                        // download the list of workers from the master
                        URL url = new URL("http://" + masterAddress + "/getWorkers");
                        String workers = new String(url.openStream().readAllBytes(), StandardCharsets.UTF_8);
                        String[] workerArray = workers.split("\n");

                        while (!replicationQueue.isEmpty()) {
                            ReplicationItem item = replicationQueue.poll();
                            //replicate the item to all the workers
                            for (String worker : workerArray) {
                                int result = item.replicate(worker);
                                if (result == 200) {
                                    System.out.println("Replication successful");
                                } else {
                                    System.out.println("Replication failed");
                                    replicationQueue.add(item);
                                }
                            }
                        }
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
