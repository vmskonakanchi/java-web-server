package cis5550.generic;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

public class Worker {
    private static final long THREAD_SLEEP_TIME = 1000 * 5; // 5 seconds wait time for the thread

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
                    Path pathToFile = Path.of(filePath, "id");
                    System.out.println("Hitting Master");
                    String idToPing = "";
                    try {
                        if (Files.exists(pathToFile)) {
                            idToPing = Files.readString(pathToFile, StandardCharsets.UTF_8);
                        } else {
                            Random r = new Random(System.currentTimeMillis());
                            idToPing = String.valueOf(10000 + r.nextInt(20000));
                            Files.createFile(pathToFile);
                        }
                        URL toPingUrl = new URL("http://" + address + "/ping?id=" + idToPing + "&port=" + portToPing);
                        toPingUrl.getContent();
                    } catch (Exception e) {
                        throw new RuntimeException("Error : " + e.getMessage());
                    }
                }
            }, 0, THREAD_SLEEP_TIME);
        } catch (Exception e) {
            System.out.println("Error : " + e.getMessage());
        }

    }
}
