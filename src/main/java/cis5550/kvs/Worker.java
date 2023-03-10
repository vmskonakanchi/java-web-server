package cis5550.kvs;

import cis5550.webserver.Server;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class Worker extends cis5550.generic.Worker {

    private static final int MAX_THREADS = 10;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Enter the required <port> <storage directory> <ip:port>");
            System.exit(1);
        }
        //passing the port as a server
        Server.port(Integer.parseInt(args[0]));
        startPingThread(args[2], args[0], args[1]); // calling start ping thread

        DataManager dataManager = new DataManager(); // data structure for storing data
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS); // thread pool for handling requests

        // Lock for synchronizing PUT requests
        Object lock = new Object();

        Server.put("/data/:T/:R/:C", (req, res) -> {
            try {
                String tableName = req.params("T");
                String rowName = req.params("R");
                String columnName = req.params("C");
                if (req.queryParams().contains("ifcolumn") && req.queryParams().contains("equals")) {
                    String ifColumnName = req.queryParams("ifcolumn");
                    String ifColumnValue = req.queryParams("equals");

                    // Check if the ifcolumn exists and has the value specified in equals
                    int latestVersion = dataManager.getLatestVersion(tableName, rowName, columnName);
                    byte[] byteData = dataManager.get(tableName, rowName, ifColumnName, latestVersion) != null ? dataManager.get(tableName, rowName, ifColumnName, latestVersion) : new byte[0];
                    String data = new String(byteData, StandardCharsets.UTF_8);
                    if (!data.equals("") && data.equals(ifColumnValue)) {
                        // If the ifcolumn exists and has the value specified in equals, execute the PUT operation
                        synchronized (lock) { // acquire the lock
                            res.header("version", dataManager.put(tableName, rowName, columnName, req.bodyAsBytes()));
                        }
                        return "OK";
                    } else {
                        // If the ifcolumn does not exist or does not have the value specified in equals, return FAIL
                        return "FAIL";
                    }
                } else {
                    // If the query parameters are not present, execute the PUT operation
                    synchronized (lock) { // acquire the lock
                        res.header("version", dataManager.put(tableName, rowName, columnName, req.bodyAsBytes()));
                    }

                    return "OK";
                }
            } catch (Exception e) {
                res.status(404, "FAIL");
                return null;
            }
        });

        Server.get("/data/:T/:R/:C", (req, res) -> {
            try {
                String tableName = req.params("T");
                String rowName = req.params("R");
                String columnName = req.params("C");
                if (req.queryParams().contains("version")) {
                    int version = Integer.parseInt(req.queryParams("version"));
                    String data = new String(dataManager.get(tableName, rowName, columnName, version), StandardCharsets.UTF_8);
                    res.header("version", req.params("version"));
                    res.body(data);
                } else {
                    int latestVersion = dataManager.getLatestVersion(tableName, rowName, columnName);
                    String data = new String(dataManager.get(tableName, rowName, columnName, latestVersion), StandardCharsets.UTF_8);
                    res.header("version", String.valueOf(latestVersion));
                    res.body(data);
                }
            } catch (Exception e) {
                res.status(404, "FAIL");
            }
            return null;
        });
    }
}
