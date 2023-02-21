package cis5550.kvs;

import cis5550.webserver.Server;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class Worker extends cis5550.generic.Worker {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Enter the required <port> <storage directory> <ip:port>");
            System.exit(1);
        }
        //passing the port as a server
        Server.port(Integer.parseInt(args[0]));
        startPingThread(args[2], args[0], args[1]); // calling start ping thread

        DataManager dataManager = new DataManager(); // data structure for storing data
        ExecutorService threadPool = Executors.newFixedThreadPool(10); // thread pool for handling requests

        Server.put("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");
            if (req.queryParams().contains("ifcolumn") && req.queryParams().contains("equals")) {
                String ifColumnName = req.queryParams("ifcolumn");
                String ifColumnValue = req.queryParams("equals");
                int latestVersion = dataManager.getVersions(tableName, rowName, columnName);
                String data = dataManager.get(tableName, rowName, ifColumnName, latestVersion);

                // Check if the ifcolumn exists and has the value specified in equals
                if (data != null && data.equals(ifColumnValue)) {
                    // If the ifcolumn exists and has the value specified in equals, execute the PUT operation
                    threadPool.execute(() -> {
                        res.header("version", dataManager.put(tableName, rowName, columnName, req.body()));
                    });
                    return "OK";
                } else {
                    // If the ifcolumn does not exist or does not have the value specified in equals, return FAIL
                    return "FAIL";
                }
            } else {
                // If the query parameters are not present, execute the PUT operation
                threadPool.execute(() -> {
                    res.header("version", dataManager.put(tableName, rowName, columnName, req.body()));
                });
                return "OK";
            }
        });

        Server.get("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");
            if (req.queryParams().contains("version")) {
                int version = Integer.parseInt(req.queryParams("version"));
                String data = dataManager.get(tableName, rowName, columnName, version);
                res.header("version", req.params("version"));
                res.body(data != null ? data : "FAIL");
            } else {
                int latestVersion = dataManager.getVersions(tableName, rowName, columnName);
                String data = dataManager.get(tableName, rowName, columnName, latestVersion);
                res.header("version", String.valueOf(latestVersion));
                res.body(data != null ? data : "FAIL");
            }
            return null;
        });
    }
}
