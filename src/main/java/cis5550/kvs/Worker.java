package cis5550.kvs;

import cis5550.webserver.Server;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
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
        ExecutorService threadPool = Executors.newFixedThreadPool(1000); // thread pool for handling requests

        Server.put("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");

            if (req.queryParams().contains("ifcolumn") && req.queryParams().contains("equals")) {
                String ifColumnName = req.queryParams("ifcolumn");
                String ifColumnValue = req.queryParams("equals");

                // Check if the ifcolumn exists and has the value specified in equals
                String currentColumnValue = dataManager.get(tableName, rowName, ifColumnName);
                if (currentColumnValue != null && currentColumnValue.equals(ifColumnValue)) {
                    // If the value matches, execute the PUT operation
                    threadPool.execute(() -> dataManager.put(tableName, rowName, columnName, req.body()));
                    return "OK";
                } else {
                    // If the value does not match, return FAIL
                    return "FAIL";
                }
            } else {
                // If the query parameters are not present, execute the PUT operation
                threadPool.execute(() -> dataManager.put(tableName, rowName, columnName, req.body()));
                return "OK";
            }
        });

        Server.get("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");
            String data = dataManager.get(tableName, rowName, columnName);
            res.body(data != null ? data : "FAIL");
            return null;
        });
    }
}
