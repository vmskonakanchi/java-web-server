package cis5550.kvs;

import cis5550.webserver.Server;


import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class Worker extends cis5550.generic.Worker {

    private static final int MAX_THREADS = 2;

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Enter the required <port> <storage directory> <ip:port>");
            System.exit(1);
        }
        //passing the port as a server
        Server.port(Integer.parseInt(args[0]));
        startPingThread(args[2], args[0], args[1]); // calling start ping thread

        DataManager dataManager = new DataManager(args[1]); // data structure for storing data
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREADS);

        //creating a thread to load the data from the disk
        try {
            Thread t = new Thread(() -> {
                if (dataManager.loadDataFromDisk()) {
                    System.out.println("Loaded data from disk");
                } else {
                    System.out.println("Data not loaded successfully");
                }
            });
            t.start();
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Server.put("/data/:T/:R/:C", (req, res) -> {
            try {
                String tableName = req.params("T");
                String rowName = req.params("R");
                String columnName = req.params("C");
                if (req.queryParams().contains("ifcolumn") && req.queryParams().contains("equals")) {
                    String ifColumnName = req.queryParams("ifcolumn");
                    String ifColumnValue = req.queryParams("equals");

                    // Check if the ifcolumn exists and has the value specified in equals
                    byte[] byteData = dataManager.get(tableName, rowName, ifColumnName) != null ? dataManager.get(tableName, rowName, ifColumnName) : new byte[0];
                    String data = new String(byteData, StandardCharsets.UTF_8);
                    if (!data.equals("") && data.equals(ifColumnValue)) {
                        // If the ifcolumn exists and has the value specified in equals, execute the PUT operation
                        threadPool.execute(() -> {
                            dataManager.put(tableName, rowName, columnName, req.bodyAsBytes());
                        });
                        return "OK";
                    } else {
                        // If the ifcolumn does not exist or does not have the value specified in equals, return FAIL
                        return "FAIL";
                    }
                } else {
                    // If the query parameters are not present, execute the PUT operation
                    threadPool.execute(() -> {
                        dataManager.put(tableName, rowName, columnName, req.bodyAsBytes());
                    });
                    return "OK";
                }
            } catch (Exception e) {
                res.status(404, "FAIL");
                System.out.println(e.getMessage());
                return null;
            }
        });

        Server.get("/data/:T/:R/:C", (req, res) -> {
            try {
                String tableName = req.params("T");
                String rowName = req.params("R");
                String columnName = req.params("C");
                String data = new String(dataManager.get(tableName, rowName, columnName), StandardCharsets.UTF_8);
                res.body(data);
            } catch (Exception e) {
                e.printStackTrace();
                res.status(404, "FAIL");
            }
            return null;
        });

        Server.put("/persist/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if (dataManager.createTable(tableName)) {
                return "OK";
            } else {
                res.status(403, "FAIL");
                return null;
            }
        });

        Server.get("/", (req, res) -> dataManager.getTablesAsHtml());
        Server.get("/tables", (req, res) -> dataManager.getTables());

        Server.put("/delete/:tableName", (req, res) -> {
            try {
                String tableName = req.params("tableName");
                if (dataManager.deleteTable(tableName)) {
                    return "OK";
                } else {
                    res.status(404, "NOT FOUND");
                    return null;
                }
            } catch (Exception e) {
                res.status(404, "NOT FOUND");
                return null;
            }
        });

        Server.get("/view/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if (tableName == null) {
                res.status(404, "FAIL");
                return null;
            }
            return dataManager.getRowsFromTable(tableName);
        });

        Server.get("/count/:tableName", (req, res) -> {
            try {
                String tableName = req.params("tableName");
                return dataManager.countRowsFromTable(tableName);
            } catch (Exception e) {
                res.status(404, "NOT FOUND");
                return null;
            }
        });

        Server.put("/rename/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            String newTableName = req.body();
            if (dataManager.renameTable(tableName, newTableName)) {
                return "OK";
            } else {
                res.status(404, "FAIL");
                return null;
            }
        });

        Server.get("/data/:tableName/:rowName", (req, res) -> {
            String tableName = req.params("tableName");
            String rowName = req.params("rowName");
            try {
                if (!dataManager.hasTable(tableName)) {
                    res.status(404, "NOT FOUND");
                    return null;
                }
                res.bodyAsBytes(dataManager.getRow(tableName, rowName).toByteArray());
                return null;
            } catch (Exception e) {
                res.status(404, e.getMessage());
                return null;
            }
        });

        Server.get("/data/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if (!dataManager.hasTable(tableName)) {
                res.status(404, "NOT FOUND");
                return null;
            }
            if (req.queryParams().contains("startRow") && req.queryParams().contains("endRowExclusive")) {
                String startRow = req.queryParams("startRow");
                String endRow = req.queryParams("endRowExclusive");
            } else {
                res.body(dataManager.getRowDataFromTable(tableName));
            }
            return null;
        });

        Server.put("/data/:tableName", (req, res) -> {
            String tableName = req.params("tableName");
            if (!dataManager.hasTable(tableName)) {
                res.status(404, "NOT FOUND");
                return null;
            }
            if (dataManager.saveRows(tableName, req.body())) {
                return "OK";
            } else {
                return "FAIL";
            }
        });
    }
}
