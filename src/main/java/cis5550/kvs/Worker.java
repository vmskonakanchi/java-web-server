package cis5550.kvs;

import cis5550.webserver.Server;

import java.util.ArrayList;
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


        Server.put("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");
            new Thread(() -> {
                dataManager.put(tableName, rowName, columnName, req.body());
            }).start();
            return "OK";
        });

        Server.get("/data/:T/:R/:C", (req, res) -> {
            String tableName = req.params("T");
            String rowName = req.params("R");
            String columnName = req.params("C");
            res.body(dataManager.get(tableName, rowName, columnName));
            return null;
        });
    }
}
