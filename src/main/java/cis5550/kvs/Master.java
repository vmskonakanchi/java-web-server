package cis5550.kvs;

import cis5550.webserver.Server;

public class Master extends cis5550.generic.Master {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Please provide a port number , example: 8080");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        // starting the webserver with the port number
        Server.port(port);
        // registering the routes for the webserver
        registerRoutes();
        // handling the requests to the home route
        Server.get("/", (req, res) -> workerTable());
        Server.get("/getWorkers", (req, res) -> getWorkerList(req.headers("HOST")));
    }
}
