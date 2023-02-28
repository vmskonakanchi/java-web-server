package cis5550.generic;

import cis5550.kvs.Entry;
import cis5550.webserver.Server;

import java.util.Hashtable;

public class Master {

    private static final Hashtable<String, Entry> workerMap = new Hashtable<>();

    public static String getWorkers() {
        StringBuilder builder = new StringBuilder();
        int count = (int) workerMap.values().stream().filter(Entry::isAlive).count();
        builder.append(count);
        builder.append("\n");
        for (Entry entry : workerMap.values()) {
            if (entry.isAlive()) {
                builder.append(entry.toText());
            }
        }
        return builder.toString();
    }

    public static String workerTable() {
        StringBuilder builder = new StringBuilder();
        builder.append("<html><body><table><tr><th>Worker ID</th><th>IP Address</th><th>Port Number</th><th>Link</th></tr>");
        for (Entry entry : workerMap.values()) {
            builder.append(entry.toHtml());
        }
        builder.append("</table></body></html>");
        return builder.toString();
    }

    protected static String getWorkerList(String requestedIp) {
        StringBuilder builder = new StringBuilder();
        for (Entry entry : workerMap.values()) {
            if (entry.isAlive() && !entry.getHost().equals(requestedIp)) {
                builder.append(entry.getHost()).append("\n");
            }
        }
        return builder.toString();
    }

    public static void registerRoutes() {
        // create routes for /ping and /workers using route table
        Server.get("/ping", (req, res) -> {
            if (!req.queryParams().contains("id") || !req.queryParams().contains("port")) {
                res.status(400, "ERROR");
                return "ERROR";
            }
            String id = req.queryParams("id");
            String portNumber = req.queryParams("port");
            if (workerMap.containsKey(id)) {
                workerMap.get(id).setPortNumber(portNumber);
                workerMap.get(id).setIpAddress(req.ip());
                workerMap.get(id).updateLastPing();
            } else {
                workerMap.put(id, new Entry(id, req.ip(), portNumber));
            }
            return "OK";
        });

        Server.get("/workers", (req, res) -> getWorkers());
    }
}
