package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;

import static cis5550.webserver.Server.*;

import cis5550.constants.Utils;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;
import cis5550.webserver.Request;
import jdk.jshell.execution.Util;

class Worker extends cis5550.generic.Worker {

    public static void main(String args[]) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <masterIP:port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String server = args[1];
        startPingThread(server, "" + port, port);
        final File myJAR = new File("__worker" + port + "-current.jar");

        port(port);

        post("/useJAR", (request, response) -> {
            FileOutputStream fos = new FileOutputStream(myJAR);
            fos.write(request.bodyAsBytes());
            fos.close();
            return "OK";
        });

        post("/rdd/flatMap", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String masterAddress = "localhost:8000";
            String jarName = "tests/flame-flatmap.jar";
            try {
                FlameRDD.StringToIterable iterable = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), new File(jarName));
                KVSClient client = new KVSClient(masterAddress);
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                int i = 0;
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    Iterable<String> opIterator = iterable.op(r.key());
                    if (opIterator != null) {
                        for (String s : opIterator) {
                            String rowName = Hasher.hash(s + i);
                            client.put(outputTable, rowName, Utils.COLUMN_NAME, s);
                            i++;
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }
}
