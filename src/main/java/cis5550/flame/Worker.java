package cis5550.flame;

import java.util.*;
import java.net.*;
import java.io.*;
import java.util.concurrent.atomic.*;

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
            String master = req.queryParams("master");
            try {
                FlameRDD.StringToIterable iterable = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    Iterable<String> opIterator = iterable.op(r.get(Utils.COLUMN_NAME));
                    System.out.println("Rows Data : " + r.key().charAt(0) + " : " + r.get(Utils.COLUMN_NAME));
                    if (opIterator == null) continue; //if the lambda returns null, skip
                    for (String s : opIterator) {
                        client.put(outputTable, Hasher.hash(UUID.randomUUID() + "-" + System.currentTimeMillis() + s), Utils.COLUMN_NAME, s);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/mapToPair", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            try {
                FlameRDD.StringToPair iterable = (FlameRDD.StringToPair) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                //hand the input string from input table to the lambda and get a pair back
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    FlamePair pair = iterable.op(row.get(Utils.COLUMN_NAME));
                    if (pair == null) continue; //if the lambda returns null, skip
                    client.put(outputTable, pair.a, row.key(), pair.b);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/foldByKey", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            String zeroElement = req.queryParams("arg");
            try {
                FlamePairRDD.TwoStringsToString iterable = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                //hand the input string from input table to the lambda and get a pair back
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    System.out.println("zeroElement: " + zeroElement);
                    Integer accumulator = Integer.parseInt(zeroElement);
                    for (String col : row.columns()) {
                        accumulator = Integer.parseInt(iterable.op(accumulator.toString(), row.get(col)));
                    }
                    client.put(outputTable, row.key(), Utils.COLUMN_NAME, accumulator.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/fromTable", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            FlameContext.RowToString iterable = (FlameContext.RowToString) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
            if (startKey.equals("null")) startKey = null;
            if (endKey.equals("null")) endKey = null;
            KVSClient client = new KVSClient(master);
            try {
                Iterator<Row> rows = client.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    if (iterable.op(row) == null) continue; //if the lambda returns null, skip
                    client.put(outputTable, row.key(), Utils.COLUMN_NAME, iterable.op(row));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/flatMapToPair", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            try {
                FlameRDD.StringToPairIterable iterable = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    Iterable<FlamePair> opIterator = iterable.op(r.get(Utils.COLUMN_NAME));
                    if (opIterator == null) continue; //if the lambda returns null, skip
                    for (FlamePair p : opIterator) {
                        client.put(outputTable, p.a, r.key(), p.b);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }
}