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

        post("/rdd/flatMap/iterable", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            try {
                FlamePairRDD.PairToStringIterable iterable = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    for (String col : r.columns()) {
                        Iterable<String> opIterator = iterable.op(new FlamePair(r.key(), r.get(col)));
                        if (opIterator == null) continue; //if the lambda returns null, skip
                        for (String s : opIterator) {
                            client.put(outputTable, s, Utils.COLUMN_NAME, s);
                        }
                    }
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
                FlamePairRDD.PairToPairIterable iterable = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(req.bodyAsBytes(), myJAR);
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    for (String s : r.columns()) {
                        Iterable<FlamePair> opIterator = iterable.op(new FlamePair(r.key(), r.get(s)));
                        if (opIterator == null) continue; //if the lambda returns null, skip
                        for (FlamePair p : opIterator) {
                            client.put(outputTable, p.a, Utils.COLUMN_NAME, p.b);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/distinct", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            try {
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    client.put(outputTable, r.get(Utils.COLUMN_NAME), Utils.COLUMN_NAME, r.get(Utils.COLUMN_NAME));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/join", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
            String outputTable = req.queryParams("outputTable");
            String anotherTable = req.queryParams("arg");
            String startKey = req.queryParams("startKey");
            String endKey = req.queryParams("endKey");
            String master = req.queryParams("master");
            try {
                KVSClient client = new KVSClient(master);
                if (startKey.equals("null")) startKey = null;
                if (endKey.equals("null")) endKey = null;
                Iterator<Row> iterator = client.scan(inputTable, startKey, endKey);
                while (iterator.hasNext()) {
                    Row r = iterator.next();
                    Iterator<Row> anotherIterator = client.scan(anotherTable, startKey, endKey);
                    while (anotherIterator.hasNext()) {
                        Row anotherRow = anotherIterator.next();
                        if (r.key().equals(anotherRow.key())) {
                            for (String col : r.columns()) {
                                for (String anotherCol : anotherRow.columns()) {
                                    client.put(outputTable, r.key(), col + anotherCol, r.get(col) + "," + anotherRow.get(anotherCol));
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });

        post("/rdd/fold", (req, res) -> {
            String inputTable = req.queryParams("inputTable");
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
                int accumulator = Integer.parseInt(zeroElement);
                while (iterator.hasNext()) {
                    Row row = iterator.next();
                    for (String col : row.columns()) {
                        accumulator = Integer.parseInt(iterable.op(Integer.toString(accumulator), row.get(col)));
                    }
                }
                res.body(Integer.toString(accumulator));
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }
}