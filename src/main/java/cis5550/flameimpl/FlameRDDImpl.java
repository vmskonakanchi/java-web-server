package cis5550.flameimpl;

import cis5550.constants.Utils;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.HTTP;
import cis5550.tools.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public class FlameRDDImpl implements FlameRDD {
    private final String tableName;
    private final KVSClient kvs;

    public FlameRDDImpl(String tableName, KVSClient kvsClient) {
        this.tableName = tableName;
        this.kvs = kvsClient;
    }

    @Override
    public int count() throws Exception {
        return kvs.count(tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        kvs.rename(tableName, tableNameArg);
    }

    @Override
    public FlameRDD distinct() throws Exception {
        return FlameContextImpl.invokeOperation("/rdd/distinct", null, FlameRDD.class, tableName);
    }

    @Override
    public Vector<String> take(int num) throws Exception {
        Iterator<Row> iterator = kvs.scan(tableName);
        Vector<String> result = new Vector<>();
        for (int i = 0; i < num; i++) {
            if (iterator.hasNext()) {
                Row r = iterator.next();
                result.add(r.get(Utils.COLUMN_NAME));
            }
        }
        return result;
    }

    @Override
    public String fold(String zeroElement, FlamePairRDD.TwoStringsToString lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        Vector<String> results = (Vector<String>) FlameContextImpl.invokeOperation("/rdd/fold", dataToSend, Vector.class, tableName, zeroElement);
        int finalResult = 0;
        for (String result : results) {
            finalResult += Integer.parseInt(result);
            System.out.println("Result: " + result);
        }
        return String.valueOf(finalResult);
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        Iterator<Row> iterator = kvs.scan(tableName);
        while (iterator.hasNext()) {
            Row r = iterator.next();
            result.add(r.get(Utils.COLUMN_NAME));
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        try {
            byte[] dataToSend = Serializer.objectToByteArray(lambda);
            return FlameContextImpl.invokeOperation("/rdd/flatMap", dataToSend, FlameRDD.class, tableName);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/flatMapToPair", dataToSend, FlamePairRDD.class, tableName);
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/mapToPair", dataToSend, FlamePairRDD.class, tableName);
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(r);
        return FlameContextImpl.invokeOperation("/rdd/intersection", dataToSend, FlameRDD.class, tableName);
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return FlameContextImpl.invokeOperation("/rdd/sample", null, FlameRDD.class, tableName, String.valueOf(f));
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/groupBy", dataToSend, FlamePairRDD.class, tableName);
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/filter", dataToSend, FlameRDD.class, tableName);
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/mapPartitions", dataToSend, FlameRDD.class, tableName);
    }
}