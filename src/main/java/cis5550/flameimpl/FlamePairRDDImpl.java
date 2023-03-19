package cis5550.flameimpl;

import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlamePairRDDImpl implements FlamePairRDD {

    private final String tableName;
    private final KVSClient kvs;

    public FlamePairRDDImpl(String tableName, KVSClient kvsClient) {
        this.tableName = tableName;
        this.kvs = kvsClient;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public List<FlamePair> collect() throws Exception {
        List<FlamePair> result = new ArrayList<>();
        Iterator<Row> iterator = kvs.scan(tableName);
        while (iterator.hasNext()) {
            Row r = iterator.next();
            for (String column : r.columns()) {
                result.add(new FlamePair(r.key(), r.get(column)));
            }
        }
        return result;
    }

    @Override
    public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/foldByKey", dataToSend, FlamePairRDD.class, tableName, zeroElement);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        kvs.rename(tableName, tableNameArg);
    }

    @Override
    public FlameRDD flatMap(PairToStringIterable lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/flatMap/iterable", dataToSend, FlameRDD.class, tableName);
    }

    @Override
    public FlamePairRDD flatMapToPair(PairToPairIterable lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/flatMapToPair", dataToSend, FlamePairRDD.class, tableName);
    }

    @Override
    public FlamePairRDD join(FlamePairRDD other) throws Exception {
        FlamePairRDDImpl otherImpl = (FlamePairRDDImpl) other;
        return FlameContextImpl.invokeOperation("/rdd/join", null, FlamePairRDD.class, tableName, otherImpl.getTableName());
    }

    @Override
    public FlamePairRDD cogroup(FlamePairRDD other) throws Exception {
        return null;
    }
}