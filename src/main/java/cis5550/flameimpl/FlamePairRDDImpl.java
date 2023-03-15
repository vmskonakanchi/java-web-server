package cis5550.flameimpl;

import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
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
}
