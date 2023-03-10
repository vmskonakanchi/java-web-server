package cis5550.flameimpl;

import cis5550.constants.Utils;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlameRDDImpl implements FlameRDD {
    private final String tableName;

    public FlameRDDImpl(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public List<String> collect() throws Exception {
        KVSClient client = new KVSClient("localhost:8000");
        List<String> result = new ArrayList<>();
        Iterator<Row> iterator = client.scan(tableName);
        while (iterator.hasNext()) {
            Row r = iterator.next();
            result.add(r.get(Utils.COLUMN_NAME));
        }
        return result;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) throws Exception {
        return null;
    }

    @Override
    public FlameRDD intersection(FlameRDD r) throws Exception {
        return null;
    }

    @Override
    public FlameRDD sample(double f) throws Exception {
        return null;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) throws Exception {
        return null;
    }
}
