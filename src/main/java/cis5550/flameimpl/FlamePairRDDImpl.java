package cis5550.flameimpl;

import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;

import java.util.List;

public class FlamePairRDDImpl implements FlameRDD {
    @Override
    public List<String> collect() throws Exception {
        return null;
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
