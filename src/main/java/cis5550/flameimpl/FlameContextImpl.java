package cis5550.flameimpl;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;

import java.util.List;

public class FlameContextImpl implements FlameContext {

    @Override
    public KVSClient getKVS() {
        return null;
    }

    @Override
    public void output(String s) {

    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        return null;
    }
}
