package cis5550.flameimpl;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FlameContextImpl implements FlameContext {
    private static Map<String, String> jobs = new ConcurrentHashMap<>();
    private static int jobNumber = 1; // the first job number will be 1
    public static final String COLUMN_NAME = "value";
    private final String jarName;

    public FlameContextImpl(String name) {
        jarName = name;
    }

    @Override
    public KVSClient getKVS() {
        return null;
    }

    @Override
    public void output(String s) {
        jobs.put(s, s); //adding job to the job hashmap - which we will use in the feature
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String jobId = getJobId(); //generating a new job id for each job
        KVSClient client = new KVSClient("localhost:8000"); //using KVS client to add the data to table
        for (String s : list) {
            String rowName = Hasher.hash(jobId);
            client.put(jobId, rowName, COLUMN_NAME, s);
        }
        return new FlameRDDImpl(jobId);
    }

    private static String getJobId() {
        jobNumber++; //incrementing the JobNumber so that the job number is unique everytime
        return String.valueOf(jobNumber);
    }
}
