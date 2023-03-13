package cis5550.flameimpl;

import cis5550.constants.Utils;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;

import java.util.*;

public class FlameContextImpl implements FlameContext {
    private final List<String> jobs = new LinkedList<>();
    private final String jarName;

    public FlameContextImpl(String name) {
        jarName = name;
    }

    @Override
    public KVSClient getKVS() {
        return new KVSClient("localhost:8000");
    }

    @Override
    public void output(String s) {
        jobs.add(s); //adding job to the jobs list - which we will use in the feature
    }

    public String getJobString() {
        StringBuilder sb = new StringBuilder();
        for (String s : jobs) {
            sb.append(s);
        }
        return sb.toString();
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String jobId = getJobId(); //generating a new job id for each job
        KVSClient client = getKVS(); //using KVS client to add the data to table
        for (int i = 0; i < list.size(); i++) {
            String rowName = Hasher.hash(i + jobId);
            client.put(jobId, rowName, Utils.COLUMN_NAME, list.get(i));
        }
        return new FlameRDDImpl(jobId);
    }

    /**
     * This method is used to invoke the operation on the lambda function
     *
     * @param argument - the argument to be passed to the lambda function
     * @param lambda   - the lambda function
     */
    public void invokeOperation(String argument, byte[] lambda) {
        String operationId = Utils.getOperationId(); //generating a new operation id for each operation
        Partitioner partitioner = new Partitioner();
//        partitioner.addKVSWorker();
    }

    /**
     * This method is used to generate a unique job id for each job
     *
     * @return String
     */
    private static String getJobId() {
        Utils.jobNumber++; //incrementing the JobNumber so that the job number is unique everytime
        return String.valueOf(Utils.jobNumber);
    }
}
