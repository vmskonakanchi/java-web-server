package cis5550.flameimpl;

import cis5550.constants.Utils;
import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;

import java.nio.charset.StandardCharsets;
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
        String jobId = String.valueOf(Utils.jobNumber); //generating a new job id for each job
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
    public static FlameRDD invokeOperation(String argument, byte[] lambda) {
        String operationId = "output_" + UUID.randomUUID() + "_" + System.currentTimeMillis();
        Partitioner partitioner = new Partitioner();
        try {
            HTTP.Response response = HTTP.doRequest("GET", "http://localhost:8000/workers", null);
            String[] workers = new String(response.body(), StandardCharsets.UTF_8).split("\n");
            for (int i = 1; i < workers.length; i++) {
                String workerId = workers[i].split(",")[0]; //getting the workerId
                String workerAddress = workers[i].split(",")[1]; //getting the workerAddress
                partitioner.addKVSWorker(workerAddress, null, null);
            }
            HTTP.Response flameResponse = HTTP.doRequest("GET", "http://localhost:9000/workers", null);
            String[] flameWorkers = new String(flameResponse.body(), StandardCharsets.UTF_8).split("\n");
            for (int i = 1; i < flameWorkers.length; i++) {
                String flameWorkerAddress = flameWorkers[i].split(",")[1];
                partitioner.addFlameWorker(flameWorkerAddress);
            }
            Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
            Thread[] requestThreads = new Thread[partitions.size()];
            for (int i = 0; i < partitions.size(); i++) {
                Partitioner.Partition p = partitions.get(i);
                Thread t = new Thread(() -> {
                    try {
                        String urlString = p.assignedFlameWorker + argument + "?inputTable=" + "saxeq" + "&outputTable=" + operationId + "&startKey=" + p.fromKey + "&endKey=" + p.toKeyExclusive;
                        HTTP.Response res = HTTP.doRequest("POST", urlString, lambda);
                        if (res.statusCode() != 200) {
                            throw new RuntimeException("The operation failed");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                requestThreads[i] = t;
                t.start(); //starting the thread
            }
            for (Thread requestThread : requestThreads) {
                requestThread.join(); // waiting for the all responses to arrive
            }
            return new FlameRDDImpl(operationId);
        } catch (Exception ignored) {
            return null;
        }
    }
}
