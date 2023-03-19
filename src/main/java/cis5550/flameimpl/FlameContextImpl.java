package cis5550.flameimpl;

import cis5550.constants.Utils;
import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static cis5550.generic.Master.getWorkers;

public class FlameContextImpl implements FlameContext {
    private final List<String> jobs = new LinkedList<>();
    private final String jarName;

    private final KVSClient kvs;

    private static KVSClient staticKvs;

    private static Vector<String> workers;

    public FlameContextImpl(String name, KVSClient client, Vector<String> workers) {
        jarName = name;
        kvs = client;
        staticKvs = client;
        FlameContextImpl.workers = workers;
    }

    @Override
    public KVSClient getKVS() {
        return kvs;
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
        for (int i = 0; i < list.size(); i++) {
            String rowName = Hasher.hash(Hasher.hash(i + jobId));
            getKVS().put(jobId, rowName, Utils.COLUMN_NAME, list.get(i));
        }
        Utils.jobNumber++;
        return new FlameRDDImpl(jobId, kvs);
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        byte[] dataToSend = Serializer.objectToByteArray(lambda);
        return FlameContextImpl.invokeOperation("/rdd/fromTable", dataToSend, FlameRDD.class, tableName);
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {

    }

    public static <T> T invokeOperation(String argument, byte[] lambda, Class<T> type, String tableName) throws IOException, InterruptedException {
        String newTableName = "output_" + UUID.randomUUID() + "_" + System.currentTimeMillis(); //creating a new tableName
        Partitioner partitioner = new Partitioner(); //creating a partitioner
        try {
            if (staticKvs.numWorkers() == 1) {
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(0), staticKvs.getWorkerID(0), null);
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(0), null, staticKvs.getWorkerID(0));
            }
            if (staticKvs.numWorkers() > 1) {
                for (int i = 0; i < staticKvs.numWorkers() - 1; i++) {
                    String workerId = staticKvs.getWorkerID(i);
                    String workerAddress = staticKvs.getWorkerAddress(i);
                    partitioner.addKVSWorker(workerAddress, workerId, staticKvs.getWorkerID(i + 1));
                }
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(staticKvs.numWorkers()), null, null);
            }
            for (String worker : getWorkers()) { //adding flame workers to the partitioner
                partitioner.addFlameWorker(worker);
            }
            Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
            Thread[] requestThreads = new Thread[partitions.size()];
            for (int i = 0; i < partitions.size(); i++) {
                Partitioner.Partition p = partitions.get(i);
                requestThreads[i] = new Thread(() -> {
                    try {
                        String urlString = p.assignedFlameWorker + argument + "?inputTable=" + tableName + "&outputTable=" + newTableName + "&startKey=" + p.fromKey + "&endKey=" + p.toKeyExclusive + "&master=" + staticKvs.getMaster();
                        System.out.println("Sending request to " + urlString);
                        HTTP.Response res = HTTP.doRequest("POST", urlString, lambda);
                        if (res.statusCode() != 200) {
                            throw new RuntimeException("The operation failed");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                requestThreads[i].start(); //starting the thread to request
            }
            for (Thread requestThread : requestThreads) {
                requestThread.join(); // waiting for the all responses to arrive
            }
            //checking types for classes
            if (type == null) {
                throw new IllegalArgumentException("Type cannot be null");
            }

            Object result;

            if (type == FlameRDD.class) {
                result = new FlameRDDImpl(newTableName, staticKvs);
            } else if (type == FlamePairRDD.class) {
                result = new FlamePairRDDImpl(newTableName, staticKvs);
            } else {
                throw new IllegalArgumentException("Type not supported: " + type.getName());
            }
            return type.cast(result);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }


    public static <T> T invokeOperation(String argument, byte[] lambda, Class<T> type, String tableName, String arg) throws IOException, InterruptedException {
        String newTableName = "output_" + UUID.randomUUID() + "_" + System.currentTimeMillis(); //creating a new tableName
        Partitioner partitioner = new Partitioner(); //creating a partitioner
        try {
            if (staticKvs.numWorkers() == 1) {
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(0), staticKvs.getWorkerID(0), null);
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(0), null, staticKvs.getWorkerID(0));
            }
            if (staticKvs.numWorkers() > 1) {
                for (int i = 0; i < staticKvs.numWorkers() - 1; i++) {
                    String workerId = staticKvs.getWorkerID(i);
                    String workerAddress = staticKvs.getWorkerAddress(i);
                    partitioner.addKVSWorker(workerAddress, workerId, staticKvs.getWorkerID(i + 1));
                }
                partitioner.addKVSWorker(staticKvs.getWorkerAddress(staticKvs.numWorkers()), null, null);
            }
            for (String worker : getWorkers()) { //adding flame workers to the partitioner
                partitioner.addFlameWorker(worker);
            }
            Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();
            Vector<String> results = new Vector<>();
            Thread[] requestThreads = new Thread[partitions.size()];
            Object result;
            for (int i = 0; i < partitions.size(); i++) {
                Partitioner.Partition p = partitions.get(i);
                requestThreads[i] = new Thread(() -> {
                    try {
                        String urlString = p.assignedFlameWorker + argument + "?inputTable=" + tableName + "&outputTable=" + newTableName + "&startKey=" + p.fromKey + "&endKey=" + p.toKeyExclusive + "&master=" + staticKvs.getMaster() + "&arg=" + arg;
                        HTTP.Response res = HTTP.doRequest("POST", urlString, lambda);
                        if (type == Vector.class) {
                            results.add(new String(res.body(), StandardCharsets.UTF_8));
                        }
                        if (res.statusCode() != 200) {
                            throw new RuntimeException("The operation failed");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                requestThreads[i].start(); //starting the thread to request
            }
            for (Thread requestThread : requestThreads) {
                requestThread.join(); // waiting for the all responses to arrive
            }
            //checking types for classes
            if (type == null) {
                throw new IllegalArgumentException("Type cannot be null");
            }

            if (type == FlameRDD.class) {
                result = new FlameRDDImpl(newTableName, staticKvs);
            } else if (type == FlamePairRDD.class) {
                result = new FlamePairRDDImpl(newTableName, staticKvs);
            } else if (type == Vector.class) {
                result = results;
            } else {
                throw new IllegalArgumentException("Type not supported: " + type.getName());
            }
            return type.cast(result);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }
}
