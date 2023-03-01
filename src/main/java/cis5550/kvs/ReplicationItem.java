package cis5550.kvs;

import java.net.HttpURLConnection;
import java.net.URL;

/*
 * A class to represent the replication item to be processed in another thread
 * */
public class ReplicationItem extends Thread {
    private final String tableName;
    private final String rowName;
    private final String columnName;
    private final byte[] dataToReplicate;

    private final String[] workerList;

    public ReplicationItem(String tableName, String rowName, String columnName, byte[] dataToReplicate, String[] workerList) {
        this.tableName = tableName;
        this.rowName = rowName;
        this.columnName = columnName;
        this.dataToReplicate = dataToReplicate;
        this.workerList = workerList;
    }

    @Override
    public void run() {
        for (String worker : workerList) {
            if (replicate(worker) == 200) {
                System.out.println("Replication successful");
                break;
            } else {
                System.out.println("Replication failed");
            }
        }
    }

    private int replicate(String worker) {
        try {
            URL url = new URL("http://" + worker + "/data/" + tableName + "/" + rowName + "/" + columnName);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("PUT");
            con.setDoOutput(true);
            con.getOutputStream().write(dataToReplicate);
            con.getOutputStream().flush();
            con.getOutputStream().close();
            System.out.println("Replicated to " + worker + " with status code " + con.getResponseCode());
            return con.getResponseCode();
        } catch (Exception e) {
            e.printStackTrace();
            return 500;
        }
    }
}
