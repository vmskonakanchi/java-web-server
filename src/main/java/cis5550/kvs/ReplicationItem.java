package cis5550.kvs;

import java.net.HttpURLConnection;
import java.net.URL;

/*
 * A class to represent the replication item to be processed
 * by the replication thread in the worker
 * */
public class ReplicationItem {
    private String tableName;
    private String rowName;
    private String columnName;
    private byte[] dataToReplicate;

    public ReplicationItem(String tableName, String rowName, String columnName, byte[] dataToReplicate) {
        this.tableName = tableName;
        this.rowName = rowName;
        this.columnName = columnName;
        this.dataToReplicate = dataToReplicate;
    }

    public int replicate(String worker) {
        try {
            System.out.println("Replicating to " + worker);
            URL toSendUrl = new URL("http://" + worker + "/data/" + tableName + "/" + rowName + "/" + columnName);
            HttpURLConnection connection = (HttpURLConnection) toSendUrl.openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.getOutputStream().write(dataToReplicate);
            connection.getOutputStream().flush();
            connection.getOutputStream().close();
            connection.connect();
            return connection.getResponseCode();
        } catch (Exception e) {
            e.printStackTrace();
            return 500;
        }
    }
}