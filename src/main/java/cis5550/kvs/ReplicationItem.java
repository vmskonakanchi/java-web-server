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
            URL toSendUrl = new URL("http://" + worker + "/data/put/" + tableName + "/" + rowName + "/" + columnName);
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getRowName() {
        return rowName;
    }

    public void setRowName(String rowName) {
        this.rowName = rowName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public byte[] getDataToReplicate() {
        return dataToReplicate;
    }

    public void setDataToReplicate(byte[] dataToReplicate) {
        this.dataToReplicate = dataToReplicate;
    }
}
