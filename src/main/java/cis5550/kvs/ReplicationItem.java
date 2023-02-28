package cis5550.kvs;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

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
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + worker + "/data/" + tableName + "/" + rowName + "/" + columnName))
                    .PUT(HttpRequest.BodyPublishers.ofByteArray(dataToReplicate))
                    .build();
            HttpResponse<String> response = client.send(request,
                    HttpResponse.BodyHandlers.ofString());
            return response.statusCode();
        } catch (Exception e) {
            e.printStackTrace();
            return 500;
        }
    }
}
