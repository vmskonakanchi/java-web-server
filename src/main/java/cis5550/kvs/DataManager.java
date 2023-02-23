package cis5550.kvs;

import javax.swing.text.html.HTMLDocument;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManager {

    private final Map<String, Table> data;
    private final ReentrantReadWriteLock lock;

    public DataManager() {
        data = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock();
    }

    public void put(String tableName, String rowName, String columnName, byte[] value) {
        synchronized (data) {
            Table t = data.get(tableName);
            if (t == null) {
                t = new Table(tableName);
                data.put(tableName, t);
            }
            t.addColumnToRow(rowName, columnName, value);
            t.save(Path.of("").toAbsolutePath() + "/__worker");
        }
    }

    public byte[] get(String tableName, String rowName, String columnName) {
        synchronized (data) {
            Table t = data.get(tableName);
            if (t == null) {
                return null;
            }
            return t.getColumnValue(rowName, columnName);
        }
    }

    public void save(String parentFolder) {
        for (Table t : data.values()) {
            t.save(parentFolder);
        }
    }
}
