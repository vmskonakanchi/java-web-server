package cis5550.kvs;


import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {

    private final Map<String, Table> data;

    public DataManager() {
        data = new ConcurrentHashMap<>();
    }

    public synchronized boolean createTable(String tableName, String directory) {
        //check for file exists in the directory
        if (data.containsKey(tableName)) {
            return false;
        }
        Path filePath = Path.of(directory, tableName + ".table");
        if (Files.notExists(filePath)) {
            try {
                Files.createFile(filePath);
                data.put(tableName, new Table(tableName));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public synchronized void put(String tableName, String rowName, String columnName, byte[] value) {
        Table t = data.get(tableName);
        if (t == null) {
            t = new Table(tableName);
            data.put(tableName, t);
        }
        t.addColumnToRow(rowName, columnName, value);
    }

    public synchronized byte[] get(String tableName, String rowName, String columnName) {
        Table t = data.get(tableName);
        if (t == null) {
            return null;
        }
        return t.getColumnValue(rowName, columnName);
    }

    public synchronized void save(String parentFolder) {
        for (Table t : data.values()) {
            t.save(parentFolder);
        }
    }
}
