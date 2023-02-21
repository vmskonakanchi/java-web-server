package cis5550.kvs;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataManager {
    private Map<String, Map<String, Map<String, String>>> data;

    public DataManager() {
        this.data = new ConcurrentHashMap<>();
    }

    public void put(String tableName, String rowName, String columnName, String value) {
        Map<String, Map<String, String>> table = data.getOrDefault(tableName, new ConcurrentHashMap<>());
        Map<String, String> row = table.getOrDefault(rowName, new ConcurrentHashMap<>());
        row.put(columnName, value);
        table.put(rowName, row);
        data.put(tableName, table);
    }

    public String get(String tableName, String rowName, String columnName) {
        Map<String, Map<String, String>> table = data.get(tableName);
        if (table != null) {
            Map<String, String> row = table.get(rowName);
            if (row != null) {
                return row.get(columnName);
            }
        }
        return null;
    }
}
