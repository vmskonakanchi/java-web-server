package cis5550.kvs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManager {
    private Map<String, Map<String, Map<String, Map<Integer, String>>>> data;

    public DataManager() {
        this.data = new ConcurrentHashMap<>();
    }

    public String put(String tableName, String rowName, String columnName, String value) {
        Map<String, Map<String, Map<Integer, String>>> table = data.getOrDefault(tableName, new ConcurrentHashMap<>());
        Map<String, Map<Integer, String>> row = table.getOrDefault(rowName, new ConcurrentHashMap<>());
        Map<Integer, String> column = row.getOrDefault(columnName, new ConcurrentHashMap<>());
        int version = column.keySet().stream().max(Integer::compare).orElse(0) + 1;
        column.put(version, value);
        row.put(columnName, column);
        table.put(rowName, row);
        data.put(tableName, table);
        return String.valueOf(version);
    }

    public String get(String tableName, String rowName, String columnName, int version) {
        Map<String, Map<String, Map<Integer, String>>> table = data.get(tableName);
        if (table != null) {
            Map<String, Map<Integer, String>> row = table.get(rowName);
            if (row != null) {
                Map<Integer, String> columnVersions = row.get(columnName);
                if (columnVersions != null && columnVersions.containsKey(version)) {
                    return columnVersions.get(version);
                }
            }
        }
        return null;
    }

    public int getVersions(String tableName, String rowName, String columnName) {
        Map<String, Map<String, Map<Integer, String>>> table = data.get(tableName);
        if (table != null) {
            Map<String, Map<Integer, String>> row = table.get(rowName);
            if (row != null) {
                Map<Integer, String> columnVersions = row.get(columnName);
                return columnVersions.keySet().stream().max(Integer::compare).orElse(0);
            }
        }
        return 0;
    }
}
