package cis5550.kvs;

import javax.swing.text.html.HTMLDocument;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataManager {

    private final Map<String, Map<String, Map<String, Map<Integer, byte[]>>>> data;
    private ReentrantReadWriteLock lock;

    public DataManager() {
        data = new ConcurrentHashMap<>();
        lock = new ReentrantReadWriteLock();
    }

    public String put(String table, String row, String column, byte[] value) {
        lock.writeLock().lock();
        try {
            Map<String, Map<String, Map<Integer, byte[]>>> rowMap = data.get(table);
            if (rowMap == null) {
                rowMap = new ConcurrentHashMap<>();
                data.put(table, rowMap);
            }
            Map<String, Map<Integer, byte[]>> colMap = rowMap.get(row);
            if (colMap == null) {
                colMap = new ConcurrentHashMap<>();
                rowMap.put(row, colMap);
            }
            Map<Integer, byte[]> versionMap = colMap.get(column);
            if (versionMap == null) {
                versionMap = new ConcurrentHashMap<>();
                colMap.put(column, versionMap);
            }
            int latestVersion = getLatestVersion(versionMap);
            int newVersion = latestVersion + 1;
            versionMap.put(newVersion + 1, value);
            return String.valueOf(newVersion);

        } finally {
            lock.writeLock().unlock();
        }
    }

    private int getLatestVersion(Map<Integer, byte[]> versionMap) {
        lock.readLock().lock();
        try {
            return versionMap.keySet().stream().max(Integer::compareTo).orElse(0);
        } finally {
            lock.readLock().unlock();
        }
    }

    public byte[] get(String table, String row, String column, int version) {
        lock.readLock().lock();
        try {
            Map<Integer, byte[]> versionMap =
                    data.computeIfAbsent(table, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(row, k -> new ConcurrentHashMap<>())
                            .computeIfAbsent(column, k -> new ConcurrentHashMap<>());
            return versionMap.get(version);
        } finally {
            lock.readLock().unlock();
        }
    }

    public int getLatestVersion(String table, String row, String column) {
        lock.readLock().lock();
        try {
            Map<String, Map<String, Map<Integer, byte[]>>> rowMap = data.get(table);
            if (rowMap == null) {
                return 0;
            }
            Map<String, Map<Integer, byte[]>> colMap = rowMap.get(row);
            if (colMap == null) {
                return 0;
            }
            Map<Integer, byte[]> versionMap = colMap.get(column);
            if (versionMap == null || versionMap.isEmpty()) {
                return 0;
            }
            return getLatestVersion(versionMap);
        } finally {
            lock.readLock().unlock();
        }
    }
}
