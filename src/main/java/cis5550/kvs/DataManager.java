package cis5550.kvs;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

public class DataManager {

    private Map<String, Map<String, Map<String, Map<Integer, byte[]>>>> data;
    private Lock lock;

    public DataManager() {
        data = new ConcurrentHashMap<>();
    }

    public synchronized String put(String table, String row, String column, byte[] value) {
        try {
            lock.lock();
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
            versionMap.put(newVersion, value);
            return String.valueOf(newVersion);
        } finally {
            lock.unlock();
        }
    }

    private synchronized int getLatestVersion(Map<Integer, byte[]> versionMap) {
        int latestVersion = 0;
        for (int version : versionMap.keySet()) {
            if (version > latestVersion) {
                latestVersion = version;
            }
        }
        return latestVersion;
    }

    public synchronized byte[] get(String table, String row, String column, int version) {
        try {
            lock.lock();
            Map<String, Map<String, Map<Integer, byte[]>>> rowMap = data.get(table);
            if (rowMap == null) {
                return null;
            }
            Map<String, Map<Integer, byte[]>> colMap = rowMap.get(row);
            if (colMap == null) {
                return null;
            }
            Map<Integer, byte[]> versionMap = colMap.get(column);
            if (versionMap == null || versionMap.size() < version) {
                return null;
            }
            return versionMap.get(version);
        } finally {
            lock.unlock();
        }
    }

    public synchronized int getLatestVersion(String table, String row, String column) {
        try {
            lock.lock();
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
            lock.unlock();
        }
    }
}
