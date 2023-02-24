package cis5550.kvs;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Table implements Comparable<Table> {

    private String name;
    private final Map<String, Row> rows;
    public final Map<String, Long> offsetToRow;

    public Table(String name, boolean needToWriteTable) {
        this.name = needToWriteTable ? name + ".table" : name;
        rows = new ConcurrentHashMap<>();
        offsetToRow = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public synchronized List<Row> getRows() {
        return new ArrayList<>(rows.values());
    }

    public synchronized Row addRow(String rowName) {
        return rows.put(rowName, new Row(rowName));
    }

    public synchronized Row addRow(Row row) {
        return rows.put(row.key, row);
    }

    public synchronized void addColumnToRow(String rowName, String columnName, byte[] value) {
        Row r = rows.computeIfAbsent(rowName, k -> new Row(rowName));
        r.put(columnName, value);
        //save a single row to the disk
        saveRowToDisk(r);
    }

    private synchronized void saveRowToDisk(Row r) {
        try {
            String filePath = Path.of("").toAbsolutePath().toString();
            Path actualPath = Path.of(filePath, "__worker", name);
            RandomAccessFile accessFile = new RandomAccessFile(actualPath.toFile(), "rws");
            if (accessFile.length() == 0) {
                accessFile.seek(0);
            } else {
                accessFile.seek(accessFile.length());
            }
            accessFile.write(r.toByteArray());
            accessFile.writeBytes("\n");
            offsetToRow.computeIfAbsent(r.key, k -> {
                try {
                    return accessFile.length();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            accessFile.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public synchronized String getRowsFromDisk(String parentPath) {
        try {
            StringBuilder builder = new StringBuilder();
            File readFile = new File(parentPath + "/" + name);
            long totalRows = Files.lines(readFile.toPath()).count();
            InputStream is = new FileInputStream(readFile);
            for (int i = 0; i < totalRows; i++) {
                Row r = Row.readFrom(is);
                builder.append(new String(r.toByteArray(), StandardCharsets.UTF_8))
                        .append("\n");
            }
            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }


    public synchronized void save(String parentFolder) {
        //open a file and write the rows to the file
        Path filePath = Path.of(parentFolder, name);
        Iterator<Row> rowIterator = rows.values().iterator();
        try {
            while (rowIterator.hasNext()) {
                Row r = rowIterator.next();
                Files.write(filePath, r.toByteArray());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public synchronized byte[] getColumnValue(String rowName, String columnName) {
        Row r = rows.get(rowName);
        if (r != null) {
            return r.getBytes(columnName);
        }
        return null;
    }

    public boolean renameTable(String newTableName) {
        this.name = newTableName;
        try {
            File old = new File(name);
            File newFile = new File(newTableName);
            if (newFile.exists()) {
                throw new RuntimeException("Table already exists");
            }
            return old.renameTo(newFile);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    public synchronized Row getRow(String rowName) {
        return rows.get(rowName);
    }

    @Override
    public synchronized int compareTo(Table o) {
        return this.name.compareTo(o.name);
    }

    public synchronized boolean contains(String rowName) {
        return rows.containsKey(rowName);
    }
}
