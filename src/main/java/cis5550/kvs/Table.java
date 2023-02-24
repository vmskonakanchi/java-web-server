package cis5550.kvs;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Table implements Comparable<Table> {

    private final String name;
    private final Map<String, Row> rows;

    public Table(String name) {
        this.name = name + ".table";
        rows = new ConcurrentHashMap<>();
    }

    public String getName() {
        return name;
    }

    public synchronized Row addRow(String rowName) {
        Row r = new Row(rowName);
        rows.put(rowName, r);
        return r;
    }

    public synchronized void addColumnToRow(String rowName, String columnName, byte[] value) {
        Row r = rows.computeIfAbsent(rowName, k -> new Row(rowName));
        r.put(columnName, value);
        //save a single row to the folder
        saveRowToDisk(r);
    }

    private synchronized void saveRowToDisk(Row r) {
        try {
            String filePath = Path.of("").toAbsolutePath().toString();
            Path actualPath = Path.of(filePath, "__worker", name);
            RandomAccessFile accessFile = new RandomAccessFile(actualPath.toFile(), "rw");
            accessFile.seek(0);
            accessFile.write(r.toByteArray());
            accessFile.writeBytes("\n");
            accessFile.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
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

    @Override
    public int compareTo(Table o) {
        return this.name.compareTo(o.name);
    }

    public synchronized byte[] getColumnValue(String rowName, String columnName) {
        Row r = rows.get(rowName);
        if (r != null) {
            return r.getBytes(columnName);
        }
        return null;
    }
}
