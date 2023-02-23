package cis5550.kvs;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedList;

public class Table implements Comparable<Table> {

    private final String name;
    private final LinkedList<Row> rows;

    public Table(String name) {
        this.name = name + ".table";
        rows = new LinkedList<>();
    }

    public String getName() {
        return name;
    }

    public Row addRow(String rowName) {
        Row r = new Row(rowName);
        rows.addLast(r);
        return r;
    }

    public void addColumnToRow(String rowName, String columnName, byte[] value) {
        Row r = null;
        if (!rows.contains(rowName)) {
            r = addRow(rowName);
        } else {
            r = rows.get(rows.indexOf(rowName));
        }
        if (r != null) {
            r.put(columnName, value);
        } else {
            throw new RuntimeException("Row cannot be found");
        }
    }

    public void save(String parentFolder) {
        //open a file and write the rows to the file
        Path filePath = Path.of(parentFolder, name);
        Iterator<Row> rowIterator = rows.iterator();
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

    public byte[] getColumnValue(String rowName, String columnName) {
        Row r = rows.get(rows.indexOf(rowName));
        if (r != null) {
            return r.getBytes(columnName);
        }
        return null;
    }
}
