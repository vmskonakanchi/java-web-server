package cis5550.kvs;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class DataManager {

    private final Map<String, Table> data;

    private final String workingDirectory;

    public DataManager(String workingDirectory) {
        data = new ConcurrentHashMap<>();
        this.workingDirectory = workingDirectory;
    }

    public synchronized boolean createTable(String tableName) {
        //check for file exists in the directory
        if (data.containsKey(tableName)) {
            return false;
        }
        Path filePath = Path.of(workingDirectory, tableName + ".table");
        if (Files.notExists(filePath)) {
            try {
                Files.createFile(filePath);
                data.put(tableName, new Table(tableName, true));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public synchronized void put(String tableName, String rowName, String columnName, byte[] value) {
        Table t = data.computeIfAbsent(tableName, k -> new Table(tableName, true));
        t.addColumnToRow(rowName, columnName, value);
    }

    public synchronized byte[] get(String tableName, String rowName, String columnName) {
        Table t = data.get(tableName);
        if (t == null) {
            return null;
        }
        return t.getColumnValue(rowName, columnName);
    }

    public synchronized void save() {
        for (Table t : data.values()) {
            t.save(workingDirectory);
        }
    }

    public synchronized String getRowsFromTable(String tableName) {
        if (!data.containsKey(tableName)) {
            throw new RuntimeException("Required table not found");
        }
        StringBuilder builder = new StringBuilder();
        builder.append("<html><body>");
        builder.append("<h1> Viewing ").append(tableName).append(" Table").append("</h1>");
        builder.append("<table>");
        //get the columns for all the rows
        builder.append("<tr><th>Row</th>");
        Set<String> columns = data.get(tableName).getRows().stream().map(Row::columns).flatMap(Collection::stream).collect(Collectors.toSet());
        for (String s : columns) {
            builder.append("<th>").append(s).append("</th>");
        }
        builder.append("</tr>");
        for (Row r : data.get(tableName).getRows()) {
            builder.append("<tr><td>").append(r.key).append("</td>");
            for (String s : columns) {
                builder.append("<td>").append(r.get(s)).append("</td>");
            }
            builder.append("</tr>");
        }
        builder.append("</table></body></html>");
        return builder.toString();
    }

    public synchronized String getTablesAsHtml() {
        StringBuilder builder = new StringBuilder();
        builder.append("<html><body>");
        builder.append("<h1>Tables</h1>");
        builder.append("<table>");
        builder.append("<tr><th>Table Name</th><th>Number of Rows</th><th>Persistent</th></tr>");
        for (String t : data.keySet()) {
            builder.append("<tr><td><a href=/view/").append(t).append(">").append(t).append("</a></td><td>").append(data.get(t).getRows().size()).append("</td><td>").append("persistent").append("</td></tr>");
        }
        builder.append("</table>");
        builder.append("</body></html>");
        return builder.toString();
    }

    public synchronized int countRowsFromTable(String tableName) {
        if (!data.containsKey(tableName)) {
            throw new RuntimeException("Required table not found");
        }
        try {
            return Files.readAllLines(Path.of(workingDirectory, tableName + ".table")).size() + 1;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        }
    }


    public synchronized boolean renameTable(String tableName, String newTableName) {
        Table t = data.get(tableName);
        if (t.renameTable(workingDirectory, newTableName)) {
            data.remove(tableName);
            data.put(newTableName, t);
            return true;
        }
        return false;
    }

    public synchronized boolean loadDataFromDisk() {
        Path path = Path.of(workingDirectory);
        try {
            File f = new File(path.toUri());
            if (f.listFiles().length > 0) {
                for (File readFile : f.listFiles()) {
                    if (readFile.getName().contains("table")) {
                        String tableName = readFile.getName().split("\\.")[0];
                        Table t = new Table(tableName, false);
                        long totalRows = Files.lines(readFile.toPath()).count();
                        InputStream is = new FileInputStream(readFile);
                        for (int i = 0; i < totalRows; i++) {
                            Row r = Row.readFrom(is);
                            t.addRow(r, false);
                        }
                        data.put(t.getName(), t);
                    }
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public synchronized boolean deleteTable(String tableName) {
        if (!data.containsKey(tableName)) {
            throw new RuntimeException("Table not found");
        }
        try {
            Files.delete(Path.of(workingDirectory, tableName + ".table"));
            data.remove(tableName);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public synchronized String getTables() {
        StringBuilder builder = new StringBuilder();
        for (String table : data.keySet()) {
            builder.append(table).append("\n");
        }
        return builder.toString();
    }

    public synchronized Row getRow(String tableName, String rowName) {
        if (!data.get(tableName).contains(rowName)) {
            throw new RuntimeException("Row Not Found");
        }
        Table table = data.get(tableName);
        return table.getRow(rowName);
    }

    public synchronized boolean hasTable(String tableName) {
        return data.containsKey(tableName);
    }

    public synchronized void saveRows(String tableName, String dataToInsert) {
        Table t = data.computeIfAbsent(tableName, k -> new Table(tableName, true));
        try {
            String[] lines = dataToInsert.split("\n");
            for (String s : lines) {
                String[] split = s.split(" ");
                t.addColumnToRow(split[0], split[1], split[split.length - 1].getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized String getRowDataFromTable(String tableName) {
        return data.get(tableName).getRowsFromDisk(workingDirectory);
    }
}
