package cis5550.kvs;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class PersistentDataManager {

    private final Set<Table> tables;

    public PersistentDataManager() {
        tables = new HashSet<>();
    }

    public boolean createTable(String tableName, String directory) {
        //check for file exists in the directory
        if (tables.contains(new Table(tableName))) {
            return false;
        }
        Path filePath = Path.of(directory, tableName + ".table");
        if (Files.notExists(filePath)) {
            try {
                Files.createFile(filePath);
                tables.add(new Table(tableName));
                return true;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    public String getTablesAsHtml() {
        Iterator<Table> tableIterator = tables.iterator();
        StringBuilder builder = new StringBuilder();
        while (tableIterator.hasNext()) {
            builder.append(tableIterator.next().getName())
                    .append("\n");
        }
        return builder.toString();
    }
}
