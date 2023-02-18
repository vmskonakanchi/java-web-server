package cis5550.kvs;

import java.util.concurrent.ConcurrentHashMap;

//For usage in assignment-5
public class DataManager {

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, Row>> dataStore = new ConcurrentHashMap<>();

    public void put(String table, String rowName, String column, String dataToInsert) {
        //creating a thread for efficiency
        if (dataStore.contains(table)) {
            ConcurrentHashMap<String, Row> tempStore = dataStore.get(table);
            if (tempStore.contains(rowName)) {
                // insert the data into row
                Row row = tempStore.get(rowName);
                row.put(column, dataToInsert);
            } else {
                //create a new row and insert data
                Row rowToAdd = new Row(rowName);
                rowToAdd.put(column, dataToInsert);
                tempStore.put(rowName, rowToAdd);
                dataStore.replace(table, tempStore);
            }
        } else {
            //create a new table
            ConcurrentHashMap<String, Row> tempStore = new ConcurrentHashMap<>();
            Row rowToAdd = new Row(rowName); // creating a new row
            rowToAdd.put(column, dataToInsert); // adding column and the value in the column to the row
            tempStore.put(rowName, rowToAdd); // adding row to the table
            dataStore.put(table, tempStore); // adding table to the map
        }
    }

    public String get(String table, String row, String column) {
        return dataStore.get(table).get(row).get(column) != null ? dataStore.get(table).get(row).get(column) : null;
    }

}
