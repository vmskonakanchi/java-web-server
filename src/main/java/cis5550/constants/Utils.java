package cis5550.constants;

public class Utils {
    public static final String COLUMN_NAME = "value";
    public static int jobNumber = 1;
    public static int operationNumber = 1;

    public static String getOperationId() {
        return "" + operationNumber++;
    }
}