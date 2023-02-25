package cis5550.kvs;


import java.time.LocalTime;

/*
* This is the data structure for holding our entries for the worker nodes
* this contains id , ipaddress and the port number as public
* */
public class Entry implements Comparable<Entry> {

    private static final int PING_INTERVAL = 15;
    private String id;
    private String ipAddress;
    private String portNumber;
    private final String hyperLink;

    private LocalTime lastPing;

    public Entry(String id, String ipAddress, String portNumber) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.portNumber = portNumber;
        this.hyperLink = "<a href=\"http://" + ipAddress + ":" + portNumber + "\">Go</a>";
        this.lastPing = LocalTime.now();
    }

    public void updateLastPing() {
        this.lastPing = LocalTime.now();
    }

    public String getId() {
        return id;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getPortNumber() {
        return portNumber;
    }

    public String getHyperLink() {
        return hyperLink;
    }

    public LocalTime getLastPing() {
        return lastPing;
    }

    public boolean isAlive() {
        LocalTime now = LocalTime.now();
        return !now.isAfter(lastPing.plusSeconds(PING_INTERVAL));
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setPortNumber(String portNumber) {
        this.portNumber = portNumber;
    }

    @Override
    public String toString() {
        return this.ipAddress + ":" + this.portNumber;
    }

    @Override
    public int compareTo(Entry o) {
        return o.id.compareTo(this.id);
    }

    public String toText() {
        return id + "," + ipAddress + ":" + portNumber + "\n";
    }

    public String toHtml() {
        StringBuilder builder = new StringBuilder();
        builder.append("<tr><td>");
        builder.append(id);
        builder.append("</td><td>");
        builder.append(ipAddress);
        builder.append("</td><td>");
        builder.append(portNumber);
        builder.append("</td><td>");
        builder.append(hyperLink);
        builder.append("</td></tr>");
        return builder.toString();
    }

    public String getHost() {
        return ipAddress + ":" + portNumber;
    }
}
