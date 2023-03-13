package cis5550.testings;

import cis5550.tools.HTTP;
import cis5550.tools.Partitioner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Testings {
    public static void main(String[] args) throws IOException {
        Partitioner partitioner = new Partitioner();
        partitioner.addKVSWorker("http://localhost:8001", null, null);
        partitioner.addFlameWorker("http://localhost:9001");
        partitioner.addFlameWorker("http://localhost:9002");
        for (Partitioner.Partition p : partitioner.assignPartitions()) {
            System.out.println(p);
        }
    }
}
