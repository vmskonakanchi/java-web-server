package cis5550.test;

import cis5550.flame.FlameSubmit;

import java.util.Random;

public class FilterTest {
    public static void main(String[] args) throws Exception {
        Random r = new Random();
        int num = 20 + r.nextInt(10);
        String arg[] = new String[1 + num];
        String provided = "";
        arg[0] = "filter";
        int sum = 0;
        for (int i = 0; i < num; i++) {
            int n = r.nextInt(10);
            arg[1 + i] = "" + n;
            provided = provided + ((i > 0) ? "," : "") + arg[1 + i];
            sum += n;
        }
        String response = FlameSubmit.submit("localhost:9000", "tests/hw7testjob.jar", "cis5550.test.FilterTest", arg);
        System.out.println("Response: " + response);
    }
}
