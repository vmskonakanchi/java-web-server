package cis5550.jobs;

import cis5550.flame.FlameContext;

public class Crawler {
    public static void run(FlameContext context, String[] urls) {
        if (urls.length < 1) {
            context.output("No URLs provided");
            return;
        }
        context.output("OK");
    }
}
