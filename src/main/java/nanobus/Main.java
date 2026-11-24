package nanobus;

import nanobus.harness.Bench;
import nanobus.harness.Chaos;
import nanobus.harness.Churn;
import nanobus.harness.Stall;

import java.nio.file.Path;

public final class Main {
    public static void main(String[] argv) throws Exception {
        if (argv.length == 0) {
            System.out.println("""
                    nano-bus: partitioned pub/sub with a replicated journal
                      witness --port 7000 --dir work/w --ttl 400
                      broker  --id A --port 7100 --peer 127.0.0.1:7200 --witness 127.0.0.1:7000 --shards 2 --metrics 9101
                      chaos   --mode safe|naive|nodedup --seconds 120 --rate 20000
                      bench   --rates 50000,100000 --gcs G1 --heap-cache 0
                      churn   --seconds 60 [--leak] [--pool off]
                      stall   --brokers 127.0.0.1:7100,127.0.0.1:7200   (demo load for the alert test)""");
            return;
        }
        Args a = new Args(argv, 1);
        switch (argv[0]) {
            case "witness" -> {
                new Witness(a.num("port", 7000), Path.of(a.str("dir", "work/w")), a.num("ttl", 400));
                Thread.currentThread().join();
            }
            case "broker" -> {
                Broker b = new Broker(BrokerConfig.parse(a));
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try { b.close(); } catch (Exception e) { e.printStackTrace(); }
                }));
                Thread.currentThread().join();
            }
            case "chaos" -> System.exit(Chaos.run(a));
            case "bench" -> System.exit(Bench.run(a));
            case "churn" -> System.exit(Churn.run(a));
            case "stall" -> System.exit(Stall.run(a));
            default -> throw new IllegalArgumentException(argv[0]);
        }
    }
}
