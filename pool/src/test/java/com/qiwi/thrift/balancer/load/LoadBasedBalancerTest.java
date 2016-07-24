package com.qiwi.thrift.balancer.load;

import com.qiwi.thrift.utils.ThriftClientAddress;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.assertTrue;


/**
 * При различных глюках пропускная способность датацентров и нод может быстро изменятся.
 * Пул должен подобрать оптимальную для всех дата-центров нагрузку.
 */
@SuppressWarnings("MagicNumber")
public class LoadBasedBalancerTest {
    public static final ThriftBalancerConfig CONFIG = new ThriftBalancerConfig
            .Builder(ThriftBalancerConfig.MethodOfFailureHandling.TRY_CONTINUE)
            .setServers(Arrays.asList(new ThriftClientAddress("test", 9090, Collections.emptyMap())))
            .build();
    private Random random;

    @DataProvider
    public Object[][] convergenceTimeDataProvider() {
        ClientPoolEmulator[][] nearCrashWithPreferred = {{new ClientPoolEmulator(
                new DcEmulator(2000),
                108
        ), new ClientPoolEmulator(new DcEmulator(100), 40)}};
        nearCrashWithPreferred[0][1].getAccumulator().setPreferred(true);

        DcEmulator dc1 = new DcEmulator(2700);
        DcEmulator dc2 = new DcEmulator(1600);
        return new Object[][] {
                {"Three client",              1.2, new ClientPoolEmulator[][] {
                        {new ClientPoolEmulator(dc1, 160), new ClientPoolEmulator(dc2,  90)},
                        {new ClientPoolEmulator(dc1, 160), new ClientPoolEmulator(dc2,  90)},
                        {new ClientPoolEmulator(dc1,  90), new ClientPoolEmulator(dc2, 160)},
                }},
                {"Dc 2 fail",               1.80, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(1000),  25), new ClientPoolEmulator(new DcEmulator(  40),  25)}}},
                {"Dc 1 fail",               1.80, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(  40),  25), new ClientPoolEmulator(new DcEmulator(1000),  25)}}},
                {"Dc 2 partial-fail",       1.20, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(1000),  25), new ClientPoolEmulator(new DcEmulator( 200),  25)}}},
                {"Dc 1 partial-fail",       1.20, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator( 200),  25), new ClientPoolEmulator(new DcEmulator(1000),  25)}}},
                {"Slow requests",           1.15, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(1000), 150), new ClientPoolEmulator(new DcEmulator(1000), 110)}}},
                {"Very slow requests ",     1.15, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(2000), 190), new ClientPoolEmulator(new DcEmulator(2000), 160)}}},
                {"slow requests, dc1 fail", 1.15, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator( 500), 190), new ClientPoolEmulator(new DcEmulator(1000), 160)}}},
                {"Long ping to dc1",        1.15, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(4000), 150), new ClientPoolEmulator(new DcEmulator(4000),  20)}}},
                {"Near crash",              1.40, new ClientPoolEmulator[][] {{new ClientPoolEmulator(new DcEmulator(2000), 108), new ClientPoolEmulator(new DcEmulator( 100),   40)}}},
                {"Near crash",              1.40, nearCrashWithPreferred},
        };
    }

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        random = new Random(90678437L);
    }



    @Test(groups = "unit", dataProvider = "convergenceTimeDataProvider")
    public void convergenceTest(String name, double maxLoadDiff, ClientPoolEmulator[][] calcs) throws Exception {
        Modeler modeler = test(
                "DC_LOAD_TO_WEIGHT_FUNCTION " + name,
                nodes -> new ClientEmulator(nodes),
                maxLoadDiff,
                calcs
        );
        assertTrue(modeler.convergenceTick > 0, "No convergence");
        assertTrue(modeler.getMaxLoadDiff() < maxLoadDiff, "To high load diff " + modeler.getMaxLoadDiff());
        assertTrue(modeler.getMaxLoad() < 30, "To high max load " + modeler.getMaxLoad());
    }

    @Test(groups = "unit")
    public void preferredDcTest() throws Exception {
        DcEmulator dc1 = new DcEmulator(2000);
        DcEmulator dc2 = new DcEmulator(2000);
        DcEmulator dc3 = new DcEmulator(2000);

        int requestLatencyMs = 40;
        ClientPoolEmulator[][] calcs = {{
                    new ClientPoolEmulator(dc1, requestLatencyMs),
                    new ClientPoolEmulator(dc2, requestLatencyMs + 20),
                    new ClientPoolEmulator(dc3, requestLatencyMs + 40),
                },{
                    new ClientPoolEmulator(dc2, requestLatencyMs),
                    new ClientPoolEmulator(dc1, requestLatencyMs + 20),
                    new ClientPoolEmulator(dc3, requestLatencyMs + 40),
                }};
        calcs[0] [0].getAccumulator().setPreferred(true);
        calcs[1] [1].getAccumulator().setPreferred(true);
        Modeler modeler = test(
                "preferredDcTest",
                nodes -> new ClientEmulator(nodes),
                100.0,
                calcs
        );

        assertTrue(modeler.convergenceTick > 0, "No convergence");
        assertTrue(calcs[0][0].weight1 > 0.90, "preferred dc not used " + calcs[0][0].weight1);
        assertTrue(calcs[0][0].avgDcLoad < 0.70, "To high max load " + calcs[0][0].avgDcLoad);
        assertTrue(calcs[1][1].weight1 > 0.80, "preferred dc not used " + calcs[1][1].weight1);
        assertTrue(calcs[1][1].avgDcLoad < 0.70, "To high max load " + calcs[1][1].avgDcLoad);
    }

    public Modeler test(String expression, Function<List<ClientPoolEmulator>, WeightModel> modelFactory, double maxLoadDiff, ClientPoolEmulator[][] calcs){
        for (ClientPoolEmulator[] calc : calcs) {
            for (ClientPoolEmulator pool : calc) {
                pool.reset();
            }
        }
        System.out.println(expression);
        Modeler modeler = new Modeler(
                modelFactory,
                maxLoadDiff,
                calcs
        );
        modeler.run(400);
        if (modeler.convergenceTick < 0) {
            System.out.println("Not convergence");
        }
        modeler.printConvergenceResult();
        modeler.print();
        return modeler;
     }


    public interface WeightModel{
        public void run();
        Stream<ClientPoolEmulator> getPoolStream();
    }

    public class ClientEmulator implements WeightModel {
        private final List<ClientPoolEmulator> nodes;
        private final LoadBasedBalancer<ClientPoolEmulator> poolEmulatorLoadBasedBalancer;

        public ClientEmulator(
                List<ClientPoolEmulator> nodes
        ) {
            this.nodes = nodes;
            this.poolEmulatorLoadBasedBalancer = new LoadBasedBalancer<>(
                    nodes,
                    dc -> dc.getAccumulator(),
                    dc -> dc.getDcLoad(),
                    CONFIG.getDcLoadFilterFactor(),
                    CONFIG.getDcLoadPredictFactor(),
                    CONFIG.getDcWeightFilterFactor(),
                    () -> random.nextDouble()
            );
        }

        @Override
        public void run() {
            nodes.forEach(node -> node.run());
            poolEmulatorLoadBasedBalancer.reBalance();
            double sum = nodes.stream()
                    .mapToDouble(node -> node.getAccumulator().getWeight())
                    .sum();
            nodes.forEach(node -> node.setWeight(node.getAccumulator().getWeight() / sum));
        }

        @Override
        public Stream<ClientPoolEmulator> getPoolStream() {
            return nodes.stream();
        }
    }

    private static class Modeler {
        private final List<WeightModel> models;
        private final Set<DcEmulator> dcEmulatorSet;
        private final double maxLoadDiff;

        long currentTick;
        long convergenceTick = -1;

        public Modeler(
                Function<List<ClientPoolEmulator>, WeightModel> modelFactory,
                double maxLoadDiff,
                ClientPoolEmulator[][] clientList
        ) {
            this.maxLoadDiff = maxLoadDiff;
            models = new ArrayList<>();
            dcEmulatorSet = new HashSet<>();
            for (ClientPoolEmulator[] emulators : clientList) {
                models.add(modelFactory.apply(Arrays.asList(emulators)));
                for (ClientPoolEmulator emulator : emulators) {
                    dcEmulatorSet.add(emulator.dcEmulator);
                }
            }
            for (ClientPoolEmulator[] emulators : clientList) {
                emulators[0].avgDcLoad = 0.8;
                emulators[0].weight1 = 1;
            }
        }

        public void run(long tick){
            for (; currentTick < tick; currentTick++) {
                for (DcEmulator emulator : dcEmulatorSet) {
                    emulator.run();
                }
                for (ClientPoolEmulator emulator : getPoolStream().collect(Collectors.toList())) {
                    System.out.printf("%4.3f\t", emulator.weight1);
                }
                System.out.println();
                for (WeightModel model : models) {
                    model.run();
                }
                testСonvergence();
            }
        }

        public Stream<ClientPoolEmulator> getPoolStream(){
            return models.stream().flatMap(WeightModel::getPoolStream);
        }

        public void testСonvergence(){
            if (convergenceTick > 0) {
                return;
            }
            double eps = getPoolStream()
                    .mapToDouble(dc -> dc.weight1)
                    .average()
                    .getAsDouble() * Math.exp(-1);

            boolean stabilized = getPoolStream()
                    .noneMatch(dc -> dc.isWeightChanged(eps));
            if (stabilized && getMaxLoadDiff() < maxLoadDiff) {
                convergenceTick = currentTick;
                printConvergenceResult();
                //print();
            }
        }


        public double getMaxLoadDiff(){
            List<Stats> loadStats = getLoadStats();
            double max = 0;
            double min = 1000;
            for (Stats stat : loadStats) {
                max = Math.max(max, stat.poolLoadDiff);
                min = Math.min(min, stat.poolLoadDiff);
            }

            return 1 / min > max ? 1 / min : max;
        }

        public void printConvergenceResult(){
            System.out.printf(
                    (currentTick == convergenceTick? "Convergence": "Result     ")
                    + " on tick %7d. max load %4.3f, load div %4.3f%n",
                    currentTick,
                    getMaxLoad(),
                    getMaxLoadDiff()
            );
        }

        private double getMaxLoad() {
            return getPoolStream().mapToDouble(ClientPoolEmulator::getAvgDcLoad).max().orElse(1000);
        }

        private static class Stats {
            final int clientId;
            final ClientPoolEmulator pool;
            final long maxDcRequest;
            final long maxSend;
            final long maxRequests;
            final double dcLoad;
            final long rps;
            final double poolLoad;
            double poolLoadDiff;

            private Stats(
                    int clientId,
                    ClientPoolEmulator pool,
                    long maxDcRequest,
                    long maxSend,
                    long maxRequests,
                    double dcLoad,
                    long rps,
                    double poolLoad
            ) {
                this.clientId = clientId;
                this.pool = pool;
                this.maxDcRequest = maxDcRequest;
                this.maxSend = maxSend;
                this.maxRequests = maxRequests;
                this.dcLoad = dcLoad;
                this.rps = rps;
                this.poolLoad = poolLoad;
            }
        }

        private List<Stats> getLoadStats(){
            Map<DcEmulator, Long> dcRpsMap = getPoolStream()
                    .collect(Collectors.toMap(
                            pool -> pool.dcEmulator,
                            ClientPoolEmulator::getRps,
                            (rps1, rps2) -> rps1 + rps2
                    ));
            List<Stats> result = new ArrayList<>();

            int clientId = 0;
            for (WeightModel  model: models) {
                List<Stats> normalize = new ArrayList<>();
                long sumMaxRequests = 0;
                long remainingRequests = 1000;
                double maxSendSum = model.getPoolStream().mapToDouble(pool -> pool.getMaxRequestSend()).sum();
                for (ClientPoolEmulator pool : model.getPoolStream().collect(Collectors.toList())) {
                    DcEmulator dc = pool.dcEmulator;
                    long maxDcRequest = Math.max(dc.maxRequestPerSecond - dcRpsMap.get(dc) + pool.getRps(), 1);
                    long rps = pool.getRps();
                    double dcLoad = rps / (double)maxDcRequest;
                    long maxSend = pool.getMaxRequestSend();
                    double poolLoad = rps / (double)maxSend;

                    long maxRequests;

                    double percentileOfSlots = maxSend / maxSendSum;
                    if (maxDcRequest / 1000.0 < percentileOfSlots * 1.1) {
                        maxRequests = maxDcRequest;
                        remainingRequests -= maxDcRequest;
                    } else {
                        maxRequests = maxSend;
                        sumMaxRequests += maxRequests;
                    }

                    Stats stats = new Stats(
                            clientId,
                            pool,
                            maxDcRequest,
                            maxSend,
                            maxRequests,
                            dcLoad,
                            rps,
                            poolLoad
                    );
                    result.add(stats);
                    normalize.add(stats);
                }
                for (Stats stats : normalize) {
                    if (stats.maxRequests == stats.maxDcRequest) {
                        stats.poolLoadDiff = stats.rps / (double)stats.maxRequests;
                    } else {
                        double percentileOfRequests = stats.rps / (double) remainingRequests;
                        double percentileOfSlots = stats.maxRequests / (double) sumMaxRequests;
                        stats.poolLoadDiff = percentileOfRequests / percentileOfSlots;
                    }
                }

                clientId++;
            }
            return result;
        }

        public void print(){
            if (convergenceTick == currentTick) {
                System.out.println("Convergence on tick "  + currentTick);
            } else if (convergenceTick > 0) {
                System.out.println("Tick " + currentTick + ", already convergence");
            } else {
                System.out.println("Tick " + currentTick + ", not convergence yet");
            }
            for (Stats stats : getLoadStats()) {
                System.out.printf(
                        "Pool %2d dc %20s max dc %7d max pool %7d dcLoad %4.3f poolLoad %4.3f loadDiff %4.3f weight %4.3f load %4.3f%n",
                        stats.clientId,
                        stats.pool,
                        stats.maxDcRequest,
                        stats.maxSend,
                        stats.dcLoad,
                        stats.poolLoad,
                        stats.poolLoadDiff,
                        stats.pool.weight1,
                        stats.pool.avgDcLoad
                );
            }
        }
    }

    private class DcEmulator {
        private final long maxRequestPerSecond; // 1000 для одного дс
        private final long queueSize = 2000;

        long pendingRequests;

        long clientCount;

        public DcEmulator(long maxRequestPerSecond) {
            this.maxRequestPerSecond = maxRequestPerSecond;
        }

        public void run(){
            pendingRequests -= maxRequestPerSecond;
            if (pendingRequests < 0) {
                pendingRequests = 0;
            }
        }

        public long submitRequests(long request){
            long freeSlots = (queueSize - Math.max(0, pendingRequests - maxRequestPerSecond)) / clientCount;
            long result = Math.min(freeSlots, request);
            pendingRequests += freeSlots;
            return result;
        }

        public void addClient(){
            clientCount++;
        }
    }

    private static class ClientPoolEmulator {
        private final LoadBasedBalancer.LoadAccumulator accumulator;
        private final DcEmulator dcEmulator;
        private final long requestLatencyMs; // 100 мс для одного дс
        private final long maxConnections = 100;

        private long pendingRequests = 0;
        private double dcLoad = 0;
        private double avgDcLoad = 0;
        private double weight1 = 0;
        double[] weightHistory = new double[5];

        public ClientPoolEmulator(DcEmulator dcEmulator, long requestLatencyMs) {
            this.dcEmulator = dcEmulator;
            dcEmulator.addClient();
            this.requestLatencyMs = requestLatencyMs;
            accumulator = new LoadBasedBalancer.LoadAccumulator(this);
        }

        public LoadBasedBalancer.LoadAccumulator getAccumulator() {
            return accumulator;
        }

        public double calcLoad() {
            long maxRequestSend = getMaxRequestSend();

            pendingRequests += getRps();
            long arrivedRequest = pendingRequests;
            pendingRequests -=// Math.min(pendingRequests, Math.min(maxRequestSend, dcEmulator.maxRequestPerSecond));
                    dcEmulator.submitRequests(Math.min(pendingRequests, maxRequestSend));


            if (pendingRequests == 0) {
                return arrivedRequest / (double)getMaxRequestSend();
            } else {
                return 1 + pendingRequests / (double)maxConnections;
            }
        }

        private long getRps() {
            return Math.round(1000 * weight1);
        }


        public void run(){
            dcLoad = calcLoad();
            avgDcLoad = avgDcLoad * CONFIG.getDcLoadFilterFactor() + dcLoad * (1 - CONFIG.getDcLoadFilterFactor());
        }

        public double getAvgDcLoad(){
            return avgDcLoad;
        }

        private long getMaxRequestSend() {
            return maxConnections * 1000 / requestLatencyMs;
        }

        public void reset() {
            pendingRequests = 0;
            avgDcLoad = 0;
            weight1 = 0;
            dcEmulator.pendingRequests = 0;
        }

        public void setWeight(double weight) {
            for (int i = weightHistory.length - 1; i > 0; i--) {
                weightHistory[i] = weightHistory[i - 1];

            }
            weightHistory[0] = weight;
            weight1 = weight;
        }

        public boolean isWeightChanged(double eps){
            double max = Arrays.stream(weightHistory).max().getAsDouble();
            double min = Arrays.stream(weightHistory).min().getAsDouble();
            return max - min > eps;
        }

        @Override
        public String toString() {
            return String.format("{rps=%4d, , latency=%4d}", dcEmulator.maxRequestPerSecond, requestLatencyMs);
        }

        public double getDcLoad() {
            return dcLoad;
        }
    }
}
