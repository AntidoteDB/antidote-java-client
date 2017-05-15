package eu.antidotedb.client;

import eu.antidotedb.client.transformer.TransformerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The Class PoolManager.
 */
public class PoolManager {

    private static final int CHECK_INTERVAL = 3;
    public static final int DEFAULT_INITIAL_POOL_SIZE = 1;
    public static final int DEFAULT_MAX_POOL_SIZE = 50;
    /**
     * The pools.
     */
    private List<ConnectionPool> pools = new CopyOnWriteArrayList<>();
    private final List<TransformerFactory> transformerFactories;


    /**
     * Creates a new empty pool manager.
     * Use addHost to add hosts.
     */
    public PoolManager(List<TransformerFactory> transformerFactories) {
        this.transformerFactories = transformerFactories;
        //starting concurrent thread for heartbeat
        unhealthyHostRecovery();
    }


    /**
     * Instantiates a new pool manager.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param h               Host to add into pool
     */
    public void addHost(int maxPoolSize, int initialPoolSize, Host h) {
        pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort(), transformerFactories));
    }

    /**
     * Instantiates a new pool manager.
     *
     * @param h Host to add into pool
     */
    public void addHost(Host h) {
        addHost(DEFAULT_MAX_POOL_SIZE, DEFAULT_INITIAL_POOL_SIZE, h);
    }

    /**
     * Recover unhealthy pools.
     */
    private void unhealthyHostRecovery() {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();
        Runnable periodicTask = new Runnable() {
            public void run() {
                // Scheduled Task
                for (ConnectionPool p : pools) {
                    if (p.checkHealth(p)) {
                        //make it healthy
                        p.setHealthy(true);
                    }
                }
            }

        };
        executor.scheduleAtFixedRate(periodicTask, 0, CHECK_INTERVAL, TimeUnit.MINUTES);
    }

    /**
     * Returns a random healthy connection
     */
    public Connection getConnection() {
        int selectedIndex = 0;
        if (pools.size() > 0) {
            for (int i = 0; i < pools.size(); i++) {
                selectedIndex = (int) (Math.random() * pools.size());
                ConnectionPool p = pools.get(selectedIndex);
                if (p.isHealthy()) {
                    try {
                        Connection c = p.getConnection();
                        if (c != null) {
                            return c;
                        }
                    } catch (InterruptedException e) {
                        throw new AntidoteException(e);
                    }
                }
            }

        }
        throw new AntidoteException("Cannot open connection to any host. " +
                "(Configured hosts: " + pools.stream().map(p -> p.getHost() + ":" + p.getPort()).collect(Collectors.joining(", ")) + ")");
    }


}
