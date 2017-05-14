package eu.antidotedb.client;

import java.net.Socket;
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


    /**
     * Creates a new empty pool manager.
     * Use addHost to add hosts.
     */
    public PoolManager() {
    }

    /**
     * Instantiates a new pool manager.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param configFilePath  the path of config file
     */
    public PoolManager(int maxPoolSize, int initialPoolSize, String configFilePath) {
        AntidoteConfigManager cfgMgr = new AntidoteConfigManager();
        if (!cfgMgr.configExist(configFilePath)) {
            throw new RuntimeException("Config File not found!");
        }
        List<Host> hosts = cfgMgr.getConfigHosts(configFilePath);
        createPools(maxPoolSize, initialPoolSize, hosts);
    }

    /**
     * Instantiates a new pool manager.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     */
    public PoolManager(int maxPoolSize, int initialPoolSize) {
        AntidoteConfigManager cfgMgr = new AntidoteConfigManager();
        if (!cfgMgr.configExist()) {
            cfgMgr.generateDefaultConfig();
        }
        List<Host> hosts = cfgMgr.getConfigHosts();
        createPools(maxPoolSize, initialPoolSize, hosts);

    }

    /**
     * Generates pool.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param hosts           the hosts
     */
    private void createPools(int maxPoolSize, int initialPoolSize, List<Host> hosts) {
        for (Host h : hosts) {
            addHost(maxPoolSize, initialPoolSize, h);
            pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
        }
        //if pool is empty
        if (pools.size() == 0) {
            throw new IllegalArgumentException("Unable to make Database connection ! Please try again.");
        }
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
        pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
    }

    /**
     * Instantiates a new pool manager.
     *
     * @param h               Host to add into pool
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
                        Socket s = p.getConnection();
                        if (s != null) {
                            return new Connection(p, s);
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
