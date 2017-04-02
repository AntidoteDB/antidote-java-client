package main.java.AntidoteClient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.security.PublicKey;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The Class PoolManager.
 */
public class PoolManager {

    /**
     * The pools.
     */
    private List<ConnectionPool> pools = new LinkedList<ConnectionPool>();

    /**
     * The retries.
     */
    private int retries = 0;

    /**
     * Generates pool.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param hosts           the hosts
     */
    private void createPool(int maxPoolSize, int initialPoolSize, List<Host> hosts) {
        for (Host h : hosts) {
            pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
        }
//        for (ConnectionPool p : pools) {
//            if (!p.isHealthy()) {
        unhealthyHostRecovery();

//            }
//        }
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
        createPool(maxPoolSize, initialPoolSize, hosts);
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
        createPool(maxPoolSize, initialPoolSize, hosts);

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
     * Recover unhealthy pools.
     */
    public void unhealthyHostRecovery() {
        ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor();
        Runnable periodicTask = new Runnable() {
            public void run() {
                // Invoke method(s) to do the work
                for (ConnectionPool p : pools) {
                    if (!p.isHealthy()) {
                        if (p.checkHealth(p)) {
                            pools.add(p);
                        }
                        ;
                    }
                }
            }

        };
        executor.scheduleAtFixedRate(periodicTask, 0, 5, TimeUnit.MINUTES);
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    public Connection getConnection() {
        int selectedIndex = 0;
        List<ConnectionPool> healthyPools = getHealthyPools();
        if (healthyPools.size() > 0) {
            do {
                for (int i = 0; i < healthyPools.size(); i++) {
                    //maybe I can work on this . This is random that's why any connection send to client
                    //todo What can be the implementation ?  Should I have to give index in seq if that pool is full used then index will increase
                    selectedIndex = (int) (Math.random() * healthyPools.size());

                    ConnectionPool p = healthyPools.get(selectedIndex);
                    if (p.isHealthy()) {
                        try {
                            Socket s = p.getConnection();
                            if (s != null) {
                                return new Connection(p, s);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            } while (++retries < 3);
        }
        throw new RuntimeException("Cannot open connection");
    }

    /**
     * Send message.
     *
     * @param requestMessage the request message
     * @return the antidote message
     */
    public AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        Connection c = this.getConnection();
        try {
            DataOutputStream dataOutputStream = new DataOutputStream(c.getSocket().getOutputStream());
            dataOutputStream.writeInt(requestMessage.getLength());
            dataOutputStream.writeByte(requestMessage.getCode());
            requestMessage.getMessage().writeTo(dataOutputStream);
            dataOutputStream.flush();

            DataInputStream dataInputStream = new DataInputStream(c.getSocket().getInputStream());
            int responseLength = dataInputStream.readInt();
            int responseCode = dataInputStream.readByte();

            byte[] messageData = new byte[responseLength - 1];
            dataInputStream.readFully(messageData, 0, responseLength - 1);

            return new AntidoteMessage(responseLength, responseCode, messageData);

        } catch (Exception e) {
            //if msg fails make it to unhealthy
            c.setunHealthyConnection();
            throw new RuntimeException(e.getMessage(), e.getCause());

        } finally {
            c.returnConnection();
        }
    }

    /**
     * Gets the healthy pools.
     *
     * @return the healthy pools
     */
    private List<ConnectionPool> getHealthyPools() {
        List<ConnectionPool> healthyPools = new LinkedList<ConnectionPool>();
        for (ConnectionPool p : pools) {
            if (p.isHealthy()) {
                healthyPools.add(p);
            }
        }
        return healthyPools;
    }
}
