package main.java.AntidoteClient;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.security.PublicKey;
import java.util.LinkedList;
import java.util.List;

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
