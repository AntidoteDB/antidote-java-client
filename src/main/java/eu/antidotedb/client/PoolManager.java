package eu.antidotedb.client;

import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.Transformer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The Class PoolManager.
 */
public class PoolManager {

    private static final int CHECK_INTERVAL = 3;
    /**
     * The pools.
     */
    private List<ConnectionPool> pools = new LinkedList<ConnectionPool>();

    /**
     * The retries.
     */
    private int retries = 0;


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
     * Generates pool.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param hosts           the hosts
     */
    private void createPool(int maxPoolSize, int initialPoolSize, List<Host> hosts) {
        ConnectionPool obj;
        for (Host h : hosts) {
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
    public boolean addHost(int maxPoolSize, int initialPoolSize, Host h) {
        int initSize = pools.size();
        pools.add(new ConnectionPool(maxPoolSize, initialPoolSize, h.getHostname(), h.getPort()));
        return pools.size() > initSize;
    }

    /**
     * Recover unhealthy pools.
     */
    public void unhealthyHostRecovery() {
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
     * Gets the connection.
     *
     * @return the connection
     */
    //todo retruning connection sending redundent pools
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

    /**
     * Send message.
     *
     * @param requestMessage the request message
     * @param c              the connection object
     * @param downstream     handler for sending message
     * @return the antidote message
     */
    public <R> R sendMessage(AntidoteRequest<R> requestMessage, Connection c, Transformer downstream) {
        AntidoteResponse.Handler<R> responseExtractor = requestMessage.readResponseExtractor();
        AntidoteResponse response = requestMessage.accept(downstream.toHandler(c));
        if (responseExtractor == null) {
            return null;
        }
        if (response == null) {
            throw new AntidoteException("Missing response for " + requestMessage);
        }
        return response.accept(responseExtractor);

//        try {
//            DataOutputStream dataOutputStream = new DataOutputStream(c.getSocket().getOutputStream());
//            dataOutputStream.writeInt(requestMessage.getLength());
//            dataOutputStream.writeByte(requestMessage.getCode());
//            requestMessage.getMessage().writeTo(dataOutputStream);
//            dataOutputStream.flush();
//            DataInputStream dataInputStream = new DataInputStream(c.getSocket().getInputStream());
//            int responseLength = dataInputStream.readInt();
//            int responseCode = dataInputStream.readByte();
//            byte[] messageData = new byte[responseLength - 1];
//            dataInputStream.readFully(messageData, 0, responseLength - 1);
//            return new AntidoteMessage(responseLength, responseCode, messageData);
//        } catch (IOException e) {
//            //if msg fails make it to unhealthy
//            c.setunHealthyConnection();
//            throw new AntidoteException("Could not send message", e);
//        } finally {
//            c.returnConnection();
//        }
    }



}
