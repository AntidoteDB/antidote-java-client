package main.java.AntidoteClient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class ConnectionPool.
 */
public class ConnectionPool {

    /**
     * The Constant logger.
     */
    private static final Logger logger = Logger.getLogger(ConnectionPool.class.getCanonicalName());
    private static final int DEFAULT_TIMEOUT = 200;

    /**
     * The pool.
     */
    private BlockingQueue<Socket> pool;

    private int maxPoolSize;

    private int initialPoolSize;

    private int currentPoolSize;

    private String host;

    private int port;

    private boolean healthy;

    /**
     * The failures.
     */
    private int failures;

    /**
     * The active connections.
     */
    private int activeConnections;

    /**
     * Instantiates a new connection pool.
     *
     * @param maxPoolSize     the max pool size
     * @param initialPoolSize the initial pool size
     * @param host            the host
     * @param port            the port
     */
    public ConnectionPool(int maxPoolSize, int initialPoolSize, String host, int port) {

        if ((initialPoolSize > maxPoolSize) || initialPoolSize < 1 || maxPoolSize < 1) {
            throw new IllegalArgumentException("Invalid pool size parameters");
        }

        // default max pool size to 10
        this.setMaxPoolSize(maxPoolSize > 0 ? maxPoolSize : 10);
        this.setInitialPoolSize(initialPoolSize);
        this.setHost(host);
        this.setPort(port);
        this.pool = new LinkedBlockingQueue<Socket>(maxPoolSize);
        this.setHealthy(true);
        this.failures = 0;

        for (int i = 0; i < initialPoolSize; i++) {
            if (this.isHealthy() && getCurrentPoolSize() < maxPoolSize) {
                if (!openAndPoolConnection()) {
                    if (++failures >= 3) {
                        this.setHealthy(false);

                    }
                }
            } else {
                break;
            }
        }

        if (pool.size() != initialPoolSize) {
            logger.log(Level.WARNING,
                    "Initial sized pool creation failed. InitializedPoolSize={0}, initialPoolSize={1}",
                    new Object[]{pool.size(), initialPoolSize});
        }

    }

    public boolean checkHealth(ConnectionPool p) {
        Socket s = new Socket();
        try {
            s.setSoTimeout(DEFAULT_TIMEOUT);
            s.connect(new InetSocketAddress(p.getHost(), p.getPort()), DEFAULT_TIMEOUT);
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);

        } finally {
            try {
                s.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Open and pool connection.
     *
     * @return true, if successful
     */
    private synchronized boolean openAndPoolConnection() {
        try {

            Socket s = new Socket();
            s.setSoTimeout(DEFAULT_TIMEOUT);
            s.connect(new InetSocketAddress(getHost(), getPort()), DEFAULT_TIMEOUT);
            pool.offer(s);
            setCurrentPoolSize(getCurrentPoolSize() + 1);
            this.setHealthy(true);
            this.failures = 0;

//            logger.log(Level.INFO, "Created connection {0}, currentPoolSize={1}, maxPoolSize={2}, activeConnection={3}",
//                    new Object[]{s, getCurrentPoolSize(), getMaxPoolSize(), activeConnections});
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     * @throws InterruptedException the interrupted exception
     */
    public Socket getConnection() throws InterruptedException {
        if (pool.peek() == null && getCurrentPoolSize() < getMaxPoolSize()) {
            if (!openAndPoolConnection()) {
                failures++;
            } else {
                activeConnections++;
            }
        }

        //  logger.log(Level.INFO, "Requested Connection {0}, currentPoolSize={1}, maxPoolSize={2}, activeConnection={3}",
        //        new Object[]{pool.take(), getCurrentPoolSize(), getMaxPoolSize(), activeConnections});
        activeConnections++; //I have to add because it not incrmenting the active connection
        return pool.take();
    }

    /**
     * Surrender connection.
     *
     * @param s the s
     */
    public void surrenderConnection(Socket s) {
//        System.out.println("Surrender :" + s + " pool size : " + pool.size() + "removeing" + pool.offer(s));
        activeConnections--;
        pool.offer(s); // offer() as we do not want to go beyond capacity
    }

    /**
     * The healthy.
     */
    /**
     * Checks if is healthy.
     *
     * @return true, if is healthy
     */
    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    /**
     * The host.
     */
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /**
     * The port.
     */
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Maximum number of connections that the pool can have.
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * Number of connections that should be created initially.
     */
    public int getInitialPoolSize() {
        return initialPoolSize;
    }

    public void setInitialPoolSize(int initialPoolSize) {
        this.initialPoolSize = initialPoolSize;
    }

    /**
     * Number of connections generated so far.
     */
    public int getCurrentPoolSize() {
        return currentPoolSize;
    }

    public void setCurrentPoolSize(int currentPoolSize) {
        this.currentPoolSize = currentPoolSize;
    }
}