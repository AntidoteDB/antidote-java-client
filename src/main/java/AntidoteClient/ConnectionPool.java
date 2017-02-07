package main.java.AntidoteClient;

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

    /** The Constant logger. */
    private static final Logger logger = Logger.getLogger(ConnectionPool.class.getCanonicalName());

    /** The pool. */
    private BlockingQueue<Socket> pool;
    
    /**  Maximum number of connections that the pool can have. */
    private int maxPoolSize;
    
    /**  Number of connections that should be created initially. */
    private int initialPoolSize;
    
    /**  Number of connections generated so far. */
    private int currentPoolSize;

    /** The host. */
    private String host;
    
    /** The port. */
    private int port;

    /** The healthy. */
    private boolean healthy;
    
    /** The failures. */
    private int failures;
    
    /** The active connections. */
    private int activeConnections;

    /**
     * Instantiates a new connection pool.
     *
     * @param maxPoolSize the max pool size
     * @param initialPoolSize the initial pool size
     * @param host the host
     * @param port the port
     */
    public ConnectionPool(int maxPoolSize, int initialPoolSize, String host, int port) {

        if( (initialPoolSize > maxPoolSize) || initialPoolSize < 1 || maxPoolSize < 1 ) {
            throw new IllegalArgumentException("Invalid pool size parameters");
        }

        // default max pool size to 10
        this.maxPoolSize = maxPoolSize>0 ? maxPoolSize : 10;
        this.initialPoolSize = initialPoolSize;
        this.host = host;
        this.port = port;
        this.pool = new LinkedBlockingQueue<Socket>(maxPoolSize);
        this.healthy = true;
        this.failures = 0;

        for(int i=0; i<initialPoolSize; i++) {
            if (this.healthy && currentPoolSize < maxPoolSize) {
                if(!openAndPoolConnection()) {
                    if (++failures >= 3) {
                        this.healthy = false;
                    }
                }
            } else {
                break;
            }
        }

        if(pool.size() != initialPoolSize) {
            logger.log(Level.WARNING,
                    "Initial sized pool creation failed. InitializedPoolSize={0}, initialPoolSize={1}",
                    new Object[]{pool.size(), initialPoolSize});
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
            s.setSoTimeout(200);
            s.connect(new InetSocketAddress(host, port), 200);
            pool.offer(s);
            currentPoolSize++;
            this.healthy = true;
            this.failures = 0;
            logger.log(Level.FINE, "Created connection {0}, currentPoolSize={1}, maxPoolSize={2}",
                    new Object[]{s, currentPoolSize, maxPoolSize});
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
        if (pool.peek() == null && currentPoolSize < maxPoolSize) {
            if (!openAndPoolConnection()) {
                failures++;
            } else {
                activeConnections++;
            }

        }
        return pool.take();
    }

    /**
     * Surrender connection.
     *
     * @param s the s
     */
    public void surrenderConnection(Socket s) {
        activeConnections--;
        if(!(s instanceof Socket)) { return; }
        pool.offer(s); // offer() as we do not want to go beyond capacity
    }

    /**
     * Checks if is healthy.
     *
     * @return true, if is healthy
     */
    public boolean isHealthy() {
        return healthy;
    }
}