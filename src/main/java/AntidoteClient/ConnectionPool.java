package main.java.AntidoteClient;

import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionPool {

    private static final Logger logger = Logger.getLogger(ConnectionPool.class.getCanonicalName());

    private BlockingQueue<Socket> pool;
    /** Maximum number of connections that the pool can have */
    private int maxPoolSize;
    /** Number of connections that should be created initially */
    private int initialPoolSize;
    /** Number of connections generated so far */
    private int currentPoolSize;

    private String host;
    private int port;

    private boolean healthy;
    private int failures;
    private int activeConnections;

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

    private synchronized boolean openAndPoolConnection() {
        try {
            Socket s = new Socket(host, port);
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

    public void surrenderConnection(Socket s) {
        activeConnections--;
        if(!(s instanceof Socket)) { return; }
        pool.offer(s); // offer() as we do not want to go beyond capacity
    }

    public boolean isHealthy() {
        return healthy;
    }
}