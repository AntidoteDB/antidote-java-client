package eu.antidotedb.client;

import eu.antidotedb.client.transformer.TransformerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private static final int DEFAULT_TIMEOUT = 0; // wait forever
    // time (in ms) to wait until new connection becomes available
    private static final long GET_CONNECTION_WAIT_MS = 20;
    private static final int MAX_FAILURE = 3;

    /**
     * Pool of available connections
     */
    private final BlockingQueue<Connection> pool;

    /**
     * List of all connections created for this pool.
     */
    private final List<Connection> connections = new CopyOnWriteArrayList<>();


    private final int maxPoolSize;

    private final int initialPoolSize;

    private final String host;

    private final int port;

    private volatile boolean healthy;

    /**
     * The failures.
     */
    private AtomicInteger failures = new AtomicInteger();
    private List<TransformerFactory> transformerFactories;


    /**
     * Instantiates a new connection pool.
     *
     * @param maxPoolSize          the max pool size
     * @param initialPoolSize      the initial pool size
     * @param host                 the host
     * @param port                 the port
     * @param transformerFactories
     */
    public ConnectionPool(int maxPoolSize, int initialPoolSize, String host, int port, List<TransformerFactory> transformerFactories) {
        this.transformerFactories = transformerFactories;

        if ((initialPoolSize > maxPoolSize) || initialPoolSize < 1 || maxPoolSize < 1) {
            throw new IllegalArgumentException("Invalid pool size parameters");
        }

        // default max pool size to 10
        this.maxPoolSize = maxPoolSize;
        this.initialPoolSize = initialPoolSize;
        this.host = host;
        this.port = port;
        this.pool = new ArrayBlockingQueue<>(maxPoolSize);
        this.healthy = true;

        for (int i = 0; i < initialPoolSize; i++) {
            if (this.isHealthy() && getCurrentPoolSize() < maxPoolSize) {
                if (!openAndPoolConnection()) {
                    if (failures.incrementAndGet() >= MAX_FAILURE) {
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
        try {
            Connection s = p.getConnection();
            s.getSocket().setSoTimeout(DEFAULT_TIMEOUT);
//            DataInputStream din = new DataInputStream(s.getSocket().getInputStream());
            // TODO send heartbeat message
            p.surrenderConnection(s);
            p.setHealthy(true);
            return true;
        } catch (IOException e) {
            p.setHealthy(false);
            return false;
        } catch (InterruptedException e) {
            // when interrupted, assume we are healthy (check interrupted)
            return true;
        }
    }

    /**
     * Open and pool connection.
     *
     * @return true, if successful
     */
    private boolean openAndPoolConnection() {
        try {
            Connection c = openConnection();
            pool.offer(c);
            return true;
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error opening connection to " + host + ":" + port, e);
            return false;
        }
    }

    private Connection openConnection() throws IOException {
        Socket s = new Socket();
        s.setSoTimeout(DEFAULT_TIMEOUT);
        s.connect(new InetSocketAddress(getHost(), getPort()), DEFAULT_TIMEOUT);
        Connection c = new Connection(this, s, transformerFactories);
        connections.add(c);
        return c;
    }

    /**
     * returns a connection from the pool
     */
    public Connection getConnection() throws InterruptedException {
        Connection connection = pool.poll(GET_CONNECTION_WAIT_MS, TimeUnit.MILLISECONDS);
        if (connection != null) {
            return connection;
        }
        if (getCurrentPoolSize() < getMaxPoolSize()) {
            // there is a small chance here, that we generate too many connections
            try {
                return openConnection();
            } catch (IOException e) {
                throw new AntidoteException("Could not open connection to " + host + ":" + port, e);
            }
        }
        throw new AntidoteException("Could not get connection to " + host + ":" + port + " (too many connections)");
    }

    /**
     * Surrender connection.
     *
     * @param s the s
     */
    public void surrenderConnection(Connection s) {
        try {
            pool.add(s);
        } catch (IllegalStateException e) {
            // over capacity, discard connection
            s.discard();
            connections.remove(s);
        }
    }

    /**
     * Checks if is healthy.
     *
     * @return true, if is healthy
     */
    public boolean isHealthy() {
        return healthy;
    }

    void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    /**
     * The host.
     */
    public String getHost() {
        return host;
    }

    /**
     * The port.
     */
    public int getPort() {
        return port;
    }


    /**
     * Maximum number of connections that the pool can have.
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * Number of connections that should be created initially.
     */
    public int getInitialPoolSize() {
        return initialPoolSize;
    }


    /**
     * Number of connections generated so far.
     */
    public int getCurrentPoolSize() {
        return connections.size();
    }

    /**
     * Returns the number of currently active connections.
     */
    public int getActiveConnections() {
        return connections.size() - pool.size();
    }
}