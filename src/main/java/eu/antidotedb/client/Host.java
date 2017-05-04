package eu.antidotedb.client;

/**
 * The Class Host.
 */
public class Host {
    
    /** The hostname. */
    private String hostname;
    
    /** The port. */
    private int port;

    /**
     * Instantiates a new host.
     *
     * @param h the h
     * @param p the p
     */
    public Host(String h, int p) {
        hostname = h;
        port = p;
    }

    /**
     * Gets the hostname.
     *
     * @return the hostname
     */
    public String getHostname() {
        return hostname;
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }
}
