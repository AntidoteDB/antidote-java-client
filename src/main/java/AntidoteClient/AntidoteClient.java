package main.java.AntidoteClient;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {
    
    /**  Pool Manager. */
    private PoolManager poolManager;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
    }
    
    /**
     * Send message to database.
     *
     * @param requestMessage the update message
     * @return the response
     */
    protected AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        return poolManager.sendMessage(requestMessage);
    }
}

