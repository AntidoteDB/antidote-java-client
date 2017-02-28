package main.java.AntidoteClient;

import java.io.Closeable;
import java.io.IOException;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {
    
    /**  Pool Manager. */
    private PoolManager poolManager;

    /**  The antidote transaction. */
    private AntidoteTransaction antidoteTransaction;

    /**  The antidote static transaction. */
    private AntidoteStaticTransaction antidoteStaticTransaction;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
        this.antidoteTransaction = null;
        this.antidoteStaticTransaction = null;
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

    /**
     * Create antidote transaction.
     *
     * @return the antidote transaction
     */
    public AntidoteTransaction createTransaction(){
        antidoteTransaction = new AntidoteTransaction(this);
        antidoteTransaction.setTransactionStatus(AntidoteTransaction.TransactionStatus.CREATED);
        antidoteTransaction.startTransaction();
        return antidoteTransaction;
    }

    /**
     * Create antidote static transaction.
     *
     * @return the antidote static transaction
     */
    public AntidoteTransaction createStaticTransaction(){
        antidoteStaticTransaction = new AntidoteStaticTransaction(this);
        antidoteStaticTransaction.setTransactionStatus(AntidoteStaticTransaction.TransactionStatus.CREATED);
        antidoteStaticTransaction.startTransaction();
        return antidoteStaticTransaction;
    }
}

