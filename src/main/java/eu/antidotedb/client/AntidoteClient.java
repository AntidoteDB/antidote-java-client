package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.ByteString;

import java.util.List;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {

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
    AntidoteMessage sendMessage(AntidoteRequest requestMessage) {
        return getPoolManager().sendMessage(requestMessage);
    }


    AntidoteMessage sendMessage(AntidoteRequest requestMessage, Connection connection) {
        return getPoolManager().sendMessage(requestMessage, connection);
    }

    /**
     * Create antidote transaction.
     *
     * @return the antidote transaction
     */
    public InteractiveTransaction startTransaction() {
        return new InteractiveTransaction(this);
    }

    /**
     * Create antidote static transaction.
     *
     * @return the antidote static transaction
     */
    public AntidoteStaticTransaction createStaticTransaction() {
        return new AntidoteStaticTransaction(this);
    }

    public void pull(List<AntidoteCRDT> objects) {
        BatchRead batchRead = new BatchRead();
        for (AntidoteCRDT object : objects) {
            object.pull(batchRead);
        }
        batchRead.commit();
    }

    /**
     * Pool Manager.
     */
    public PoolManager getPoolManager() {
        return poolManager;
    }

    CommitInfo completeTransaction(ApbCommitResp commitResponse) {
        if (commitResponse.getSuccess()) {
            return new CommitInfo(commitResponse.getCommitTime());
        } else {
            throw new AntidoteException("Failed to commit transaction (Error code: " + commitResponse.getErrorcode() + ")");
        }


    }

    public Bucket bucket(String bucketKey) {
        return bucket(ByteString.copyFromUtf8(bucketKey));
    }

    private Bucket bucket(ByteString bucketKey) {
        return new Bucket(bucketKey);
    }

}

