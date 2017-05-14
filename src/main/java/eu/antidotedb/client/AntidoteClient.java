package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB.*;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
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



    public BatchRead newBatchRead() {
        return new BatchRead(this);
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





    /**
     * Reads the values of a list of objects in one batch read
     */
    public List<Object> readObjects(List<ObjectRef> objectRefs) {
        List<Object> results = new ArrayList<>(objectRefs.size());
        // TODO change to batch read
        for (ObjectRef objectRef : objectRefs) {
            results.add(objectRef.read(noTransaction()));
        }
        return results;
    }

    public NoTransaction noTransaction() {
        return new NoTransaction(this);
    }

    /**
     * pulls in new state for a set of CRDTs
     *
     * all reads are based on the same snapshot
     */
    public void pull(Iterable<? extends AntidoteCRDT> objects) {
        BatchRead batchRead = newBatchRead();
        for (AntidoteCRDT object : objects) {
            object.pull(batchRead);
        }
        batchRead.commit();
    }

    // TODO inline
    public void readCrdts(Iterable<? extends AntidoteCRDT> antidoteCRDTS) {
        pull(antidoteCRDTS);
//        // TODO change to pull
//        try (InteractiveTransaction tx = startTransaction()) {
//            for (AntidoteCRDT antidoteCRDT : antidoteCRDTS) {
//                antidoteCRDT.pull(tx);
//            }
//            tx.commitTransaction();
//        }
    }

    // TODO inline
    public void readOuterObjects(List<CrdtMapDynamic<String>> antidoteCRDTS) {
        readCrdts(antidoteCRDTS);
    }
}

