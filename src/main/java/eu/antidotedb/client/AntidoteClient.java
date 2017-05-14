package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB.*;
import com.google.protobuf.ByteString;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.transformer.Transformer;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {

    private PoolManager poolManager;

    private Transformer downstream;

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager the pool manager object
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
        this.downstream = new SocketSender();
    }

    /**
     * Adds a transformer on top of the stack of transformers of this connection.
     *
     * @param transformer the new transformer to be added
     */
    public void addTransformer(Transformer transformer) {
        transformer.connect(this.downstream);
        this.downstream = transformer;
    }

    /**
     * Send message to database.
     * This will use an arbitrary connection from the connection pool.
     *
     * @param requestMessage the update message
     * @return the response
     */
    <R> R sendMessageArbitraryConnection(AntidoteRequest<R> requestMessage) {
        try (Connection connection = getPoolManager().getConnection()) {
            return sendMessage(requestMessage, connection);
        }
    }

    /**
     * Send message to database.
     *
     * @param requestMessage the update message
     * @param connection     the connection to use for sending
     * @return the response
     */
    <R> R sendMessage(AntidoteRequest<R> requestMessage, Connection connection) {
        return getPoolManager().sendMessage(requestMessage, connection, downstream);
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
     * <p>
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

