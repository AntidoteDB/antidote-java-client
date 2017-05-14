package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB.*;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.Transformer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Class AntidoteClient.
 */
public final class AntidoteClient {

    private PoolManager poolManager;

    private Transformer downstream = new SocketSender();


    public AntidoteClient(Host ... hosts) {
        this.poolManager = new PoolManager();
        for (Host host : hosts) {
            poolManager.addHost(host);
        }
    }

    /**
     * Instantiates a new antidote client.
     *
     * @param poolManager defines where to find Antidote hosts
     */
    public AntidoteClient(PoolManager poolManager) {
        this.poolManager = poolManager;
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
     * Sends a message to the database.
     * This will use an arbitrary connection from the connection pool.
     * This is a synchronous call which will block until the response is available.
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
     * Sends a message to the database.
     * This is a synchronous call which will block until the response is available.
     *
     * @param requestMessage the update message
     * @param connection     the connection to use for sending
     * @return the response
     */
    <R> R sendMessage(AntidoteRequest<R> requestMessage, Connection connection) {
        AntidoteResponse.Handler<R> responseExtractor = requestMessage.readResponseExtractor();
        AntidoteResponse response = requestMessage.accept(downstream.toHandler(connection));
        if (responseExtractor == null) {
            return null;
        }
        if (response == null) {
            throw new AntidoteException("Missing response for " + requestMessage);
        }
        return response.accept(responseExtractor);
    }

    /**
     * Starts an interactive transactions.
     * Interactive transactions allow to mix several reads and writes in a single atomic unit.
     */
    public InteractiveTransaction startTransaction() {
        return new InteractiveTransaction(this);
    }

    /**
     * Creates a static transaction.
     * Static transactions can be used to execute a set of updates atomically.
     *
     */
    public AntidoteStaticTransaction createStaticTransaction() {
        return new AntidoteStaticTransaction(this);
    }


    public BatchRead newBatchRead() {
        return new BatchRead(this);
    }

    /**
     * Get the pool manager.
     * This can be used to configure connections at runtime.
     */
    public PoolManager getPoolManager() {
        return poolManager;
    }

    /**
     * Completes a transaction and throws an exception if there was a problem
     */
    CommitInfo completeTransaction(ApbCommitResp commitResponse) {
        if (commitResponse.getSuccess()) {
            return new CommitInfo(commitResponse.getCommitTime());
        } else {
            throw new AntidoteException("Failed to commit transaction (Error code: " + commitResponse.getErrorcode() + ")");
        }


    }


    /**
     * Reads the values of a list of objects in one batch read
     */
    public <T> List<T> readObjects(TransactionWithReads tx, List<ObjectRef<? extends T>> objectRefs) {
        BatchRead batchRead = newBatchRead();
        List<BatchReadResult<? extends T>> results = new ArrayList<>(objectRefs.size());
        for (ObjectRef<? extends T> objectRef : objectRefs) {
            BatchReadResult<? extends T> res = objectRef.read(batchRead);
            results.add(res);
        }
        batchRead.commit(tx);
        return results.stream()
                .map(BatchReadResult::get)
                .collect(Collectors.toList());
    }

    /**
     * Use this for executing updates and reads without a transaction context.
     */
    public NoTransaction noTransaction() {
        return new NoTransaction(this);
    }

    /**
     * pulls in new state for a set of CRDTs
     * <p>
     * all reads are based on the same snapshot
     */
    public void pull(TransactionWithReads tx, Iterable<? extends AntidoteCRDT> objects) {
        BatchRead batchRead = newBatchRead();
        for (AntidoteCRDT object : objects) {
            object.pull(batchRead);
        }
        batchRead.commit(tx);
    }

}

