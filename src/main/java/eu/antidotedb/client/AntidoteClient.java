package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB.ApbCommitResp;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.TransformerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An AntidoteClient manages the connection to one or more Antidote servers.
 * <p>
 * It is the main entry point for working with the client, in particular it is the source of transactions.
 * Every operation has to be executed in the context of a transaction.
 * See: {@link #startTransaction()}, {@link #createStaticTransaction()}, {@link #noTransaction()}, and {@link #newBatchRead()}.
 * <p>
 * Moreover there is {@link #readObjects(TransactionWithReads, Iterable)} to read several objects at once
 * and {@link #pull(TransactionWithReads, Iterable)} to update several mutable {@link AntidoteCRDT} objects at once.
 */
public class AntidoteClient {

    private PoolManager poolManager;


    /**
     * Initializes an AntidoteClient with the given hosts.
     *
     * @param hosts The Antidote hosts (ip and port)
     */
    public AntidoteClient(Host... hosts) {
        this(Collections.emptyList(), hosts);
    }

    /**
     * Initializes an AntidoteClient with the given hosts.
     *
     * @param hosts The Antidote hosts (ip and port)
     */
    public AntidoteClient(List<Host> hosts) {
        this(Collections.emptyList(), hosts);
    }

    /**
     * Initializes an AntidoteClient.
     *
     * @param transformerFactories transformers for factories (the last transformer will be at the top of the stack, so applied first)
     * @param hosts                The Antidote hosts (ip and port)
     */
    public AntidoteClient(List<TransformerFactory> transformerFactories, Host... hosts) {
        this(transformerFactories, Arrays.asList(hosts));
    }

    /**
     * Initializes an AntidoteClient.
     *
     * @param transformerFactories transformers for factories (the last transformer will be at the top of the stack, so applied first)
     * @param hosts                The Antidote hosts (ip and port)
     */
    public AntidoteClient(List<TransformerFactory> transformerFactories, List<Host> hosts) {
        init(transformerFactories, hosts);
    }

    /**
     * initializes the client. Called by every constructor except for {@link #AntidoteClient(PoolManager)}.
     */
    protected void init(List<TransformerFactory> transformerFactories, List<Host> hosts) {
        this.poolManager = new PoolManager(transformerFactories);
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
     * Sends a message to the database.
     * This will use an arbitrary connection from the connection pool.
     * This is a synchronous call which will block until the response is available.
     *
     * @param requestMessage the update message
     * @return the response
     */
    <R> R sendMessageArbitraryConnection(AntidoteTransaction tx, AntidoteRequest<R> requestMessage) {
        Connection connection = getPoolManager().getConnection();
        try {
            tx.onGetConnection(connection);
            return sendMessage(requestMessage, connection);
        } finally {
            tx.onReleaseConnection(connection);
            connection.close();
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
        AntidoteResponse response = requestMessage.accept(connection.transformer());
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
     * <p>
     * Since an interactive transaction uses database resources, you should ensure that the transaction is closed in any case.
     * The recommended pattern is to use a try-with-resource statement and commit the transaction at the end of it:
     * <pre>
     * {@code
     * try (InteractiveTransaction tx = antidoteClient.startTransaction()) {
     *     // updates and reads here
     *     tx.commitTransaction();
     * }
     * }
     * </pre>
     */
    public InteractiveTransaction startTransaction() {
        return new InteractiveTransaction(this);
    }

    /**
     * Creates a static transaction.
     * Static transactions can be used to execute a set of updates atomically.
     */
    public AntidoteStaticTransaction createStaticTransaction() {
        return new AntidoteStaticTransaction(this);
    }


    /**
     * Starts a new batch read, which allows to read several objects at once.
     * The {@link BatchRead} can be committed using {@link BatchRead#commit} or {@link BatchRead#commit(TransactionWithReads)}.
     */
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
    public <T> List<T> readObjects(TransactionWithReads tx, Iterable<? extends ObjectRef<? extends T>> objectRefs) {
        BatchRead batchRead = newBatchRead();
        List<BatchReadResult<? extends T>> results = new ArrayList<>();
        for (ObjectRef<? extends T> objectRef : objectRefs) {
            BatchReadResult<? extends T> res = objectRef.read(batchRead);
            results.add(res);
        }
        batchRead.commit(tx);
        List<T> list = new ArrayList<>();
        for (BatchReadResult<? extends T> result : results) {
            T t = result.get();
            list.add(t);
        }
        return list;
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

