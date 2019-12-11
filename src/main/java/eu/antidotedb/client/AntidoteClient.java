package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbCommitResp;
import eu.antidotedb.client.messages.AntidoteRequest;
import eu.antidotedb.client.messages.AntidoteResponse;
import eu.antidotedb.client.transformer.TransformerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * An AntidoteClient manages the connection to one or more Antidote servers.
 * <p>
 * It is the main entry point for working with the client, in particular it is the source of transactions.
 * Every operation has to be executed in the context of a transaction.
 * See: {@link #startTransaction()}, {@link #createStaticTransaction()}, {@link #noTransaction()}, and {@link #newBatchRead()}.
 */
public class AntidoteClient {

    private PoolManager poolManager;


    /**
     * Initializes an AntidoteClient with the given hosts.
     *
     * @param inetAddrs The addresses of the Antidote hosts
     */
    public AntidoteClient(InetSocketAddress... inetAddrs) {
        this(Collections.emptyList(), inetAddrs);
    }

    /**
     * Initializes an AntidoteClient with the given hosts.
     *
     * @param inetAddrs The addresses of the Antidote hosts
     */
    public AntidoteClient(List<InetSocketAddress> inetAddrs) {
        this(Collections.emptyList(), inetAddrs);
    }

    /**
     * Initializes an AntidoteClient.
     *
     * @param transformerFactories transformers for factories (the last transformer will be at the top of the stack, so applied first)
     * @param inetAddrs            The addresses of the Antidote hosts
     */
    public AntidoteClient(List<TransformerFactory> transformerFactories, InetSocketAddress... inetAddrs) {
        this(transformerFactories, Arrays.asList(inetAddrs));
    }

    /**
     * Initializes an AntidoteClient.
     *
     * @param transformerFactories transformers for factories (the last transformer will be at the top of the stack, so applied first)
     * @param inetAddrs            The addresses of the Antidote hosts
     */
    public AntidoteClient(List<TransformerFactory> transformerFactories, List<InetSocketAddress> inetAddrs) {
        init(transformerFactories, inetAddrs);
    }

    /**
     * initializes the client. Called by every constructor except for {@link #AntidoteClient(PoolManager)}.
     */
    protected void init(List<TransformerFactory> transformerFactories, List<InetSocketAddress> inetAddrs) {
        this.poolManager = new PoolManager(transformerFactories);
        for (InetSocketAddress host : inetAddrs) {
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
        return new InteractiveTransaction(this, null);
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
     * @param timestamp The minimal timestamp that this transaction should be based on.
     *                  Use the CommitInfo from the commit of a previous transaction if
     *                  you want to guarantee that the new transaction sees the former one.
     */
    public InteractiveTransaction startTransaction(CommitInfo timestamp) {
        return new InteractiveTransaction(this, timestamp);
    }

    /**
     * Creates a static transaction.
     * Static transactions can be used to execute a set of updates atomically.
     */
    public AntidoteStaticTransaction createStaticTransaction() {
        return new AntidoteStaticTransaction(this, null);
    }

    /**
     * Creates a static transaction.
     * Static transactions can be used to execute a set of updates atomically.
     * @param timestamp
     */
    public AntidoteStaticTransaction createStaticTransaction(CommitInfo timestamp) {
        return new AntidoteStaticTransaction(this, timestamp);
    }


    /**
     * Starts a new batch read, which allows to read several objects at once.
     * The {@link BatchRead} can be committed using {@link BatchRead#commit} or {@link BatchRead#commit(TransactionWithReads)}.
     */
    public BatchRead newBatchRead() {
        return new BatchRead();
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
     * Use this for executing updates and reads without a transaction context.
     */
    public NoTransaction noTransaction() {
        return new NoTransaction(this, null);
    }

    /**
     * Use this for executing updates and reads without a transaction context.
     */
    public NoTransaction noTransaction(CommitInfo timestamp) {
        return new NoTransaction(this, timestamp);
    }


    public static boolean createDC(InetSocketAddress managerNode, List<String> nodeNames) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(managerNode);
            AntidotePB.ApbCreateDC createDCMsg = AntidotePB.ApbCreateDC.newBuilder()
                    .addAllNodes(nodeNames)
                    .build();
            SocketSender socketSender = new SocketSender(s);
            AntidotePB.ApbOperationResp createDCResp = socketSender.handle(createDCMsg).accept(new AntidoteResponse.MsgOperationResp.Extractor());
            return createDCResp.getSuccess();
        }
    }

    public static ByteString getConnectionDescriptor(InetSocketAddress managerNode) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(managerNode);
            AntidotePB.ApbGetConnectionDescriptor apbGetConnectionDescriptor = AntidotePB.ApbGetConnectionDescriptor.newBuilder()
                    .build();
            SocketSender socketSender = new SocketSender(s);
            AntidotePB.ApbGetConnectionDescriptorResp connectionDescriptor = socketSender.handle(apbGetConnectionDescriptor).accept(new AntidoteResponse.MsgGetConnectionDescriptorResponse.Extractor());
            if (connectionDescriptor.getSuccess()) {
                return connectionDescriptor.getD();
            } else {
                throw new IOException("Error getting connection descriptor of node: Error code " + connectionDescriptor.getErrorcode());
            }
        }
    }

    public static boolean connectToDCs(InetSocketAddress managerNode, List<ByteString> descriptors) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(managerNode);
            AntidotePB.ApbConnectToDCs apbConnectToDcs = AntidotePB.ApbConnectToDCs.newBuilder()
                    .addAllDescriptors(descriptors)
                    .build();
            SocketSender socketSender = new SocketSender(s);
            AntidotePB.ApbOperationResp createDCResp = socketSender.handle(apbConnectToDcs).accept(new AntidoteResponse.MsgOperationResp.Extractor());
            return createDCResp.getSuccess();
        }
    }
}

