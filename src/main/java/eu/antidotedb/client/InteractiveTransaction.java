package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbStartTransaction;
import eu.antidotedb.antidotepb.AntidotePB.ApbTxnProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.antidotedb.client.messages.AntidoteRequest;

public class InteractiveTransaction extends TransactionWithReads implements AutoCloseable {

    /**
     * The explicit connection object.
     */
    private Connection connection;

    /**
     * The antidote client.
     */
    private final AntidoteClient antidoteClient;

    /**
     * The transaction status.
     */
    private TransactionStatus transactionStatus;

    /**
     * The descriptor.
     */
    private ByteString descriptor;


    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public InteractiveTransaction(AntidoteClient antidoteClient) {
        this.antidoteClient = antidoteClient;
        this.connection = antidoteClient.getPoolManager().getConnection();
        startTransaction();
        this.transactionStatus = TransactionStatus.STARTED;
    }

    private void startTransaction() {
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readwriteTransaction = ApbStartTransaction.newBuilder();
        readwriteTransaction.setProperties(transactionProperties);

        ApbStartTransaction startTransactionMessage = readwriteTransaction.build();
        AntidotePB.ApbStartTransactionResp transactionResponse = getClient().sendMessage(AntidoteRequest.of(startTransactionMessage), connection);
        descriptor = transactionResponse.getTransactionDescriptor();
    }

    /**
     * Get the antidote client.
     *
     * @return the antidote client
     */
    protected AntidoteClient getClient() {
        return antidoteClient;
    }

    /**
     * The enum types of the transaction status.
     */
    protected enum TransactionStatus {
        STARTED, COMMITTED, ABORTED, CLOSED
    }


    /**
     * Commit transaction.
     */
    public CommitInfo commitTransaction() {
        if (descriptor == null) {
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        AntidotePB.ApbCommitTransaction.Builder commitTransaction = AntidotePB.ApbCommitTransaction.newBuilder();
        commitTransaction.setTransactionDescriptor(descriptor);

        AntidotePB.ApbCommitTransaction commitTransactionMessage = commitTransaction.build();
        descriptor = null;
        AntidotePB.ApbCommitResp commitResponse = getClient().sendMessage(AntidoteRequest.of(commitTransactionMessage), connection);

        CommitInfo res = antidoteClient.completeTransaction(commitResponse);
        this.transactionStatus = TransactionStatus.COMMITTED;
        return res;
    }

    /**
     * Abort transaction.
     */
    public void abortTransaction() {
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("Cannot abort transaction in state " + transactionStatus);
        }
        AntidotePB.ApbAbortTransaction.Builder abortTransaction = AntidotePB.ApbAbortTransaction.newBuilder();
        abortTransaction.setTransactionDescriptor(descriptor);

        AntidotePB.ApbAbortTransaction abortTransactionMessage = abortTransaction.build();
        getClient().sendMessage(AntidoteRequest.of(abortTransactionMessage), connection);
        this.transactionStatus = TransactionStatus.ABORTED;
    }

    /**
     * Update helper that has the generic part of the code.
     *
     * @param operation the operation
     * @param name      the name
     * @param bucket    the bucket
     * @param type      the type
     */
    protected void updateHelper(AntidotePB.ApbUpdateOperation.Builder operation, String name, String bucket, AntidotePB.CRDT_type type) {
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction first");
        }
        AntidotePB.ApbBoundObject.Builder object = AntidotePB.ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        AntidotePB.ApbUpdateOp.Builder updateInstruction = AntidotePB.ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);


        performUpdate(updateInstruction);
    }

    @Override
    void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction) {
        if (getDescriptor() == null) {
            throw new AntidoteException("You need to start the transaction first");
        }
        AntidotePB.ApbUpdateObjects.Builder updateMessage = AntidotePB.ApbUpdateObjects.newBuilder();
        updateMessage.setTransactionDescriptor(getDescriptor());
        updateMessage.addUpdates(updateInstruction);

        AntidotePB.ApbUpdateObjects updateMessageObject = updateMessage.build();
        AntidotePB.ApbOperationResp resp = getClient().sendMessage(AntidoteRequest.of(updateMessageObject), connection);
        if (!resp.getSuccess()) {
            throw new AntidoteException("Could not perform update (error code: " + resp.getErrorcode() + ")");
        }
    }

    /**
     * Read helper that has the generic part of the code.
     *
     * @return the apb read objects resp
     */
    protected AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        // String name, String bucket, CRDT_type type
        if (getDescriptor() == null) {
            throw new AntidoteException("You need to start the transaction first");
        }
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction first");
        }
        AntidotePB.ApbBoundObject.Builder object = AntidotePB.ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(key);
        object.setType(type);
        object.setBucket(bucket);

        AntidotePB.ApbReadObjects.Builder readObject = AntidotePB.ApbReadObjects.newBuilder();
        readObject.addBoundobjects(object);
        readObject.setTransactionDescriptor(getDescriptor());

        AntidotePB.ApbReadObjects readObjectsMessage = readObject.build();
        return antidoteClient.sendMessage(AntidoteRequest.of(readObjectsMessage), connection);
    }

    protected ByteString getDescriptor() {
        return descriptor;
    }

    /**
     * Close the transaction.
     */
    public void close() {
        this.transactionStatus = TransactionStatus.CLOSED;
    }
}
