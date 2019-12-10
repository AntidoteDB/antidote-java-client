package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbStartTransaction;
import eu.antidotedb.antidotepb.AntidotePB.ApbTxnProperties;
import eu.antidotedb.client.messages.AntidoteRequest;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InteractiveTransaction extends TransactionWithReads implements AutoCloseable {

    /**
     * The explicit connection object.
     */
    protected Connection connection;

    /**
     * The antidote client.
     */
    protected final AntidoteClient antidoteClient;

    /**
     * The transaction status.
     */
    protected TransactionStatus transactionStatus;

    /**
     * The descriptor.
     */
    protected ByteString descriptor;


    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public InteractiveTransaction(AntidoteClient antidoteClient) {
        this(antidoteClient, null);
    }

    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     * @param timestamp the minimum timestamp this transaction should be based on
     */
    public InteractiveTransaction(AntidoteClient antidoteClient, CommitInfo timestamp) {
        this.antidoteClient = antidoteClient;
        this.connection = antidoteClient.getPoolManager().getConnection();
        onGetConnection(connection);
        startTransaction(timestamp);
        this.transactionStatus = TransactionStatus.STARTED;
    }

    private void startTransaction(CommitInfo timestamp) {

        ApbStartTransaction.Builder readwriteTransaction = AntidoteStaticTransaction.newStartTransaction(timestamp);


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
        close();
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
        close();
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
        performUpdates(Collections.singletonList(updateInstruction));
    }

    @Override
    void performUpdates(Collection<AntidotePB.ApbUpdateOp.Builder> updateInstructions) {
        if (getDescriptor() == null) {
            throw new AntidoteException("You need to start the transaction first");
        }
        AntidotePB.ApbUpdateObjects.Builder updateMessage = AntidotePB.ApbUpdateObjects.newBuilder();
        updateMessage.setTransactionDescriptor(getDescriptor());
        for (AntidotePB.ApbUpdateOp.Builder updateInstruction : updateInstructions) {
            updateMessage.addUpdates(updateInstruction);
        }

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

    @Override
    void batchReadHelper(List<BatchReadResultImpl> readRequests) {
        AntidotePB.ApbReadObjects.Builder readObject = AntidotePB.ApbReadObjects.newBuilder();
        for (BatchReadResultImpl request : readRequests) {
            readObject.addBoundobjects(request.getObject());
        }
        readObject.setTransactionDescriptor(descriptor);

        AntidotePB.ApbReadObjects readObjectsMessage = readObject.build();
        AntidoteRequest.MsgReadObjects request = AntidoteRequest.of(readObjectsMessage);
        AntidotePB.ApbReadObjectsResp readResponse = antidoteClient.sendMessage(request, connection);
        int i = 0;
        for (AntidotePB.ApbReadObjectResp resp : readResponse.getObjectsList()) {
            readRequests.get(i).setResult(resp);
            i++;
        }
    }

    protected ByteString getDescriptor() {
        return descriptor;
    }

    /**
     * Close the transaction.
     */
    public void close() {
        if (transactionStatus == TransactionStatus.STARTED) {
            abortTransaction();
        }
        if (transactionStatus != TransactionStatus.CLOSED) {
            this.transactionStatus = TransactionStatus.CLOSED;
            onReleaseConnection(connection);
            connection.close();
        }
    }
}
