package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class InteractiveTransaction extends TransactionWithReads {

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
    public InteractiveTransaction(AntidoteClient antidoteClient, ByteString descriptor) {
        this.antidoteClient = antidoteClient;
        this.descriptor = descriptor;
        this.transactionStatus = TransactionStatus.STARTED;
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
        AntidoteMessage message = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbCommitTransaction, commitTransactionMessage), connection);

        try {
            AntidotePB.ApbCommitResp commitResponse = AntidotePB.ApbCommitResp.parseFrom(message.getMessage());
            CommitInfo res = antidoteClient.completeTransaction(commitResponse);
            this.transactionStatus = TransactionStatus.COMMITTED;
            return res;
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse commit response", e);
        }
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
        getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbAbortTransaction, abortTransactionMessage), connection);
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
        getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbUpdateObjects, updateMessageObject), connection);
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
        AntidoteMessage readMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbReadObjects, readObjectsMessage), connection);
        AntidotePB.ApbReadObjectsResp readResponse;
        try {
            readResponse = AntidotePB.ApbReadObjectsResp.parseFrom(readMessage.getMessage());
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse read response for object " + bucket + "/" + type + "_" + key, e);
        }
        return readResponse;
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
