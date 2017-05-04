package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.ByteString;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteTransaction.
 */
public class AntidoteTransaction implements Closeable {


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
     * The list of the update operations.
     */
    private List<ApbUpdateOp.Builder> transactionUpdateList;

    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteTransaction(AntidoteClient antidoteClient) {
        this.antidoteClient = antidoteClient;
        this.descriptor = null;
        transactionUpdateList = new ArrayList<>();
        this.transactionStatus = TransactionStatus.INACTIVE;
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
        CREATED, STARTED, CLOSING, CLOSED, INACTIVE
    }

    ;

    /**
     * Get the transaction status.
     *
     * @return the transaction status
     */
    protected TransactionStatus getTransactionStatus() {
        return transactionStatus;
    }

    /**
     * Set the transaction status.
     *
     * @param transactionStatus the transaction status
     */
    protected void setTransactionStatus(TransactionStatus transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    /**
     * Start transaction.
     *
     * @return the byte string
     */
    protected void startTransaction() {
        //getting connection
        connection = getClient().getPoolManager().getConnection();

        if (getTransactionStatus() != TransactionStatus.CREATED) {
            throw new AntidoteException("You need to create the transaction before starting it");
        }
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readwriteTransaction = ApbStartTransaction.newBuilder();
        readwriteTransaction.setProperties(transactionProperties);

        ApbStartTransaction startTransactionMessage = readwriteTransaction.build();
        AntidoteMessage startMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStartTransaction, startTransactionMessage), connection);

        try {
            ApbStartTransactionResp transactionResponse = ApbStartTransactionResp.parseFrom(startMessage.getMessage());
            descriptor = transactionResponse.getTransactionDescriptor();
        } catch (Exception e) {
        }
        setTransactionStatus(TransactionStatus.STARTED);
    }

    /**
     * Commit transaction.
     */
    public void commitTransaction() {
        if (descriptor == null) {
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        if (getTransactionStatus() != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        ApbCommitTransaction.Builder commitTransaction = ApbCommitTransaction.newBuilder();
        commitTransaction.setTransactionDescriptor(descriptor);

        ApbCommitTransaction commitTransactionMessage = commitTransaction.build();
        descriptor = null;
        AntidoteMessage message = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbCommitTransaction, commitTransactionMessage), connection);

        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(message.getMessage());


        } catch (Exception e) {
        }
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Abort transaction.
     */
    public void abortTransaction() {
        if (descriptor == null) {
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        if (getTransactionStatus() != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        ApbAbortTransaction.Builder abortTransaction = ApbAbortTransaction.newBuilder();
        abortTransaction.setTransactionDescriptor(descriptor);

        ApbAbortTransaction abortTransactionMessage = abortTransaction.build();
        descriptor = null;
        getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbAbortTransaction, abortTransactionMessage), connection);
        clearTransactionUpdateList();
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Update helper that has the generic part of the code.
     *
     * @param operation the operation
     * @param name      the name
     * @param bucket    the bucket
     * @param type      the type
     */
    protected void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type) {
        if (getTransactionStatus() != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction first");
        }
        ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);

        if (this instanceof AntidoteStaticTransaction) {
            transactionUpdateListAdd(updateInstruction);
        } else {
            if (getDescriptor() == null) {
                throw new AntidoteException("You need to start the transaction first");
            }
            ApbUpdateObjects.Builder updateMessage = ApbUpdateObjects.newBuilder();
            updateMessage.setTransactionDescriptor(getDescriptor());
            updateMessage.addUpdates(updateInstruction);

            ApbUpdateObjects updateMessageObject = updateMessage.build();
            getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbUpdateObjects, updateMessageObject), connection);
        }
    }

    /**
     * Read helper that has the generic part of the code.
     *
     * @param name   the name
     * @param bucket the bucket
     * @param type   the type
     * @return the apb read objects resp
     */
    protected ApbReadObjectsResp readHelper(String name, String bucket, CRDT_type type) {
        if (getDescriptor() == null) {
            throw new AntidoteException("You need to start the transaction first");
        }
        if (getTransactionStatus() != TransactionStatus.STARTED) {
            throw new AntidoteException("You need to start the transaction first");
        }
        ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbReadObjects.Builder readObject = ApbReadObjects.newBuilder();
        readObject.addBoundobjects(object);
        readObject.setTransactionDescriptor(getDescriptor());

        ApbReadObjects readObjectsMessage = readObject.build();
        AntidoteMessage readMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbReadObjects, readObjectsMessage), connection);
        ApbReadObjectsResp readResponse = null;
        try {
            readResponse = ApbReadObjectsResp.parseFrom(readMessage.getMessage());
        } catch (Exception e) {
        }
        return readResponse;
    }

    protected ByteString getDescriptor() {
        return descriptor;
    }

    /**
     * Get the transaction list.
     *
     * @return the transaction list
     */
    protected List<ApbUpdateOp.Builder> getTransactionUpdateList() {
        return transactionUpdateList;
    }

    /**
     * Add update operations to the transaction list.
     *
     * @param update the update operation
     */
    protected void transactionUpdateListAdd(ApbUpdateOp.Builder update) {
        transactionUpdateList.add(update);
    }

    /**
     * Clear the transaction list.
     */
    protected void clearTransactionUpdateList() {
        transactionUpdateList.clear();
    }

    /**
     * Close the transaction.
     */
    public void close() {
        setTransactionStatus(TransactionStatus.CLOSED);
    }
}