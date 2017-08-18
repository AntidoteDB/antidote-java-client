package eu.antidotedb.client;

import com.google.protobuf.ByteString;
import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbStartTransaction;
import eu.antidotedb.antidotepb.AntidotePB.ApbTxnProperties;
import eu.antidotedb.client.messages.AntidoteRequest;

import java.util.*;

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
     * A buffer of update instructions.
     * Instructions are executed on commit or before a read-operation.
     */
    private List<AntidotePB.ApbUpdateOp.Builder> updateInstructionBuffer = new ArrayList<>();


    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public InteractiveTransaction(AntidoteClient antidoteClient) {
        this.antidoteClient = antidoteClient;
        this.connection = antidoteClient.getPoolManager().getConnection();
        onGetConnection(connection);
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
        performBufferedUpdates();

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

    @Override
    void performUpdate(AntidotePB.ApbUpdateOp.Builder updateInstruction) {
        updateInstructionBuffer.add(updateInstruction);
    }

    @Override
    void performUpdates(List<AntidotePB.ApbUpdateOp.Builder> updateInstructions) {
        updateInstructionBuffer.addAll(updateInstructions);
    }

    /**
     * Send all buffered updates to the database so that they are executed immediately.
     * If this method is not called, updates will be executed before the next read-operation and on commit.
     */
    public void performBufferedUpdates() {
        if (updateInstructionBuffer.isEmpty()) {
            // nothing to do
            return;
        }
        if (getDescriptor() == null) {
            throw new AntidoteException("You need to start the transaction first");
        }

        List<AntidotePB.ApbUpdateOp.Builder> updates = mergeUpdates();

        AntidotePB.ApbUpdateObjects.Builder updateMessage = AntidotePB.ApbUpdateObjects.newBuilder();
        updateMessage.setTransactionDescriptor(getDescriptor());
        for (AntidotePB.ApbUpdateOp.Builder updateInstruction : updates) {
            updateMessage.addUpdates(updateInstruction);
        }
        updateInstructionBuffer.clear();

        AntidotePB.ApbUpdateObjects updateMessageObject = updateMessage.build();
        AntidotePB.ApbOperationResp resp = getClient().sendMessage(AntidoteRequest.of(updateMessageObject), connection);
        if (!resp.getSuccess()) {
            throw new AntidoteException("Could not perform update (error code: " + resp.getErrorcode() + ")");
        }
    }

    private List<AntidotePB.ApbUpdateOp.Builder> mergeUpdates() {
        List<AntidotePB.ApbUpdateOp.Builder> res = new LinkedList<>();

        Map<AntidotePB.ApbBoundObject, List<AntidotePB.ApbUpdateOperation>> updateMap = new LinkedHashMap<>();
        for (AntidotePB.ApbUpdateOp.Builder update : updateInstructionBuffer) {
            List<AntidotePB.ApbUpdateOperation> objectOperations = updateMap.computeIfAbsent(update.getBoundobject(), k -> new LinkedList<>());
            objectOperations.add(update.getOperation());
        }

        for (Map.Entry<AntidotePB.ApbBoundObject, List<AntidotePB.ApbUpdateOperation>> oplist : updateMap.entrySet()) {
            for (AntidotePB.ApbUpdateOperation mergedUpdate : mergeUpdates(oplist.getKey().getType(), oplist.getValue())) {
                res.add(AntidotePB.ApbUpdateOp.newBuilder().setBoundobject(oplist.getKey()).setOperation(mergedUpdate));
            }
        }

        return res;
    }

    private Collection<? extends AntidotePB.ApbUpdateOperation> mergeUpdates(AntidotePB.CRDT_type type, List<AntidotePB.ApbUpdateOperation> updates) {
        switch (type) {
            case AWMAP:
            case GMAP:
            case RRMAP:
                // merge key updates and removes
                HashSet<AntidotePB.ApbMapKey> removedKeys = new HashSet<>();
                HashMap<AntidotePB.ApbMapKey, List<AntidotePB.ApbUpdateOperation>> keyUpdates = new LinkedHashMap<>();
                for (AntidotePB.ApbUpdateOperation update : updates) {
                    AntidotePB.ApbMapUpdate mapop = update.getMapop();
                    removedKeys.addAll(mapop.getRemovedKeysList());
                    for (AntidotePB.ApbMapNestedUpdate nestedUpdate : mapop.getUpdatesList()) {
                        List<AntidotePB.ApbUpdateOperation> updateList = keyUpdates.computeIfAbsent(nestedUpdate.getKey(), k -> new LinkedList<>());
                        updateList.add(nestedUpdate.getUpdate());
                    }
                }
                AntidotePB.ApbMapUpdate.Builder mapUpdateBuilder = AntidotePB.ApbMapUpdate.newBuilder();
                mapUpdateBuilder.addAllRemovedKeys(removedKeys);
                for (Map.Entry<AntidotePB.ApbMapKey, List<AntidotePB.ApbUpdateOperation>> keyUpdate : keyUpdates.entrySet()) {
                    for (AntidotePB.ApbUpdateOperation mergedUpdate : mergeUpdates(keyUpdate.getKey().getType(), keyUpdate.getValue())) {
                        mapUpdateBuilder.addUpdates(AntidotePB.ApbMapNestedUpdate.newBuilder()
                            .setKey(keyUpdate.getKey())
                            .setUpdate(mergedUpdate));
                    }
                }
                return Collections.singleton(AntidotePB.ApbUpdateOperation.newBuilder().setMapop(mapUpdateBuilder).build());
            default:
                return updates;
        }
    }

    /**
     * Read helper that has the generic part of the code.
     *
     * @return the apb read objects resp
     */
    protected AntidotePB.ApbReadObjectsResp readHelper(ByteString bucket, ByteString key, AntidotePB.CRDT_type type) {
        performBufferedUpdates();

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
        performBufferedUpdates();

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
