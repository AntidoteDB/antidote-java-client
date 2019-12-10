package eu.antidotedb.client;

import eu.antidotedb.antidotepb.AntidotePB;
import eu.antidotedb.antidotepb.AntidotePB.ApbCommitResp;
import eu.antidotedb.antidotepb.AntidotePB.ApbStartTransaction;
import eu.antidotedb.antidotepb.AntidotePB.ApbStaticUpdateObjects;
import eu.antidotedb.antidotepb.AntidotePB.ApbUpdateOp;
import eu.antidotedb.client.InteractiveTransaction.TransactionStatus;
import eu.antidotedb.client.messages.AntidoteRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Class AntidoteStaticTransaction.
 */
public class AntidoteStaticTransaction extends AntidoteTransaction {

    private final AntidoteClient client;
    private final CommitInfo timestamp;

    private TransactionStatus transactionStatus = TransactionStatus.STARTED;

    private List<ApbUpdateOp.Builder> transactionUpdateList = new ArrayList<>();


    /**
     * Instantiates a new antidote static transaction.
     *
     * @param antidoteClient the antidote client
     */
    AntidoteStaticTransaction(AntidoteClient antidoteClient) {
        this(antidoteClient, null);
    }

    /**
     * Instantiates a new antidote static transaction.
     *
     * @param antidoteClient the antidote client
     * @param timestamp minimal timestamp for the transaction
     */
    AntidoteStaticTransaction(AntidoteClient antidoteClient, CommitInfo timestamp) {
        client = antidoteClient;
        this.timestamp = timestamp;
    }


    /**
     * Commit static transaction.
     */
    public CommitInfo commitTransaction() {
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("Transaction already closed");
        }
        ApbCommitResp commitResponse = client.sendMessageArbitraryConnection(this, AntidoteRequest.of(createUpdateStaticObject()));
        CommitInfo res = client.completeTransaction(commitResponse);
        transactionStatus = TransactionStatus.COMMITTED;
        return res;
    }


    /**
     * Creates the static update object.
     *
     * @return the static update object
     */
    protected ApbStaticUpdateObjects createUpdateStaticObject() {
        ApbStaticUpdateObjects.Builder updateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        ApbStartTransaction.Builder startTransaction = newStartTransaction(timestamp);
        updateMessage.setTransaction(startTransaction);
        // TODO could optimize this by combining updates on same key
        for (ApbUpdateOp.Builder updateInstruction : transactionUpdateList) {
            updateMessage.addUpdates(updateInstruction);
        }
        return updateMessage.build();
    }

    protected static ApbStartTransaction.Builder newStartTransaction(CommitInfo timestamp) {
        ApbStartTransaction.Builder startTransaction = ApbStartTransaction.newBuilder();
        startTransaction.setProperties(AntidotePB.ApbTxnProperties.newBuilder());
        if (timestamp != null) {
            startTransaction.setTimestamp(timestamp.getCommitTime());
        }
        return startTransaction;
    }


    @Override
    void performUpdate(ApbUpdateOp.Builder updateInstruction) {
        transactionUpdateList.add(updateInstruction);
    }

    @Override
    void performUpdates(Collection<ApbUpdateOp.Builder> updateInstructions) {
        transactionUpdateList.addAll(updateInstructions);
    }

    List<ApbUpdateOp.Builder> getTransactionUpdateList() {
        return transactionUpdateList;
    }
}
