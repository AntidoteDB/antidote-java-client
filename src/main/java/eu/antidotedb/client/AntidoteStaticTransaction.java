package eu.antidotedb.client;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.InvalidProtocolBufferException;
import eu.antidotedb.client.InteractiveTransaction.TransactionStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteStaticTransaction.
 */
public final class AntidoteStaticTransaction extends AntidoteTransaction {

    private AntidoteClient client;

    private TransactionStatus transactionStatus = TransactionStatus.STARTED;

    private List<ApbUpdateOp.Builder> transactionUpdateList = new ArrayList<>();


    /**
     * Instantiates a new antidote static transaction.
     *
     * @param antidoteClient the antidote client
     */
    AntidoteStaticTransaction(AntidoteClient antidoteClient) {
        client = antidoteClient;
    }



    /**
     * Commit static transaction.
     */
    public CommitInfo commitTransaction() {
        if (transactionStatus != TransactionStatus.STARTED) {
            throw new AntidoteException("Transaction already closed");
        }
        AntidoteMessage responseMessage = client.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, createUpdateStaticObject()));
        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
            CommitInfo res = client.completeTransaction(commitResponse);
            transactionStatus = TransactionStatus.COMMITTED;
            return res;
        } catch (InvalidProtocolBufferException e) {
            throw new AntidoteException("Could not parse commit response", e);
        }


    }



    /**
     * Creates the static update object.
     *
     * @return the static update object
     */
    protected ApbStaticUpdateObjects createUpdateStaticObject() {
        ApbStaticUpdateObjects.Builder updateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        ApbStartTransaction.Builder startTransaction = ApbStartTransaction.newBuilder();
        updateMessage.setTransaction(startTransaction);
        for (ApbUpdateOp.Builder updateInstruction : transactionUpdateList) {
            updateMessage.addUpdates(updateInstruction);
        }
        return updateMessage.build();
    }


    @Override
    void performUpdate(ApbUpdateOp.Builder updateInstruction) {
        transactionUpdateList.add(updateInstruction);
    }
}
