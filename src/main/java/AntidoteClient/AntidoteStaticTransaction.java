package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.*;
import java.io.Closeable;

/**
 * Created by george on 2/22/17.
 */
public final class AntidoteStaticTransaction extends AntidoteTransaction implements Closeable{

    ApbStartTransaction.Builder startTransaction;

    /**
     * Instantiates a new antidote static transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteStaticTransaction(AntidoteClient antidoteClient){
        super(antidoteClient);
        startTransaction = null;
    }

    /**
     * Start static transaction.
     *
     * @return the start transaction
     */
    public void startTransaction(){
        if(getTransactionStatus() != TransactionStatus.CREATED){
            throw new AntidoteException("You need to create the transaction before starting it");
        }
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder writeTransaction = ApbStartTransaction.newBuilder();
        writeTransaction.setProperties(transactionProperties);

        setTransactionStatus(TransactionStatus.STARTED);
        startTransaction = writeTransaction;
    }

    /**
     * Commit static transaction.
     */
    public void commitTransaction(){
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        AntidoteMessage responseMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticUpdateObjects, createUpdateStaticObject()));
            try {
                ApbCommitResp commitResponse = ApbCommitResp.parseFrom(responseMessage.getMessage());
                System.out.println(commitResponse);
            } catch (Exception e ) {
                System.out.println(e);
            }
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Abort static transaction.
     */
    public void abortTransaction(){
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        clearTransactionList();
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Creates the static update object.
     *
     * @return the static update object
     */
    protected ApbStaticUpdateObjects createUpdateStaticObject() {
        ApbStaticUpdateObjects.Builder updateMessage = ApbStaticUpdateObjects.newBuilder(); // Message which will be sent to antidote
        updateMessage.setTransaction(startTransaction);
        for(ApbUpdateOp.Builder updateInstruction : getTransactionList()) {
            updateMessage.addUpdates(updateInstruction);
        }
        clearTransactionList();

        ApbStaticUpdateObjects UpdateMessageObject = updateMessage.build();
        return UpdateMessageObject;
    }
}
