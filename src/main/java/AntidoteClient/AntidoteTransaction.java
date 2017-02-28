package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by george on 1/21/17.
 */
public class AntidoteTransaction implements Closeable{

    /** The antidote client. */
    private final AntidoteClient antidoteClient;

    /** The transaction status. */
    private TransactionStatus transactionStatus;

    /** The descriptor. */
    private ByteString descriptor;

    /** The list of the update operations. */
    private List<ApbUpdateOp.Builder> transactionList;

    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteTransaction(AntidoteClient antidoteClient){
      this.antidoteClient=antidoteClient;
      this.descriptor = null;
      transactionList = new ArrayList<>();
      this.transactionStatus = TransactionStatus.INACTIVE;
    }

    /**
     * Get the antidote client.
     *
     * @return the antidote client
     */
    public AntidoteClient getClient(){
        return antidoteClient;
    }

    /** The enum types of the transaction status. */
    public enum TransactionStatus { CREATED, STARTED, CLOSING, CLOSED, INACTIVE };

    /**
     * Get the transaction status.
     *
     * @return the transaction status
     */
    public TransactionStatus getTransactionStatus(){
        return transactionStatus;
    }

    /** Set the transaction status.
     *
     * @param transactionStatus the transaction status
     */
    public void setTransactionStatus(TransactionStatus transactionStatus){
        this.transactionStatus = transactionStatus;
    }

    /**
     * Start transaction.
     *
     * @return the byte string
     */
    public void startTransaction(){
        if(getTransactionStatus() != TransactionStatus.CREATED){
            throw new AntidoteException("You need to create the transaction before starting it");
        }
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readwriteTransaction = ApbStartTransaction.newBuilder();
        readwriteTransaction.setProperties(transactionProperties);

        ApbStartTransaction startTransactionMessage = readwriteTransaction.build();
        AntidoteMessage startMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStartTransaction, startTransactionMessage));

        try {
            ApbStartTransactionResp transactionResponse = ApbStartTransactionResp.parseFrom(startMessage.getMessage());
            System.out.println(transactionResponse);
            descriptor = transactionResponse.getTransactionDescriptor();
        }catch (Exception e){
            System.out.println(e);
        }
        setTransactionStatus(TransactionStatus.STARTED);
    }

    /**
     * Commit transaction.
     */
    public void commitTransaction() {
        if (descriptor == null){
    		throw new AntidoteException("You need to start the transaction before committing it");
    	}
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before committing it");
        }
        ApbCommitTransaction.Builder commitTransaction = ApbCommitTransaction.newBuilder();
        commitTransaction.setTransactionDescriptor(descriptor);

        ApbCommitTransaction commitTransactionMessage = commitTransaction.build();
        descriptor = null;
        AntidoteMessage message = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbCommitTransaction, commitTransactionMessage));

        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(message.getMessage());
            System.out.println(commitResponse);


        }catch (Exception e){
            System.out.println(e);
        }
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Abort transaction.
     */
    public void abortTransaction(){
        if (descriptor == null){
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction before aborting it");
        }
        ApbAbortTransaction.Builder abortTransaction = ApbAbortTransaction.newBuilder();
        abortTransaction.setTransactionDescriptor(descriptor);

        ApbAbortTransaction abortTransactionMessage = abortTransaction.build();
        descriptor = null;
        getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbAbortTransaction, abortTransactionMessage));
        clearTransactionList();
        setTransactionStatus(TransactionStatus.CLOSING);
    }

    /**
     * Update helper that has the generic part of the code.
     *
     * @param operation the operation
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     */
    protected void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type){
        if(getTransactionStatus() != TransactionStatus.STARTED){
            throw new AntidoteException("You need to start the transaction first");
        }
        ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);

        if(this instanceof AntidoteStaticTransaction){
            transactionAdd(updateInstruction);
        }
        else{
            if (getDescriptor() == null){
                throw new AntidoteException("You need to start the transaction first");
            }
            ApbUpdateObjects.Builder updateMessage = ApbUpdateObjects.newBuilder();
            updateMessage.setTransactionDescriptor(getDescriptor());
            updateMessage.addUpdates(updateInstruction);

            ApbUpdateObjects updateMessageObject = updateMessage.build();
            getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbUpdateObjects, updateMessageObject));
        }
    }

    public ByteString getDescriptor(){
    	return descriptor;
    }

    /**
     * Get the transaction list.
     *
     * @return the transaction list
     */
    protected List<ApbUpdateOp.Builder> getTransactionList(){
        return transactionList;
    }

    /**
     * Add update operations to the transaction list.
     *
     * @param update the update operation
     */
    protected void transactionAdd(ApbUpdateOp.Builder update){
        transactionList.add(update);
    }

    /** Clear the transaction list. */
    protected void clearTransactionList(){
        transactionList.clear();
    }

    /** Close the transaction. */
    public void close() {
        setTransactionStatus(TransactionStatus.CLOSED);
    }
}