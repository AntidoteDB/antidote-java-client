package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;

/**
 * Created by george on 1/21/17.
 */
public final class AntidoteTransaction {

    /** The antidote client. */
    private final AntidoteClient antidoteClient;
    
    /** The descriptor. */
    private ByteString descriptor;

    /**
     * Instantiates a new antidote transaction.
     *
     * @param antidoteClient the antidote client
     */
    public AntidoteTransaction(AntidoteClient antidoteClient){
      this.antidoteClient=antidoteClient;
      this.descriptor = null;
    }

    /**
     * Start transaction.
     *
     * @return the byte string
     */
    public void startTransaction(){
        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readwriteTransaction = ApbStartTransaction.newBuilder();
        readwriteTransaction.setProperties(transactionProperties);

        ApbStartTransaction startTransactionMessage = readwriteTransaction.build();
        AntidoteMessage startMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStartTransaction, startTransactionMessage));

        try {
            ApbStartTransactionResp transactionResponse = ApbStartTransactionResp.parseFrom(startMessage.getMessage());
            System.out.println(transactionResponse);
            descriptor = transactionResponse.getTransactionDescriptor();
        }catch (Exception e){
            System.out.println(e);
        }
    }

    /**
     * Commit transaction.
     */
    public void commitTransaction(){
    	if (descriptor == null){
    		throw new AntidoteException("You need to start the transaction before committing it");
    	}
        ApbCommitTransaction.Builder commitTransaction = ApbCommitTransaction.newBuilder();
        commitTransaction.setTransactionDescriptor(descriptor);

        ApbCommitTransaction commitTransactionMessage = commitTransaction.build();
        descriptor = null;
        AntidoteMessage message = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbCommitTransaction, commitTransactionMessage));

        try {
            ApbCommitResp commitResponse = ApbCommitResp.parseFrom(message.getMessage());
            System.out.println(commitResponse);
        }catch (Exception e){
            System.out.println(e);
        }
    }
    
    public ByteString getDescriptor(){
    	return descriptor;
    }
}