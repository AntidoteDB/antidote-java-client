package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.*;
import com.google.protobuf.*;

/**
 * Created by george on 1/21/17.
 */
public class AntidoteTransaction {

    /** The antidote client. */
    private AntidoteClient antidoteClient;
    
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
    
    /**
     * Update helper that has the generic part of the code.
     *
     * @param operation the operation
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     */
    protected void updateHelper(ApbUpdateOperation.Builder operation, String name, String bucket, CRDT_type type){
    	if (descriptor == null){
    		throw new AntidoteException("You need to start the transaction first");
    	}
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
    	object.setKey(ByteString.copyFromUtf8(name));
    	object.setType(type);
    	object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbUpdateOp.Builder updateInstruction = ApbUpdateOp.newBuilder();
        updateInstruction.setBoundobject(object);
        updateInstruction.setOperation(operation);

        ApbUpdateObjects.Builder updateObject = ApbUpdateObjects.newBuilder();
        updateObject.addUpdates(updateInstruction);
        updateObject.setTransactionDescriptor(descriptor);

        ApbUpdateObjects updateObjectMessage = updateObject.build();
        antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbUpdateObjects, updateObjectMessage));
    }
    
    /**
     * Read helper that has the generic part of the code.
     *
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @return the apb read objects resp
     */
    protected ApbReadObjectsResp readHelper(String name, String bucket, CRDT_type type){
    	
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
    	object.setKey(ByteString.copyFromUtf8(name));
    	object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbReadObjects.Builder readObject = ApbReadObjects.newBuilder();
        readObject.addBoundobjects(object);
        readObject.setTransactionDescriptor(descriptor);

        ApbReadObjects readObjectsMessage = readObject.build();
        AntidoteMessage readMessage = antidoteClient.sendMessage(new AntidoteRequest(RiakPbMsgs.ApbReadObjects, readObjectsMessage));
        
        ApbReadObjectsResp readResponse = null;
        try {
            readResponse = ApbReadObjectsResp.parseFrom(readMessage.getMessage());
        }catch (Exception e){
            System.out.println(e);
        }
        return readResponse;
    }
}