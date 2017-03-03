package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbBoundObject;
import com.basho.riak.protobuf.AntidotePB.ApbReadObjects;
import com.basho.riak.protobuf.AntidotePB.ApbReadObjectsResp;
import com.basho.riak.protobuf.AntidotePB.ApbStartTransaction;
import com.basho.riak.protobuf.AntidotePB.ApbStaticReadObjects;
import com.basho.riak.protobuf.AntidotePB.ApbStaticReadObjectsResp;
import com.basho.riak.protobuf.AntidotePB.ApbTxnProperties;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import main.java.AntidoteClient.AntidoteTransaction.TransactionStatus;

/**
 * The Class LowLevelObject.
 */
public class ObjectRef {
	/** The name. */
    private final String name;
    
    /** The bucket. */
    private final String bucket;
    
	/** The antidote client. */
	private final AntidoteClient antidoteClient;
	
	/**
	 * Instantiates a new low level object.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public ObjectRef(String name, String bucket, AntidoteClient antidoteClient){
		this.name = name;
        this.bucket = bucket;
        this.antidoteClient = antidoteClient;
	}
 
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName(){
    	return name;
    }
    
    /**
     * Gets the bucket.
     *
     * @return the bucket
     */
    public String getBucket(){
    	return bucket;
    };
    
	/**
	 * Gets the client.
	 *
	 * @return the client
	 */
	public AntidoteClient getClient(){
		return antidoteClient;
	}
    
//TODO: Georgios
    /**
     * Read helper.
     *
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @return the apb static read objects resp
     */
    protected ApbStaticReadObjectsResp readHelper(String name, String bucket, CRDT_type type){
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message
        object.setKey(ByteString.copyFromUtf8(name));
        object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbTxnProperties.Builder transactionProperties = ApbTxnProperties.newBuilder();

        ApbStartTransaction.Builder readTransaction = ApbStartTransaction.newBuilder();
        readTransaction.setProperties(transactionProperties);

        ApbStaticReadObjects.Builder readMessage = ApbStaticReadObjects.newBuilder();
        readMessage.setTransaction(readTransaction);
        readMessage.addObjects(object);

        ApbStaticReadObjects readMessageObject = readMessage.build();
        AntidoteMessage responseMessage = getClient().sendMessage(new AntidoteRequest(RiakPbMsgs.ApbStaticReadObjects, readMessageObject));
        
        ApbStaticReadObjectsResp readResponse = null;
		try {
			readResponse = ApbStaticReadObjectsResp.parseFrom(responseMessage.getMessage());
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return readResponse;
    }

    /**
     * Read helper that has the generic part of the code.
     *
     * @param name the name
     * @param bucket the bucket
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     * @return the apb read objects resp
     */
    protected ApbReadObjectsResp readHelper(String name, String bucket, CRDT_type type, AntidoteTransaction antidoteTransaction){
    	if (antidoteTransaction.getTransactionStatus() != TransactionStatus.STARTED){
    		throw new AntidoteException("You need to start the transaction first");
    	}
    	ApbBoundObject.Builder object = ApbBoundObject.newBuilder(); // The object in the message to update
    	object.setKey(ByteString.copyFromUtf8(name));
    	object.setType(type);
        object.setBucket(ByteString.copyFromUtf8(bucket));

        ApbReadObjects.Builder readObject = ApbReadObjects.newBuilder();
        readObject.addBoundobjects(object);
        readObject.setTransactionDescriptor(antidoteTransaction.getDescriptor());

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
