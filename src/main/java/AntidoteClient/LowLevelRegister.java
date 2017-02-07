package main.java.AntidoteClient;
import com.basho.riak.protobuf.AntidotePB.ApbRegUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class LowLevelRegister.
 */
public class LowLevelRegister extends LowLevelObject {
	
	/**
	 * Instantiates a new low level register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelRegister(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}

    /**
     * Sets the value.
     *
     * @param value the value
     * @param type the type
     */
    public void set(String value, CRDT_type type){
        getClient().updateHelper(setOpBuilder(ByteString.copyFromUtf8(value)), getName(), getBucket(), type);
    }  
    
    /**
     * Sets the value.
     *
     * @param value the value
     * @param type the type
     */
    public void setBS(ByteString value, CRDT_type type){
        getClient().updateHelper(setOpBuilder(value), getName(), getBucket(), type);
    }  

    /**
     * Sets the value.
     *
     * @param value the value
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void setBS(ByteString value, CRDT_type type, AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(setOpBuilder(value), getName(), getBucket(), type);
    }
    
    /**
     * Sets the value.
     *
     * @param value the value
     * @param type the type
     * @param antidoteTransaction the antidote transaction
     */
    public void set(String value, CRDT_type type, AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(setOpBuilder(ByteString.copyFromUtf8(value)), getName(), getBucket(), type);
    }
    
    /**
     * Prepare a set operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder setOpBuilder(ByteString value){
    	ApbRegUpdate.Builder regUpdateInstruction = ApbRegUpdate.newBuilder(); // The specific instruction in update instructions
        regUpdateInstruction.setValue(value);
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setRegop(regUpdateInstruction);
        return updateOperation;
    }
}
