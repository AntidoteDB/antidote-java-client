package main.java.AntidoteClient;

import static java.lang.Math.toIntExact;
import com.basho.riak.protobuf.AntidotePB.ApbGetIntegerResp;
import com.basho.riak.protobuf.AntidotePB.ApbIntegerUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

/**
 * The Class LowLevelInteger.
 */
public final class IntegerRef extends ObjectRef{

	/**
	 * Instantiates a new low level integer.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public IntegerRef(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
    /**
     * Prepare the increment operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder incrementOpBuilder(int value){
    	ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setInc(value); //Set the integer to this value
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        return updateOperation;
    }
    
    /**
     * Prepare the set operation builder.
     *
     * @param value the value
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder setOpBuilder(int value){
    	ApbIntegerUpdate.Builder intUpdateInstruction = ApbIntegerUpdate.newBuilder(); // The specific instruction in update instructions
        intUpdateInstruction.setSet(value); //Set the integer to this value
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setIntegerop(intUpdateInstruction);
        return updateOperation;
    }
        
    /**
     * Sets the value of the integer.
     *
     * @param number the number
     * @param antidoteTransaction the antidote transaction
     */
    public void set(int number, AntidoteTransaction antidoteTransaction) {
        antidoteTransaction.updateHelper(setOpBuilder(number), getName(), getBucket(), AntidoteType.IntegerType);
    }

    /**
     * Increments the value of the integer.
     *
     * @param inc the inc
     * @param antidoteTransaction the antidote transaction
     */
    public void increment(int inc, AntidoteTransaction antidoteTransaction) {
        antidoteTransaction.updateHelper(incrementOpBuilder(inc), getName(), getBucket(), AntidoteType.IntegerType);
    }
    
    /**
     * Read integer from database.
     *
     * @param antidoteTransaction the transaction
     * @return the antidote integer
     */
    public AntidoteOuterInteger createAntidoteInteger(AntidoteTransaction antidoteTransaction){
    	ApbGetIntegerResp number = readHelper(getName(), getBucket(), AntidoteType.IntegerType, antidoteTransaction).getObjects(0).getInt();
        return new AntidoteOuterInteger(getName(), getBucket(), toIntExact(number.getValue()), getClient());  
    }
    
    /**
     * Read integer from database.
     *
     * @param antidoteTransaction the transaction
     * @return the integer value
     */
    public int readValue(AntidoteTransaction antidoteTransaction){
    	ApbGetIntegerResp number = readHelper(getName(), getBucket(), AntidoteType.IntegerType, antidoteTransaction).getObjects(0).getInt();
        return toIntExact(number.getValue());
    }
}
