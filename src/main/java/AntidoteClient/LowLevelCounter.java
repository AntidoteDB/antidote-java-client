package main.java.AntidoteClient;

import com.basho.riak.protobuf.AntidotePB.ApbCounterUpdate;
import com.basho.riak.protobuf.AntidotePB.ApbGetCounterResp;
import com.basho.riak.protobuf.AntidotePB.ApbUpdateOperation;

/**
 * The Class LowLevelCounter.
 */
public class LowLevelCounter extends LowLevelObject{
	
	/**
	 * Instantiates a new low level counter.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param antidoteClient the antidote client
	 */
	public LowLevelCounter(String name, String bucket, AntidoteClient antidoteClient){
		super(name, bucket, antidoteClient);
	}
	
	/**
	 * Increment.
	 */
	public void increment() {
        getClient().updateHelper(incrementOpBuilder(1), getName(), getBucket(), AntidoteType.CounterType);
    }
	
	/**
	 * Increment.
	 *
	 * @param inc the increment, by which the counter shall be incremented
	 */
    public void increment(int inc) {
        getClient().updateHelper(incrementOpBuilder(inc), getName(), getBucket(), AntidoteType.CounterType);
    }
    
    /**
     * Increment.
     *
     * @param antidoteTransaction the antidote transaction
     */
    public void increment(AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(incrementOpBuilder(1), getName(), getBucket(), AntidoteType.CounterType);    
    }

    /**
     * Increment.
     *
     * @param inc the inc
     * @param antidoteTransaction the antidote transaction
     */
    public void increment(int inc, AntidoteTransaction antidoteTransaction){
        antidoteTransaction.updateHelper(incrementOpBuilder(inc), getName(), getBucket(), AntidoteType.CounterType);    
    }
    
    /**
     * Prepare the increment operation builder.
     *
     * @param inc the inc
     * @return the apb update operation. builder
     */
    protected ApbUpdateOperation.Builder incrementOpBuilder(int inc){
    	ApbCounterUpdate.Builder counterUpdateInstruction = ApbCounterUpdate.newBuilder(); // The specific instruction in update instructions
        counterUpdateInstruction.setInc(inc); // Set increment
        ApbUpdateOperation.Builder updateOperation = ApbUpdateOperation.newBuilder();
        updateOperation.setCounterop(counterUpdateInstruction);
        return updateOperation;
    }
    
    /**
     * Read counter from database.
     *
     * @return the antidote counter
     */
    public AntidoteOuterCounter createAntidoteCounter() {
        ApbGetCounterResp counter = getClient().readHelper(getName(), getBucket(), AntidoteType.CounterType).getObjects().getObjects(0).getCounter();
        AntidoteOuterCounter antidoteCounter = new AntidoteOuterCounter(getName(), getBucket(), counter.getValue(), getClient());
        return antidoteCounter;
    }
    
    /**
     * Read counter from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the antidote counter
     */
    public AntidoteOuterCounter createAntidoteCounter(AntidoteTransaction antidoteTransaction){
    	ApbGetCounterResp counter = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.CounterType).getObjects(0).getCounter();
        AntidoteOuterCounter antidoteCounter = new AntidoteOuterCounter(getName(), getBucket(), counter.getValue(), getClient());
        return antidoteCounter;
    }
    
    /**
     * Read counter from database.
     *
     * @return the counter value
     */
    public int readValue() {
        ApbGetCounterResp counter = getClient().readHelper(getName(), getBucket(), AntidoteType.CounterType).getObjects().getObjects(0).getCounter();
        return counter.getValue();
    }
    
    /**
     * Read counter from database.
     *
     * @param antidoteTransaction the antidote transaction
     * @return the counter value
     */
    public int readValue(AntidoteTransaction antidoteTransaction){
    	ApbGetCounterResp counter = antidoteTransaction.readHelper(getName(), getBucket(), AntidoteType.CounterType).getObjects(0).getCounter();
        return counter.getValue();
    }
}
