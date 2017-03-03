package main.java.AntidoteClient;

import interfaces.CounterCRDT;

import java.util.List;

/**
 * The Class AntidoteOuterCounter.
 */
public final class AntidoteOuterCounter extends AntidoteCRDT implements CounterCRDT{
	
	/** The value of the counter. */
	private int value;

	/** The low level counter. */
	private final CounterRef lowLevelCounter;
	
	/**
	 * Instantiates a new antidote counter.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the counter
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterCounter(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient, AntidoteType.CounterType);
		this.value = value;
		lowLevelCounter = new CounterRef(name, bucket, antidoteClient);
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public int getValue(){
		return value;
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		value = lowLevelCounter.readValue(antidoteTransaction);
	}
	
	/**
	 * Increment by one.
	 *
	 * @param antidoteTransaction the antidote static transaction
	 */
	public void increment(AntidoteTransaction antidoteTransaction){
		increment(1, antidoteTransaction);
	}


	/**
	 * Increment.
	 *
	 * @param inc the value by which the counter is incremented
	 * @param antidoteTransaction the antidote transaction
	 */
	public void increment(int inc, AntidoteTransaction antidoteTransaction){
		value = value + inc;
		antidoteTransaction.updateHelper(lowLevelCounter.incrementOpBuilder(inc),getName(),getBucket(),getType());
	}

	public int getValue(List<AntidoteCRDT> outerObjects){
		for(AntidoteCRDT object : outerObjects)
		{
			if(object.getName() == this.getName() && object.getClient() == this.getClient() && object.getBucket() == this.getBucket())
			return ((AntidoteOuterCounter) object).getValue();
		}
		return 0;
	}
}
