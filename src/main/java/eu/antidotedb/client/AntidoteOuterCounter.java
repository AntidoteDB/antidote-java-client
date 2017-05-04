package eu.antidotedb.client;

import eu.antidotedb.client.crdt.CounterCRDT;

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

	protected void readSetValue(int newValue){
		value = newValue;
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		value = lowLevelCounter.readValue(antidoteTransaction);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		value = lowLevelCounter.readValue();
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
}
