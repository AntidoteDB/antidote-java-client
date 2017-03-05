package main.java.AntidoteClient;

import interfaces.IntegerCRDT;

/**
 * The Class AntidoteOuterInteger.
 */
public final class AntidoteOuterInteger extends AntidoteCRDT implements IntegerCRDT{
	
	/** The value of the integer. */
	private int value;

	/** The low level integer. */
	private final IntegerRef lowLevelInteger;
	
	/**
	 * Instantiates a new antidote integer.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the integer
	 * @param antidoteClient the antidote client
	 */
	public AntidoteOuterInteger(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient, AntidoteType.IntegerType);
		this.value = value;
		lowLevelInteger = new IntegerRef(name, bucket, antidoteClient);
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
	 * Sets the value.
	 *
	 * @param newValue the new value
	 * @param antidoteTransaction the antidote transaction
	 */
	public void setValue(int newValue, AntidoteTransaction antidoteTransaction){
		value = newValue;
		antidoteTransaction.updateHelper(lowLevelInteger.setOpBuilder(newValue),getName(),getBucket(),getType());
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		value = lowLevelInteger.readValue(antidoteTransaction);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		value = lowLevelInteger.readValue();
	}
	
	/**
	 * Increment by one.
	 *
	 * @param antidoteTransaction the antidote transaction
	 */
	public void increment(AntidoteTransaction antidoteTransaction){
		increment(1, antidoteTransaction);
	}

	/**
	 * Increment by inc.
	 *
	 * @param inc the value by which the integer is incremented
	 * @param antidoteTransaction the antidote transaction
	 */
	public void increment(int inc, AntidoteTransaction antidoteTransaction){
		value = value + inc;
		antidoteTransaction.updateHelper(lowLevelInteger.incrementOpBuilder(inc),getName(),getBucket(),getType());
	}
}
