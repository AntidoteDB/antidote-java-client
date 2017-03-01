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
	
	/**
	 * Sets the value.
	 *
	 * @param newValue the new value
	 */
	public void setValue(int newValue){
		value = newValue;
		updateAdd(lowLevelInteger.setOpBuilder(newValue));
	}

	public void setValue(int newValue, AntidoteTransaction antidoteTransaction){
		value = newValue;
		antidoteTransaction.updateHelper(lowLevelInteger.setOpBuilder(newValue),getName(),getBucket(),getType());
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = lowLevelInteger.readValue();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.IntegerInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.IntegerInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}
	
	/**
	 * Increment by one.
	 */
	public void increment(){
		increment(1);
	}

	public void increment(AntidoteTransaction antidoteTransaction){
		increment(1, antidoteTransaction);
	}
	
	/**
	 * Increment by inc.
	 *
	 * @param inc the value by which the integer is incremented
	 */
	public void increment(int inc){
		value = value + inc;
		updateAdd(lowLevelInteger.incrementOpBuilder(inc));
	}

	public void increment(int inc, AntidoteTransaction antidoteTransaction){
		value = value + inc;
		antidoteTransaction.updateHelper(lowLevelInteger.incrementOpBuilder(inc),getName(),getBucket(),getType());
	}
}
