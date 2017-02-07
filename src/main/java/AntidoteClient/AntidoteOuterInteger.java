package main.java.AntidoteClient;

/**
 * The Class AntidoteOuterInteger.
 */
public class AntidoteOuterInteger extends AntidoteObject implements InterfaceInteger{
	
	/** The value of the integer. */
	private int value;

	/** The low level integer. */
	private LowLevelInteger lowLevelInteger;
	
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
		lowLevelInteger = new LowLevelInteger(name, bucket, antidoteClient);
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
	
	/**
	 * Increment by inc.
	 *
	 * @param inc the value by which the integer is incremented
	 */
	public void increment(int inc){
		value = value + inc;
		updateAdd(lowLevelInteger.incrementOpBuilder(inc));
	}
}
