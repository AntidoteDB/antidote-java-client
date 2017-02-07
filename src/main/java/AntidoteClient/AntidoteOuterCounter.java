package main.java.AntidoteClient;

/**
 * The Class AntidoteOuterCounter.
 */
public class AntidoteOuterCounter extends AntidoteObject implements InterfaceCounter{
	
	/** The value of the counter. */
	private int value;
		
	/** The low level counter. */
	private LowLevelCounter lowLevelCounter;
	
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
		lowLevelCounter = new LowLevelCounter(name, bucket, antidoteClient);
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
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = lowLevelCounter.readValue();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.CounterInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.CounterInterface#synchronize()
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
	 * Increment.
	 *
	 * @param inc the value by which the counter is incremented
	 */
	public void increment(int inc){
		value = value + inc;
		updateAdd(lowLevelCounter.incrementOpBuilder(inc));
	}
}
