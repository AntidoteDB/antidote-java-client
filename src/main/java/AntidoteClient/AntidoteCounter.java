package main.java.AntidoteClient;
import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteCounter.
 */
public class AntidoteCounter extends AntidoteObject implements CounterInterface{
	
	/** The value of the counter. */
	private int value;
	
	/** The list of locally but not yet pushed operations. */
	private List<Integer> updateList;
	
	/**
	 * Instantiates a new antidote counter.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the counter
	 * @param antidoteClient the antidote client
	 */
	public AntidoteCounter(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.value = value;
		updateList = new ArrayList<Integer>();
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
		if (updateList.size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = getClient().readCounter(getName(), getBucket()).getValue();
	}
	
	public void rollBack(){
		updateList.clear();
		readDatabase();
	}
	
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
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(int u : updateList){
			getClient().updateCounter(getName(), getBucket(), u);
		}
		updateList.clear();
	}
	
	/**
	 * Increment.
	 *
	 * @param inc the value by which the counter is incremented
	 */
	public void increment(int inc){
		value = value + inc;
		updateList.add(inc);
	}
}
