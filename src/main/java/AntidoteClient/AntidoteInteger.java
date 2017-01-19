package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;

/**
 * The Class AntidoteInteger.
 */
public class AntidoteInteger extends AntidoteObject implements IntegerInterface{
	
	/** The value of the integer. */
	private int value;
	
	/** The list of locally but not yet pushed operations. */
	private List<Map.Entry<Integer, Integer>> updateList;	

	/**
	 * Instantiates a new antidote integer.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the integer
	 * @param antidoteClient the antidote client
	 */
	public AntidoteInteger(String name, String bucket, int value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.value = value;
		updateList = new ArrayList<>();
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
		updateList.add(new SimpleEntry<>(1, newValue));
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (updateList.size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		value = getClient().readInteger(getName(), getBucket()).getValue();
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
	 * Increment.
	 *
	 * @param inc the value by which the integer is incremented
	 */
	public void increment(int inc){
		value = value + inc;
		updateList.add(new SimpleEntry<>(2, inc));
	}
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(Map.Entry<Integer, Integer> update : updateList){
			if(update.getKey() == 1){
				getClient().setInteger(getName(), getBucket(), update.getValue());
			}
			else if(update.getKey() == 2){
				getClient().incrementInteger(getName(), getBucket(), update.getValue());
			}
		}
		updateList.clear();
	}
}
