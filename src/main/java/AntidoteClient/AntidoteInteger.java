package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;

/**
 * The Class AntidoteInteger.
 */
public class AntidoteInteger extends AntidoteObject {
	
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
		value = getClient().readInteger(getName(), getBucket()).getValue();
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
	 * Clear update list.
	 */
	public void clearUpdateList(){
		updateList.clear();
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
