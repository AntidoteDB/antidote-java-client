package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteRegister.
 */
public class AntidoteRegister extends AntidoteObject {
	
	/** The value. */
	private String value;
	
	/** The update list. */
	private List<String> updateList;

	/**
	 * Instantiates a new antidote register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param value the value of the register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteRegister(String name, String bucket, String value, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.value = value;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Gets the value.
	 *
	 * @return the value
	 */
	public String getValue(){
		return value;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		value = getClient().readRegister(getName(), getBucket()).getValue();
	}
	
	/**
	 * Set the value of the register.
	 *
	 * @param element the element
	 */
	public void set(String element){
		value = element;
		updateList.add(element);
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
		for(String update : updateList){
			getClient().updateRegister(getName(), getBucket(), update);	
		}
		updateList.clear();
	}
}
