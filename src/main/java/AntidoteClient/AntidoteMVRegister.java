package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class AntidoteMVRegister.
 */
public class AntidoteMVRegister extends AntidoteObject {
	
	/** The value list. */
	private List<String> valueList;
	
	/** The list of locally but not yet pushed operations. */
	private List<String> updateList;

	
	/**
	 * Instantiates a new antidote MV register.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the values of the MV-Register
	 * @param antidoteClient the antidote client
	 */
	public AntidoteMVRegister(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
		super(name, bucket, antidoteClient);
		this.valueList = valueList;
		updateList = new ArrayList<>();
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public List<String> getValueList(){
		return valueList;
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		valueList = getClient().readMVRegister(getName(), getBucket()).getValueList();
	}
	
	/**
	 * Update the MV-Register.
	 *
	 * @param element the element
	 */
	public void set(String element){
		valueList.clear();
		valueList.add(element);
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
			getClient().updateMVRegister(getName(), getBucket(), update);	
		}
		updateList.clear();
	}
}
