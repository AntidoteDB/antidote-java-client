package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The Class AntidoteSet.
 */
public class AntidoteSet extends AntidoteObject {
	
	/** The update list. */
	private List<Map.Entry<Integer, List<String>>> updateList;	
	
	/** The value list. */
	private List<String> valueList;
	
	/**
	 * Instantiates a new antidote set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the list of all values in the set
	 * @param antidoteClient the antidote client
	 */
	public AntidoteSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient) {
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
	 * Sets the value list.
	 *
	 * @param valueList the new value list
	 */
	public void setValueList(List<String> valueList){
		this.valueList = valueList;
	}
	
	/**
	 * Removes the values from the valueList.
	 *
	 * @param toRemoveList the to remove list
	 */
	public void remove(List<String> toRemoveList){
		for(String s : toRemoveList){
			if (valueList.contains(s)){
				valueList.remove(s);
			}
		}
	}
	
	/**
	 * Adds the values to the valueList.
	 *
	 * @param toAddList the to add list
	 */
	public void add(List<String> toAddList){
		for(String s : toAddList){
			if (! valueList.contains(s)){
				valueList.add(s);
			}
		}
	}
	
	/**
	 * Adds the update to the updateList.
	 *
	 * @param update the update
	 */
	public void addUpdate(Map.Entry<Integer, List<String>> update){
		updateList.add(update);
	}
	
	/**
	 * Gets the update list.
	 *
	 * @return the update list
	 */
	public List<Map.Entry<Integer, List<String>>> getUpdateList(){
		return updateList;
	}
	
	/**
	 * Clear update list.
	 */
	public void clearUpdateList(){
		updateList.clear();
	}
}
