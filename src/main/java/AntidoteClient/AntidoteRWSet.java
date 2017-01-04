package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.AbstractMap.SimpleEntry;

/**
 * The Class AntidoteRWSet.
 */
public class AntidoteRWSet extends AntidoteSet{
	
	/**
	 * Instantiates a new antidote RW set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 */
	public AntidoteRWSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient){
		super(name, bucket, valueList, antidoteClient);
	}
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		setValueList(getClient().readRWSet(getName(), getBucket()).getValueList());
	}
	
	/**
	 * Adds the element to the set.
	 *
	 * @param element the element
	 */
	public void add(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		add(elementList);
	}
	
	/**
	 * Adds the elements to the set.
	 *
	 * @param elementList the elements
	 */
	public void add(List<String> elementList){
		List<String> toAddList = new ArrayList<String>();
		for (String s : elementList){
			if (getValueList().contains(s)){
				toAddList.add(s);
			}
		}
		super.add(toAddList);
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(1, elementList);
		addUpdate(update);	}
	
	/**
	 * Removes the element from the set.
	 *
	 * @param element the element
	 */
	public void remove(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		remove(elementList);
	}
	
	/**
	 * Removes the elements from the set.
	 *
	 * @param elementList the elements
	 */
	public void remove(List<String> elementList){
		List<String> toRemoveList = new ArrayList<String>();
		for (String s : elementList){
			if (getValueList().contains(s)){
				toRemoveList.add(s);
			}
		}
		super.remove(toRemoveList);
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(2, elementList);
		addUpdate(update);
	}
	
	/**
	 * Push locally executed updates to database.
	 */
	public void push(){
		for(Map.Entry<Integer, List<String>> update : getUpdateList()){
			if(update.getKey() == 1){
				getClient().addRWSetElement(getName(), getBucket(), update.getValue());
			}
			else if(update.getKey() == 2){
				getClient().removeRWSetElement(getName(), getBucket(), update.getValue());
			}
		}
		clearUpdateList();
	}
}
