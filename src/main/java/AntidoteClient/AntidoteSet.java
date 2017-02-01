package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import java.util.AbstractMap.SimpleEntry;

/**
 * The Class AntidoteSet.
 */
public class AntidoteSet extends AntidoteObject {
	
	/** The list of locally executed but not yet pushed operations. */
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
	 * Gets the value list as ByteStrings.
	 *
	 * @return the value list BS
	 */
	public List<ByteString> getValueListBS(){
		List<ByteString> valueListBS = new ArrayList<>();
		for (String value : valueList){
			valueListBS.add(ByteString.copyFromUtf8(value));
		}
		return valueListBS;
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
	 * Removes the element from the set.
	 *
	 * @param element the element
	 */
	public void removeElement(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		removeElement(elementList);
	}
	
	/**
	 * Removes the elements from the set.
	 *
	 * @param elementList the elements
	 */
	public void removeElement(List<String> elementList){
		for(String s : elementList){
			if (valueList.contains(s)){
				valueList.remove(s);
			}
		}
		// database is also told to remove those elements from elementList that are not in the local copy
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(2, elementList);
		addUpdate(update);
	}
	
	/**
	 * Adds the element to the set.
	 *
	 * @param element the element
	 */
	public void addElement(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElement(elementList);
	}
	
	/**
	 * Adds the elements to the set.
	 *
	 * @param elementList the elements
	 */
	public void addElement(List<String> elementList){
		for (String s : elementList){
			if (! valueList.contains(s)){
				valueList.add(s);
			}
		}
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(1, elementList);
		addUpdate(update);	
	}
	
	/**
	 * Removes the element, given as ByteString, from the set.
	 *
	 * @param element the element
	 */
	public void removeElementBS(ByteString element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element.toStringUtf8());
		removeElement(elementList);
	}
	
	/**
	 * Removes the elements, given as ByteStrings, from the set.
	 *
	 * @param elementList the elements
	 */
	public void removeElementBS(List<ByteString> elementList){
		List<String> stringElementList = new ArrayList<>();
		for (ByteString elt : elementList){
			stringElementList.add(elt.toStringUtf8());
		}
		for(String s : stringElementList){
			if (valueList.contains(s)){
				valueList.remove(s);
			}
		}
		// database is also told to remove those elements from elementList that are not in the local copy
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(2, stringElementList);
		addUpdate(update);
	}
	
	/**
	 * Adds the element, given as ByteString, to the set.
	 *
	 * @param element the element
	 */
	public void addElementBS(ByteString element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element.toStringUtf8());
		addElement(elementList);
	}
	
	/**
	 * Adds the elements, given as ByteStrings to the set.
	 *
	 * @param elementList the elements
	 */
	public void addElementBS(List<ByteString> elementList){
		List<String> stringElementList = new ArrayList<>();
		for (ByteString elt : elementList){
			stringElementList.add(elt.toStringUtf8());
		}
		for (String s : stringElementList){
			if (! valueList.contains(s)){
				valueList.add(s);
			}
		}
		Map.Entry<Integer, List<String>> update = new SimpleEntry<>(1, stringElementList);
		addUpdate(update);	
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
	protected void clearUpdateList(){
		updateList.clear();
	}
}
