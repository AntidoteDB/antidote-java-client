package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.google.protobuf.ByteString;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteOuterSet.
 */
public class AntidoteOuterSet extends AntidoteObject {
	
	/** The value list. */
	private List<String> valueList;
	
	/**
	 * Instantiates a new antidote set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the list of all values in the set
	 * @param antidoteClient the antidote client
	 * @param type the type
	 */
	public AntidoteOuterSet(String name, String bucket, List<String> valueList, AntidoteClient antidoteClient, CRDT_type type) {
		super(name, bucket, antidoteClient, type);
		this.valueList = valueList;
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
	 * @return the value list as ByteStrings
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
	protected void setValueList(List<String> valueList){
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
		List<ByteString> bsElementList = new ArrayList<>();
		for (String elt : elementList){
			bsElementList.add(ByteString.copyFromUtf8(elt));
		}
		removeLocal(elementList);
		addUpdate(bsElementList, AntidoteSetOpType.SetRemove);	
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
		List<ByteString> bsElementList = new ArrayList<>();
		for (String elt : elementList){
			bsElementList.add(ByteString.copyFromUtf8(elt));
		}
		addLocal(elementList);
		addUpdate(bsElementList, AntidoteSetOpType.SetAdd);	
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
		removeLocal(stringElementList);
		addUpdate(elementList, AntidoteSetOpType.SetRemove);	
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
		addLocal(stringElementList);
		addUpdate(elementList, AntidoteSetOpType.SetAdd);	
	}
	
	/**
	 * Executes the add operation locally.
	 *
	 * @param elements the elements
	 */
	private void addLocal(List<String> elements){
		for (String s : elements){
			if (! valueList.contains(s)){
				valueList.add(s);
			}
		}
	}
	
	/**
	 * Executed the remove operation locally.
	 *
	 * @param elements the elements
	 */
	private void removeLocal(List<String> elements){
		for(String s : elements){
			if (valueList.contains(s)){
				valueList.remove(s);
			}
		}
	}
	
	/**
	 * Adds the update to the updateList.
	 *
	 * @param elements the elements
	 * @param type the type
	 */
	private void addUpdate(List<ByteString> elements, int type){
		if(type == AntidoteSetOpType.SetAdd){
			updateAdd(new LowLevelSet(getName(), getBucket(), getClient()).addOpBuilder(elements));
		}
		else if(type == AntidoteSetOpType.SetRemove){
			updateAdd(new LowLevelSet(getName(), getBucket(), getClient()).removeOpBuilder(elements));
		}
	}
}
