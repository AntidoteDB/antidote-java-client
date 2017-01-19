package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

/**
 * The Class AntidoteMapSetEntry.
 */
public class AntidoteMapSetEntry extends AntidoteMapEntry {
	
	/** The value list. */
	private List<String> valueList;
	
	/**
	 * Instantiates a new antidote map set entry.
	 *
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteMapSetEntry(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
		super(antidoteClient, name, bucket, path, outerMapType);
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
	 * Adds the element.
	 *
	 * @param element the element
	 */
	public void addElementBS(ByteString element){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		addElementBS(elementList);
	}
	
	/**
	 * Adds the elements.
	 *
	 * @param elementList the element list
	 */
	public void addElementBS(List<ByteString> elementList){
		List<String> stringElementList = new ArrayList<>();
		for (ByteString elt : elementList){
			stringElementList.add(elt.toStringUtf8());
		}
		addElementLocal(stringElementList);
		List<AntidoteMapUpdate> setAdd = new ArrayList<AntidoteMapUpdate>(); 
		setAdd.add(getClient().createORSetAddBS(elementList));
		updateHelper(setAdd);
	}
	
	/**
	 * Removes the element.
	 *
	 * @param element the element
	 */
	public void removeElementBS(ByteString element){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		removeElementBS(elementList);
	}
	
	/**
	 * Removes the elements.
	 *
	 * @param elementList the element list
	 */
	public void removeElementBS(List<ByteString> elementList){
		List<String> stringElementList = new ArrayList<>();
		for (ByteString elt : elementList){
			stringElementList.add(elt.toStringUtf8());
		}
		removeElementLocal(stringElementList);
		List<AntidoteMapUpdate> setRemove = new ArrayList<AntidoteMapUpdate>(); 
		setRemove.add(getClient().createORSetRemoveBS(elementList));
		updateHelper(setRemove);
	}
	
	/**
	 * Adds the element.
	 *
	 * @param element the element
	 */
	public void addElement(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElement(elementList);
	}
	
	/**
	 * Adds the elements.
	 *
	 * @param elementList the element list
	 */
	public void addElement(List<String> elementList){
		addElementLocal(elementList);
		List<AntidoteMapUpdate> setAdd = new ArrayList<AntidoteMapUpdate>(); 
		setAdd.add(getClient().createORSetAdd(elementList));
		updateHelper(setAdd);
	}
	
	/**
	 * Removes the element.
	 *
	 * @param element the element
	 */
	public void removeElement(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		removeElement(elementList);
	}
	
	/**
	 * Removes the elements.
	 *
	 * @param elementList the element list
	 */
	public void removeElement(List<String> elementList){
		removeElementLocal(elementList);
		List<AntidoteMapUpdate> setRemove = new ArrayList<AntidoteMapUpdate>(); 
		setRemove.add(getClient().createORSetRemove(elementList));
		updateHelper(setRemove);
	}
	
	/**
	 * Adds the elements locally.
	 *
	 * @param elementList the element list
	 */
	public void addElementLocal(List<String> elementList){
		for (String e : elementList){
			if (! valueList.contains(e)){
				valueList.add(e);
			}
		}
	}
	
	/**
	 * Adds the element locally.
	 *
	 * @param element the element
	 */
	public void addElementLocal(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElementLocal(elementList);
	}
	
	/**
	 * Removes the element locally.
	 *
	 * @param element the element
	 */
	public void removeElementLocal(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		removeElementLocal(elementList);
	}
	
	/**
	 * Removes the elements locally.
	 *
	 * @param elementList the element list
	 */
	public void removeElementLocal(List<String> elementList){
		for (String e : elementList){
			if (valueList.contains(e)){
				valueList.remove(e);
			}
		}
	}
}
