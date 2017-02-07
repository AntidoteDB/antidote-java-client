package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

/**
 * The Class AntidoteInnerSet.
 */
public class AntidoteInnerSet extends AntidoteInnerObject {
	
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
	public AntidoteInnerSet(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType){
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
	
	/**
	 * Gets the value list as ByteString.
	 *
	 * @return the value list as ByteString
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
	 * Adds the elements locally.
	 *
	 * @param elementList the element list
	 */
	protected void addElementLocal(List<String> elementList){
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
	protected void addElementLocal(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElementLocal(elementList);
	}
	
	/**
	 * Removes the element locally.
	 *
	 * @param element the element
	 */
	protected void removeElementLocal(String element){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		removeElementLocal(elementList);
	}
	
	/**
	 * Removes the elements locally.
	 *
	 * @param elementList the element list
	 */
	protected void removeElementLocal(List<String> elementList){
		for (String e : elementList){
			if (valueList.contains(e)){
				valueList.remove(e);
			}
		}
	}
}
