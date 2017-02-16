package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;

/**
 * The Class AntidoteInnerSet.
 */
public class AntidoteInnerSet extends AntidoteInnerCRDT {
	
	/** The value list. */
	private Set<String> values;
	
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
		this.values = new HashSet<>(valueList);
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public Set<String> getValues(){
		return Collections.unmodifiableSet(values);
	}
	
	/**
	 * Gets the value list as ByteStrings.
	 *
	 * @return the value list as ByteStrings
	 */
	public Set<ByteString> getValuesBS(){
		Set<ByteString> valueListBS = new HashSet<ByteString>();
		for (String s : values){
			valueListBS.add(ByteString.copyFromUtf8(s));
		}
		return Collections.unmodifiableSet(valueListBS);
	}

	/**
	 * Sets the value list.
	 *
	 * @param valueList the new value list
	 */
	protected void setValues(Set<String> valueList){
		this.values = new HashSet<>(valueList);
	}
	
	
	/**
	 * Adds the elements locally.
	 *
	 * @param elementList the element list
	 */
	protected void addElementLocal(List<String> elementList){
		values.addAll(elementList);
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
		values.removeAll(elementList);
	}
}
