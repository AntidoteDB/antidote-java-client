package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteOuterSet.
 */
public class AntidoteOuterSet extends AntidoteCRDT {
	
	/** The value list. */
	private Set<ByteString> valueList;
	
	/**
	 * Instantiates a new antidote set.
	 *
	 * @param name the name
	 * @param bucket the bucket
	 * @param valueList the list of all values in the set
	 * @param antidoteClient the antidote client
	 * @param type the type
	 */
	public AntidoteOuterSet(String name, String bucket, List<ByteString> valueList, AntidoteClient antidoteClient, CRDT_type type) {
		super(name, bucket, antidoteClient, type);
		this.valueList = new HashSet<>(valueList);
	}
	
	/**
	 * Gets the value list.
	 *
	 * @return the value list
	 */
	public Set<String> getValues(){
		Set<String> valueListString = new HashSet<>();
		for (ByteString s : valueList){
			valueListString.add(s.toStringUtf8());
		}
		return Collections.unmodifiableSet(valueListString);
	}
	
	/**
	 * Gets the value list as ByteStrings.
	 *
	 * @return the value list as ByteStrings
	 */
	public Set<ByteString> getValuesBS(){
		return Collections.unmodifiableSet(valueList);
	}
	
	/**
	 * Sets the value list.
	 *
	 * @param valueList the new value list
	 */
	protected void setValues(List<ByteString> valueList){
		this.valueList = new HashSet<>(valueList);
	}

	/**
	 * Removes the element from the set.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElement(String element, AntidoteTransaction antidoteTransaction){
		removeElementBS(ByteString.copyFromUtf8(element), antidoteTransaction);
	}

	/**
	 * Removes the elements from the set.
	 *
	 * @param elementList the elements
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElement(List<String> elementList, AntidoteTransaction antidoteTransaction){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String elt : elementList){
			bsElementList.add(ByteString.copyFromUtf8(elt));
		}
		removeLocal(bsElementList);
		addUpdate(bsElementList, AntidoteSetOpType.SetRemove, antidoteTransaction);
	}

	/**
	 * Adds the element to the set.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElement(String element, AntidoteTransaction antidoteTransaction){
		addElementBS(ByteString.copyFromUtf8(element), antidoteTransaction);

	}

	/**
	 * Adds the elements to the set.
	 *
	 * @param elementList the elements
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElement(List<String> elementList, AntidoteTransaction antidoteTransaction){
		List<ByteString> bsElementList = new ArrayList<>();
		for (String elt : elementList){
			bsElementList.add(ByteString.copyFromUtf8(elt));
		}
		addLocal(bsElementList);
		addUpdate(bsElementList, AntidoteSetOpType.SetAdd, antidoteTransaction);
	}

	/**
	 * Removes the element, given as ByteString, from the set.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElementBS(ByteString element, AntidoteTransaction antidoteTransaction){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element.toStringUtf8());
		removeElement(elementList, antidoteTransaction);
	}

	/**
	 * Removes the elements, given as ByteStrings, from the set.
	 *
	 * @param elementList the elements
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction){
		removeLocal(elementList);
		addUpdate(elementList, AntidoteSetOpType.SetRemove, antidoteTransaction);
	}

	/**
	 * Adds the element, given as ByteString, to the set.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElementBS(ByteString element, AntidoteTransaction antidoteTransaction){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element.toStringUtf8());
		addElement(elementList, antidoteTransaction);
	}

	/**
	 * Adds the elements, given as ByteStrings to the set.
	 *
	 * @param elementList the elements
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction){
		addLocal(elementList);
		addUpdate(elementList, AntidoteSetOpType.SetAdd, antidoteTransaction);
	}

	/**
	 * Executes the add operation locally.
	 *
	 * @param elements the elements
	 */
	private void addLocal(List<ByteString> elements){
		valueList.addAll(elements);
	}
	
	/**
	 * Executed the remove operation locally.
	 *
	 * @param elements the elements
	 */
	private void removeLocal(List<ByteString> elements){
		valueList.removeAll(elements);
	}

	/**
	 * Adds the update to the updateList.
	 *
	 * @param elements the elements
	 * @param type the type
	 * @param antidoteTransaction the antidote transaction
	 */
	private void addUpdate(List<ByteString> elements, int type, AntidoteTransaction antidoteTransaction){
		if(type == AntidoteSetOpType.SetAdd){
			antidoteTransaction.updateHelper(new SetRef(getName(), getBucket(), getClient()).addOpBuilder(elements),getName(),getBucket(),getType());
		}
		else if(type == AntidoteSetOpType.SetRemove){
			antidoteTransaction.updateHelper(new SetRef(getName(), getBucket(), getClient()).removeOpBuilder(elements),getName(),getBucket(),getType());
		}
	}
}
