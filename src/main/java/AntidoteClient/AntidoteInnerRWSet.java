package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;
import interfaces.SetCRDT;

/**
 * The Class AntidoteInnerRWSet.
 */
public final class AntidoteInnerRWSet extends AntidoteInnerSet implements SetCRDT{
	
	/**
	 * Instantiates a new antidote map RW set entry.
	 *
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerRWSet(List<ByteString> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
		super(valueList, antidoteClient, name, bucket, path, outerMapType);
	}
	
	/**
	 * Adds the element, given as ByteString.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElementBS(ByteString element, AntidoteTransaction antidoteTransaction){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		addElementBS(elementList, antidoteTransaction);
	}

	/**
	 * Adds the elements, given as ByteStrings.
	 *
	 * @param elementList the element list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction){
		addElementLocal(elementList);
		List<AntidoteMapUpdate> setAdd = new ArrayList<AntidoteMapUpdate>();
		setAdd.add(AntidoteMapUpdate.createRWSetAddBS(elementList));
		updateHelper(setAdd, antidoteTransaction);
	}

	/**
	 * Removes the element, given as ByteString.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElementBS(ByteString element, AntidoteTransaction antidoteTransaction){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		removeElementBS(elementList, antidoteTransaction);
	}

	/**
	 * Removes the elements, given as ByteStrings.
	 *
	 * @param elementList the element list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElementBS(List<ByteString> elementList, AntidoteTransaction antidoteTransaction){
		removeElementLocal(elementList);
		List<AntidoteMapUpdate> setRemove = new ArrayList<AntidoteMapUpdate>();
		setRemove.add(AntidoteMapUpdate.createRWSetRemoveBS(elementList));
		updateHelper(setRemove, antidoteTransaction);
	}

	/**
	 * Adds the element.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElement(String element, AntidoteTransaction antidoteTransaction){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		addElement(elementList, antidoteTransaction);
	}

	/**
	 * Adds the elements.
	 *
	 * @param elementList the element list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void addElement(List<String> elementList, AntidoteTransaction antidoteTransaction){
		List<ByteString> elementListBS = new ArrayList<>();
		for (String elt : elementList){
			elementListBS.add(ByteString.copyFromUtf8(elt));
		}
		addElementLocal(elementListBS);
		List<AntidoteMapUpdate> setAdd = new ArrayList<AntidoteMapUpdate>();
		setAdd.add(AntidoteMapUpdate.createRWSetAdd(elementList));
		updateHelper(setAdd, antidoteTransaction);
	}

	/**
	 * Removes the element.
	 *
	 * @param element the element
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElement(String element, AntidoteTransaction antidoteTransaction){
		List<String> elementList = new ArrayList<String>();
		elementList.add(element);
		removeElement(elementList, antidoteTransaction);
	}

	/**
	 * Removes the elements.
	 *
	 * @param elementList the element list
	 * @param antidoteTransaction the antidote transaction
	 */
	public void removeElement(List<String> elementList, AntidoteTransaction antidoteTransaction){
		List<ByteString> elementListBS = new ArrayList<>();
		for (String elt : elementList){
			elementListBS.add(ByteString.copyFromUtf8(elt));
		}
		removeElementLocal(elementListBS);
		List<AntidoteMapUpdate> setRemove = new ArrayList<AntidoteMapUpdate>();
		setRemove.add(AntidoteMapUpdate.createRWSetRemove(elementList));
		updateHelper(setRemove, antidoteTransaction);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(AntidoteTransaction antidoteTransaction){
		AntidoteInnerRWSet set;
		if (getType() == AntidoteType.GMapType){
			GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap(antidoteTransaction);
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValues(set.getValuesBS());
		}
		else if (getType() == AntidoteType.AWMapType){ 
			AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap(antidoteTransaction);
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValues(new HashSet<>(set.getValuesBS()));
		}
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		AntidoteInnerRWSet set;
		if (getType() == AntidoteType.GMapType){
			GMapRef lowGMap = new GMapRef(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}
			setValues(set.getValuesBS());
		}
		else if (getType() == AntidoteType.AWMapType){
			AWMapRef lowAWMap = new AWMapRef(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}
			setValues(new HashSet<>(set.getValuesBS()));
		}
	}
}
