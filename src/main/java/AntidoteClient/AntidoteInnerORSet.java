package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteInnerORSet.
 */
public class AntidoteInnerORSet extends AntidoteInnerSet implements InterfaceSet{

	/**
	 * Instantiates a new antidote map OR set entry.
	 *
	 * @param valueList the value list
	 * @param antidoteClient the antidote client
	 * @param name the name
	 * @param bucket the bucket
	 * @param path the path
	 * @param outerMapType the outer map type
	 */
	public AntidoteInnerORSet(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
		super(valueList, antidoteClient, name, bucket, path, outerMapType);
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.SetInterface#rollBack()
	 */
	public void rollBack(){
		clearUpdateList();
		readDatabase();
	}
	
	/* (non-Javadoc)
	 * @see main.java.AntidoteClient.SetInterface#synchronize()
	 */
	public void synchronize(){
		push();
		readDatabase();
	}

	/**
	 * Adds the element, given as ByteString.
	 *
	 * @param element the element
	 */
	public void addElementBS(ByteString element){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		addElementBS(elementList);
	}
	
	/**
	 * Adds the elements, given as ByteStrings.
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
	 * Removes the elements, given as ByteString.
	 *
	 * @param element the element
	 */
	public void removeElementBS(ByteString element){
		List<ByteString> elementList = new ArrayList<ByteString>();
		elementList.add(element);
		removeElementBS(elementList);
	}
	
	/**
	 * Removes the elements, given as ByteStrings.
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
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		AntidoteInnerORSet set;
		if (getType() == AntidoteType.GMapType){
			LowLevelGMap lowGMap = new LowLevelGMap(getName(), getBucket(), getClient());
			AntidoteOuterGMap outerMap = lowGMap.createAntidoteGMap();
			if (getPath().size() == 1){
				set = outerMap.getORSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getORSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValueList(set.getValueList());
		}
		else if (getType() == AntidoteType.AWMapType){ 
			LowLevelAWMap lowAWMap = new LowLevelAWMap(getName(), getBucket(), getClient());
			AntidoteOuterAWMap outerMap = lowAWMap.createAntidoteAWMap();
			if (getPath().size() == 1){
				set = outerMap.getORSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				set = readDatabaseHelper(getPath(), outerMap).getORSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValueList(set.getValueList());
		}
	}
}
