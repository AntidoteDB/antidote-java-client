package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;

import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;

/**
 * The Class AntidoteMapRWSetEntry.
 */
public class AntidoteMapRWSetEntry extends AntidoteMapSetEntry {
	
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
	public AntidoteMapRWSetEntry(List<String> valueList, AntidoteClient antidoteClient, String name, String bucket, List<ApbMapKey> path, CRDT_type outerMapType) {
		super(valueList, antidoteClient, name, bucket, path, outerMapType);
	}

	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		AntidoteMapRWSetEntry set;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());

					}
				}
				set = innerMap.getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValueList(set.getValueList());
		}
		else if (getOuterMapType() == CRDT_type.AWMAP){ 
			AntidoteAWMap outerMap = getClient().readAWMap(getName(), getBucket());
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				for (int i = 1; i<getPath().size()-1; i++){
					if (getPath().get(i).getType()==CRDT_type.AWMAP){
						innerMap = innerMap.getAWMapEntry(getPath().get(i).getKey().toStringUtf8());
					}
					else if (getPath().get(i).getType()==CRDT_type.GMAP){
						innerMap = innerMap.getGMapEntry(getPath().get(i).getKey().toStringUtf8());

					}
				}
				set = innerMap.getRWSetEntry(getPath().get(getPath().size()-1).getKey().toStringUtf8());
			}		
			setValueList(set.getValueList());
		}
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
		setAdd.add(getClient().createRWSetAdd(elementList));
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
		setRemove.add(getClient().createRWSetRemove(elementList));
		updateHelper(setRemove);
	}
}
