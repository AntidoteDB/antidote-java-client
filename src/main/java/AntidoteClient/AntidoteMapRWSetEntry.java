package main.java.AntidoteClient;

import java.util.ArrayList;
import java.util.List;
import com.basho.riak.protobuf.AntidotePB.ApbMapKey;
import com.basho.riak.protobuf.AntidotePB.CRDT_type;
import com.google.protobuf.ByteString;

/**
 * The Class AntidoteMapRWSetEntry.
 */
public class AntidoteMapRWSetEntry extends AntidoteMapSetEntry implements SetInterface{
	
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
		setAdd.add(getClient().createRWSetAddBS(elementList));
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
		setRemove.add(getClient().createRWSetRemoveBS(elementList));
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
	
	/**
	 * Gets the most recent state from the database.
	 */
	public void readDatabase(){
		if (getUpdateList().size() > 0){
			throw new AntidoteException("You can't read the database without pushing your changes first or rolling back");
		}
		AntidoteMapRWSetEntry set;
		if (getOuterMapType() == CRDT_type.GMAP){
			AntidoteGMap outerMap = getClient().readGMap(getName(), getBucket());
			if (getPath().size() == 1){
				set = outerMap.getRWSetEntry(getPath().get(0).getKey().toStringUtf8());
			}
			else{
				AntidoteMapMapEntry innerMap = null;
				if (getPath().get(0).getType()==CRDT_type.AWMAP){
					innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				}
				else if (getPath().get(0).getType()==CRDT_type.GMAP){
					innerMap = outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
				}				
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
				AntidoteMapMapEntry innerMap = null;
				if (getPath().get(0).getType()==CRDT_type.AWMAP){
					innerMap = outerMap.getAWMapEntry(getPath().get(0).getKey().toStringUtf8());
				}
				else if (getPath().get(0).getType()==CRDT_type.GMAP){
					innerMap = outerMap.getGMapEntry(getPath().get(0).getKey().toStringUtf8());
				}
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
}
